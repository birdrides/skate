package skate.internal.mapper

import org.jdbi.v3.core.mapper.ColumnMapper
import org.jdbi.v3.core.statement.StatementContext
import skate.Join
import java.sql.ResultSet
import kotlin.reflect.KType
import kotlin.reflect.jvm.javaType

internal interface ValueProvider {
  /**
   * Invoke and produce a value of a [ResultSet] from JDBC
   *
   * @param result The result set from JDBC query passed down to JDBI
   * @param context The current query statement context
   */
  operator fun invoke(result: ResultSet, context: StatementContext): Any?

  companion object {
    /**
     * Null value provider for nullable default property
     */
    val NULL = object : ValueProvider {
      override fun invoke(result: ResultSet, context: StatementContext): Any? {
        return null
      }
    }

    /**
     * Create new instance of [ValueProvider] for a specific column
     *
     * @param type The Kotlin type of this column
     * @param columnNumber The index of this column
     * @param context The current query statement context
     */
    fun column(
      type: KType,
      columnNumber: Int,
      context: StatementContext
    ): ValueProvider {
      val columnMapper = context.findColumnMapperFor(type.javaType).orElse(ColumnMapper { r, n, _ -> r.getObject(n) })
      return object : ValueProvider {
        override fun invoke(result: ResultSet, context: StatementContext): Any? {
          return columnMapper.map(result, columnNumber, context)
        }
      }
    }

    /**
     * Create new instance of [ValueProvider] for [Join] query
     *
     * @param invoker The current parent invoker
     * @param columns A list of pair of column index and column label
     */
    fun join(
      invoker: Invoker<*>,
      columns: List<Pair<Int, String>>
    ): ValueProvider {
      return object : ValueProvider {
        override fun invoke(result: ResultSet, context: StatementContext): Any? {
          // If all columns are null, the joined object should be null.
          return if (columns.all { result.getObject(it.first) == null }) {
            null
          } else {
            invoker(result, context)
          }
        }
      }
    }
  }
}
