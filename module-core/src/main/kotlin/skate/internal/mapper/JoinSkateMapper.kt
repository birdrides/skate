package skate.internal.mapper

import org.jdbi.v3.core.mapper.RowMapper
import org.jdbi.v3.core.statement.StatementContext
import skate.Query
import java.sql.ResultSet
import java.sql.SQLException
import kotlin.reflect.KClass

/**
 * Alias for column with index
 */
internal typealias Column = Pair<Int, String>

/**
 * In order to make this class cacheable, we are seeking an improved approach for extracting cacheable
 * fields from a [Query]. Currently, we have a limited number of raw queries, so the existing method should
 * suffice for now. However, we recognize the need for a more robust solution in the long run.
 */
internal class JoinSkateMapper<T : Any>(
  private val type: KClass<T>,
  private val query: Query
) : SkateMapper<T> {

  @Throws(SQLException::class)
  override fun map(rs: ResultSet, ctx: StatementContext): T {
    return specialize(rs, ctx).map(rs, ctx)
  }

  @Throws(SQLException::class)
  override fun specialize(resultSet: ResultSet, context: StatementContext): RowMapper<T> {
    val metadata = resultSet.metaData
    val columns = (1..metadata.columnCount).map { Pair(it, metadata.getColumnLabel(it)) }
    val invoker = InvokerBuilder.build(
      type = type,
      query = query,
      columns = columns,
      context = context
    )
    return RowMapper { r, c -> invoker(r, c) }
  }
}
