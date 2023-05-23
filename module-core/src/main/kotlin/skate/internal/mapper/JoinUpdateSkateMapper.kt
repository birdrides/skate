package skate.internal.mapper

import org.jdbi.v3.core.mapper.RowMapper
import org.jdbi.v3.core.statement.StatementContext
import skate.Query
import skate.Update
import java.sql.ResultSet
import java.sql.SQLException
import kotlin.reflect.KClass

/**
 * To enhance the cachability of this class, we require a more effective
 * method for extracting cacheable fields from a [Query]. Currently, as we have a
 * relatively small number of raw queries, this isn't a pressing concern.
 * However, improvements are still necessary for future efficiency and scalability.
 */
internal class JoinUpdateSkateMapper<T : Any>(
  private val type: KClass<T>,
  private val update: Update<*>
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
      update = update,
      columns = columns,
      context = context
    )
    return RowMapper { r, c -> invoker(r, c) }
  }
}
