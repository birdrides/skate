@file:Suppress("UNCHECKED_CAST")

package skate.internal.mapper

import org.jdbi.v3.core.mapper.ColumnMapper
import org.jdbi.v3.core.mapper.RowMapper
import org.jdbi.v3.core.statement.StatementContext
import java.sql.ResultSet
import java.sql.SQLException
import kotlin.reflect.KClass

internal class EnumSkateMapper<T : Any>(
  private val type: KClass<T>
) : SkateMapper<T> {

  @Throws(SQLException::class)
  override fun map(rs: ResultSet, ctx: StatementContext): T {
    return specialize(rs, ctx).map(rs, ctx)
  }

  @Throws(SQLException::class)
  override fun specialize(resultSet: ResultSet, context: StatementContext): RowMapper<T> {
    return RowMapper<T> { rs, ctx ->
      ctx
        .findColumnMapperFor(type.java)
        .orElse(ColumnMapper { r, n, _ -> r.getObject(n) as? T })
        .map(rs, 1, ctx)
    }
  }
}
