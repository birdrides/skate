package skate.internal.mapper.column

import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.argument.Argument
import org.jdbi.v3.core.argument.ArgumentFactory
import org.jdbi.v3.core.config.ConfigRegistry
import org.jdbi.v3.core.mapper.ColumnMapper
import org.jdbi.v3.core.statement.StatementContext
import net.postgis.jdbc.geometry.Geometry
import net.postgis.jdbc.PGgeometry
import org.postgresql.util.PGobject
import java.lang.reflect.Type
import java.sql.ResultSet
import java.sql.Types
import java.util.Optional

const val WGS84: Int = 4326

class GeometryArgumentFactory<T : Geometry>(private val clazz: Class<T>) : ArgumentFactory {
  override fun build(type: Type?, value: Any?, config: ConfigRegistry?): Optional<Argument> {
    return if (value is Geometry? && type?.typeName != clazz.typeName) {
      Optional.of(
        Argument { position, statement, _ ->
          if (value != null) {
            value.srid = WGS84
            val geometry = PGgeometry(value.toString())
            statement.setObject(position, geometry)
          } else {
            statement.setNull(position, Types.OTHER)
          }
        }
      )
    } else {
      Optional.empty()
    }
  }
}

class GeometryColumnMapper<T : Geometry> : ColumnMapper<T> {
  override fun map(r: ResultSet?, position: Int, ctx: StatementContext?): T? {
    val obj = r?.getObject(position)
    return if (obj != null) {
      @Suppress("UNCHECKED_CAST")
      PGgeometry((obj as PGobject).value).geometry as T
    } else {
      null
    }
  }
}

inline fun <reified T : Geometry> Jdbi.registerPostgisGeometry(): Jdbi {
  return this
    .registerArgument(GeometryArgumentFactory(T::class.java))
    .registerColumnMapper(T::class.java, GeometryColumnMapper<T>())
}
