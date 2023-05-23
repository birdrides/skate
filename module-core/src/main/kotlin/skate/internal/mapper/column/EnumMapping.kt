package skate.internal.mapper.column

import org.apache.commons.lang3.reflect.TypeUtils
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.argument.Argument
import org.jdbi.v3.core.argument.ArgumentFactory
import org.jdbi.v3.core.config.ConfigRegistry
import org.jdbi.v3.core.mapper.ColumnMapper
import org.jdbi.v3.core.mapper.ColumnMapperFactory
import org.jdbi.v3.core.statement.StatementContext
import org.postgresql.util.PGobject
import java.lang.reflect.ParameterizedType
import java.lang.reflect.Type
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Types
import java.util.Optional
import kotlin.reflect.KClass

/**
 * This is a common base class that can be used by the Skate/JDBI integration to let us process update statements
 * despite the way Java/JDBI/JDBC 'leak' type information. In the future we should probably move the array-specific
 * logic in Skate (Jdbi.kt) towards using ArgumentFactories instead of having special-casing during query materialization.
 */
sealed class EnumArgument(
  val name: String
) : Argument

class NullEnumArgument(enumName: String) : EnumArgument(enumName) {
  override fun apply(position: Int, statement: PreparedStatement, ctx: StatementContext?) {
    statement.setNull(position, Types.OTHER)
  }
}

class EnumValueArgument(
  val value: String,
  enumName: String
) : EnumArgument(enumName) {
  override fun apply(position: Int, statement: PreparedStatement, ctx: StatementContext?) {
    val result = PGobject()
    result.type = name
    result.value = value
    statement.setObject(position, result)
  }
}

class EnumArrayArgument(
  val values: List<String>,
  enumName: String
) : EnumArgument(enumName) {
  override fun apply(position: Int, statement: PreparedStatement, ctx: StatementContext?) {
    val valueList = values.joinToString(",")
    val result = PGobject()
    result.type = "$name[]"
    result.value = "{$valueList}"
    statement.setObject(position, result)
  }
}

class EnumArgumentFactory<T : Enum<T>>(private val clazz: KClass<T>, private val enumName: String) : ArgumentFactory {
  override fun build(type: Type?, value: Any?, config: ConfigRegistry?): Optional<Argument> {
    return if (type?.typeName == clazz.java.typeName) {
      if (value != null) {
        Optional.of<Argument>(EnumValueArgument(value.toString(), enumName))
      } else {
        Optional.of<Argument>(NullEnumArgument(enumName))
      }
    } else if (type is ParameterizedType && type.actualTypeArguments[0] == clazz.java) {
      if (value is Iterable<*>) {
        Optional.of<Argument>(EnumArrayArgument(value.map { it.toString() }, enumName))
      } else {
        Optional.of<Argument>(NullEnumArgument(enumName))
      }
    } else {
      Optional.empty()
    }
  }
}

class EnumColumnMapper<T : Enum<T>>(private val clazz: Class<T>) : ColumnMapper<T> {
  override fun map(resultSet: ResultSet?, position: Int, context: StatementContext?): T? {
    val value = resultSet?.getObject(position, PGobject::class.java)?.value
    return if (value != null) {
      java.lang.Enum.valueOf(clazz, value.toUpperCase())
    } else {
      null
    }
  }
}

class ArrayOfEnumColumnMapper<T : Enum<T>>(private val clazz: Class<T>) : ColumnMapper<List<T>> {
  override fun map(resultSet: ResultSet?, position: Int, context: StatementContext?): List<T>? {
    val value = resultSet?.getObject(position, PGobject::class.java)?.value ?: return null
    if (!value.startsWith("{") || !value.endsWith("}")) {
      return null
    }

    val components = value.substring(1, value.length - 1).split(",")
    return components.mapNotNull {
      if (it.isBlank()) {
        null
      } else {
        java.lang.Enum.valueOf(clazz, it.trim().toUpperCase())
      }
    }
  }
}

class EnumColumnMapperFactory<T : Enum<T>>(private val clazz: Class<T>) : ColumnMapperFactory {
  private val mapper = EnumColumnMapper(clazz)
  private val arrayMapper = ArrayOfEnumColumnMapper(clazz)

  override fun build(type: Type?, config: ConfigRegistry?): Optional<ColumnMapper<*>> {
    return if (type?.typeName == clazz.typeName) {
      Optional.of(mapper)
    } else if (type is ParameterizedType && TypeUtils.isAssignable(clazz, type.actualTypeArguments[0])) {
      Optional.of<ColumnMapper<*>>(arrayMapper)
    } else {
      Optional.empty()
    }
  }
}

inline fun <reified T : Enum<T>> Jdbi.registerEnum(enumName: String): Jdbi {
  return this
    .registerArgument(EnumArgumentFactory(T::class, enumName))
    .registerColumnMapper(EnumColumnMapperFactory(T::class.java))
}
