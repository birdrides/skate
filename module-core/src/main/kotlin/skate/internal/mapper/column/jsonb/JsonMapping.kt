package skate.internal.mapper.column.jsonb

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.MapperFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.PropertyNamingStrategy
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
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
import java.sql.ResultSet
import java.sql.Types
import java.util.Optional
import java.util.UUID
import kotlin.reflect.KClass

private val JACKSON: ObjectMapper = ObjectMapper().apply {
  propertyNamingStrategy = PropertyNamingStrategy.SNAKE_CASE
  configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
  configure(SerializationFeature.WRITE_ENUMS_USING_TO_STRING, true)
  configure(DeserializationFeature.READ_ENUMS_USING_TO_STRING, true)
  configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS, true)
  configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
  registerKotlinModule()
  registerModule(JavaTimeModule())
  findAndRegisterModules()
}

private fun <T : Any> matches(type: Type?, kClass: KClass<T>): Boolean {
  fun Type?.same(c: Class<T>): Boolean {
    if (TypeUtils.isAssignable(this, c)) {
      return true
    }
    return this != null && typeName.startsWith(c.typeName)
  }

  // Check if this is generic type, if it is recursively find a match, or else
  // follow normal match flow. Fortunately, JSONB must be either an array type or an object type,
  // anything else would be invalid. Thus, we can simplify our match by checking on the first argument.
  val jClass = kClass.java
  if (type is ParameterizedType) {
    val list = type.rawType == List::class.java
    val matched = type.actualTypeArguments.first().same(jClass)
    if (list && matched) {
      return true
    }

    if (!list && matched) {
      throw IllegalStateException("JSONB generic type must be List<$kClass> and nothing else. Found $type")
    }
  }

  return type.same(jClass)
}

class JsonArgumentFactory<T : Any>(
  private val kClass: KClass<T>
) : ArgumentFactory {

  override fun build(type: Type?, value: Any?, config: ConfigRegistry?): Optional<Argument> {
    return if (matches(type, kClass)) {
      Optional.of(
        Argument { position, statement, _ ->
          if (value != null) {
            val result = PGobject()
            result.type = "json"
            result.value = JACKSON.writeValueAsString(value)
            statement.setObject(position, result)
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

internal class JsonColumnMapper<T : Any>(
  private val kClass: KClass<T>,
  private val typeReference: TypeReference<List<T>>?
) : ColumnMapper<T> {

  override fun map(resultSet: ResultSet?, position: Int, context: StatementContext?): T? {
    val value = resultSet?.getObject(position, PGobject::class.java)?.value
    return if (value != null) {
      val tree = JACKSON.readTree(value)
      if (tree.isArray) {
        @Suppress("UNCHECKED_CAST")
        JACKSON.readValue(value, typeReference) as T?
      } else {
        JACKSON.treeToValue(tree, kClass.java)
      }
    } else {
      null
    }
  }
}

class JsonColumnMapperFactory<T : Any>(
  private val kClass: KClass<T>,
  private val typeReference: TypeReference<List<T>>?
) : ColumnMapperFactory {

  override fun build(type: Type?, config: ConfigRegistry?): Optional<ColumnMapper<*>> {
    return if (matches(type, kClass)) {
      Optional.of(JsonColumnMapper(kClass, typeReference))
    } else {
      Optional.empty()
    }
  }
}

/**
 * You must provide [TypeReference] if you want to use List<T> for your column.
 * Furthermore, it must be [List] and nothing else, so no [Set], [Array], etc…
 *
 * @param typeReference The type reference which is used to capture generic type.
 * It's nullable to avoid breaking changes.
 */
inline fun <reified T : Any> Jdbi.registerJson(
  typeReference: TypeReference<List<T>>? = null
): Jdbi {
  return this
    .registerArgument(JsonArgumentFactory(T::class))
    .registerColumnMapper(JsonColumnMapperFactory(T::class, typeReference))
}

/**
 * Note that we can't use:
 * - `real` for float
 * - `double precision` for double
 * for array type due to internal type map of PG connection is expecting
 * `float4` and `float8` respectively as they are declared in [org.postgresql.jdbc.TypeInfoCache]
 */
fun Jdbi.registerPGArrays(): Jdbi {
  // Kotlin
  registerArrayType(Int::class.java, "integer")
  registerArrayType(Long::class.java, "bigint")
  registerArrayType(Float::class.java, "float4")
  registerArrayType(Double::class.java, "float8")
  registerArrayType(String::class.java, "varchar")

  // Java
  registerArrayType(java.lang.Integer::class.java, "integer")
  registerArrayType(java.lang.Long::class.java, "bigint")
  registerArrayType(java.lang.Float::class.java, "float4")
  registerArrayType(java.lang.Double::class.java, "float8")
  registerArrayType(java.lang.String::class.java, "varchar")

  // Other
  registerArrayType(UUID::class.java, "uuid")
  return this
}
