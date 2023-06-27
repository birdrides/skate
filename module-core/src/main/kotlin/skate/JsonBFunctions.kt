@file:Suppress("UNCHECKED_CAST")

package skate

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import net.postgis.jdbc.geometry.Point
import skate.JsonFunctions.JACKSON
import kotlin.reflect.KClass
import kotlin.reflect.KProperty1

fun <T : Any> rowToJson(alias: TableAlias<T>) = Function1<String, PSqlJson>("row_to_json", AliasLiteral(alias.alias))
fun <T : Any> rowToJson(table: KClass<T>) = Function1<String, PSqlJson>("row_to_json", TableLiteral(table))

// JsonB || JsonB
infix fun Expression<PSqlJsonb>.concat(override: Expression<PSqlJsonb>): Expression<PSqlJsonb> {
  return ConcatJsonb(this, override)
}

@Suppress("UNCHECKED_CAST")
infix fun Expression<PSqlJsonb>.concat(override: Map<String, Any?>): Expression<PSqlJsonb> {
  return ConcatJsonb(
    this,
    jsonbBuildObject(*override.map { (k, v) -> k to jsonBLiteral(v) as Expression<Any> }.toTypedArray())
  )
}

inline infix fun <reified T : Any> KProperty1<T, Map<String, Any?>>.concat(override: Map<String, Any?>): Expression<Map<String, Any?>> {
  return (this.cast(TypeSpec<PSqlJsonb>("jsonb")) concat override) as Expression<Map<String, Any?>>
}

fun jsonbBuildObject(vararg pairs: Pair<String, Expression<Any>>): Expression<PSqlJsonb> {
  return BuildJsonb(pairs.toList())
}

fun asGeoJson(pointExpression: Expression<Point?>): Expression<PSqlJson> {
  return Function1("ST_AsGeoJson", pointExpression)
}

fun Expression<PSqlJson>.jsonb(): Expression<PSqlJsonb> {
  return Cast(this, TypeSpec("jsonb"))
}

fun jsonBLiteral(x: Any?): Expression<Any?> {
  return if (x is Iterable<*>) {
    val asList = x.toList()
    if (asList.isEmpty()) {
      Array(listOf(), String::class) as Expression<Any?>
    } else {
      when (asList[0]) {
        is String -> (asList as List<String>).array() as Expression<Any?>
        is Number -> (asList as List<Number>).array() as Expression<Any?>
        else -> asList.map { it.toJsonMap() }.array() as Expression<Any?>
      }
    }
  } else {
    literal(x)
  }
}

internal object JsonFunctions {
  val JACKSON = jacksonObjectMapper().apply {
    setSerializationInclusion(JsonInclude.Include.NON_NULL)
  }
}

fun <T> T.toJsonMap(): Map<String, Any> {
  return try {
    val input = JACKSON.writer().writeValueAsString(this)
    JACKSON.readerFor(Map::class.java).readValue(input)
  } catch (e: Exception) {
    hashMapOf()
  }
}
