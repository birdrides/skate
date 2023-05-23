package skate

import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.isKotlinClass
import org.jdbi.v3.core.kotlin.mapTo
import org.jdbi.v3.core.result.ResultBearing
import org.jdbi.v3.core.result.ResultIterable
import org.jdbi.v3.core.statement.SqlStatement
import skate.generator.DeleteStatement
import skate.generator.InsertStatement
import skate.generator.SelectStatement
import skate.generator.UpdateStatement
import skate.internal.mapper.SkateMappers
import skate.internal.mapper.column.EnumArgument
import java.sql.Types
import java.util.UUID

fun <T : SqlStatement<T>> T.bindValues(values: List<Any>): T {
  if (values.count() > Short.MAX_VALUE) {
    throw Exception("Query would require binding more parameters than Postgresql can support.")
  }

  for ((index, value) in values.withIndex()) {
    if (value is Array<*>) {
      val sqlArrayElementType = when (value.type) {
        Boolean::class -> "boolean"
        Int::class -> "integer"
        Double::class -> "double precision"
        Float::class -> "real"
        String::class -> "text"
        UUID::class -> "uuid"
        Map::class -> "jsonb"
        else -> {
          val optionalArg = context.findArgumentFor(value.type.java, null)
          if (!optionalArg.isPresent) {
            if (value.type.java.isEnum) {
              throw IllegalArgumentException("Unknown array parameter type '${value.type}'. If you're adding a new DB-mapped enum type, make sure you've called registerEnum(). ($value)")
            } else {
              throw IllegalArgumentException("Unknown array parameter type '${value.type}'. Please update Skate to handle this new array primitive. ($value)")
            }
          }
          val arg = optionalArg.get() as? EnumArgument
            ?: throw IllegalArgumentException("Array parameter type '${value.type}' is supported by Skate's JDBI integration but not as a Skate.Array type. ($value)")
          arg.name
        }
      }
      val sqlArray = this.context.connection.createArrayOf(sqlArrayElementType, value.items.toTypedArray())
      bindBySqlType(index, sqlArray, Types.ARRAY)
    } else {
      bind(index, value)
    }
  }
  return this
}

inline fun <reified T : Any> ResultBearing.mapResults(query: Query? = null): ResultIterable<T> {
  val clazz = T::class.java
  return if (clazz.isKotlinClass()) {
    map(SkateMappers.resolve(T::class, query))
  } else {
    mapTo()
  }
}

inline fun <reified T : Any> ResultBearing.mapResults(update: Update<*>? = null): ResultIterable<T> {
  val clazz = T::class.java
  return if (clazz.isKotlinClass()) {
    map(SkateMappers.resolve(T::class, update))
  } else {
    mapTo()
  }
}

inline fun <reified T : Any> SelectStatement.query(
  handle: Handle,
  timeoutSeconds: Int
): List<T> {
  return handle.createQuery(sql)
    .setQueryTimeout(timeoutSeconds)
    .bindValues(values)
    .mapResults<T>(query = null)
    .list()
}

inline fun <reified T : Any> Jdbi.queryRaw(
  sql: String,
  values: List<Any>,
  query: Query? = null,
  timeoutSeconds: Int
): List<T> {
  return withHandle<List<T>, Exception> { handle ->
    handle.createQuery(sql)
      .setQueryTimeout(timeoutSeconds)
      .bindValues(values)
      .mapResults<T>(query = query)
      .list()
  }
}

inline fun <reified T : Any> SelectStatement.queryFirst(
  handle: Handle,
  timeoutSeconds: Int
): T? {
  return query<T>(handle, timeoutSeconds).firstOrNull()
}

fun InsertStatement<*>.execute(
  handle: Handle,
  timeoutSeconds: Int
): Int {
  return handle
    .createUpdate(sql)
    .setQueryTimeout(timeoutSeconds)
    .apply {
      for ((index, row) in rows.withIndex()) {
        bindBean("$prefix$index", row)
      }
    }
    .bindValues(values)
    .execute()
}

inline fun <reified T : Any> InsertStatement<T>.query(
  handle: Handle,
  timeoutSeconds: Int
): List<T> {
  return handle.createQuery(sql)
    .setQueryTimeout(timeoutSeconds)
    .apply {
      for ((index, row) in rows.withIndex()) {
        bindBean("$prefix$index", row)
      }
    }
    .bindValues(values)
    .mapResults<T>(query = null)
    .list()
}

inline fun <reified T : Any> InsertStatement<T>.queryFirst(
  handle: Handle,
  timeoutSeconds: Int
): T? {
  return query(handle, timeoutSeconds).firstOrNull()
}

fun UpdateStatement.execute(
  handle: Handle,
  timeoutSeconds: Int
): Int {
  return handle.createUpdate(sql)
    .setQueryTimeout(timeoutSeconds)
    .bindValues(values)
    .execute()
}

inline fun <reified T : Any> UpdateStatement.query(
  handle: Handle,
  timeoutSeconds: Int
): List<T> {
  return handle.createQuery(sql)
    .setQueryTimeout(timeoutSeconds)
    .bindValues(values)
    .mapResults<T>(update = update)
    .list()
}

inline fun <reified T : Any> UpdateStatement.queryFirst(
  handle: Handle,
  timeoutSeconds: Int
): T? {
  return query<T>(handle, timeoutSeconds).firstOrNull()
}

fun DeleteStatement.execute(
  handle: Handle,
  timeoutSeconds: Int
): Int {
  return handle.createUpdate(sql)
    .setQueryTimeout(timeoutSeconds)
    .bindValues(values)
    .execute()
}

fun Handle.nextValue(sequence: String, timeout: Int): Long {
  return createQuery("SELECT NEXTVAL(?::regclass)").bind(0, sequence)
    .setQueryTimeout(timeout)
    .mapTo(Long::class.java).first()
}
