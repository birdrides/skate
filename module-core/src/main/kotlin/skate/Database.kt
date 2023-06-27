package skate

import com.fasterxml.jackson.databind.ObjectMapper
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.KotlinPlugin
import org.jdbi.v3.core.kotlin.useSequence
import org.jdbi.v3.core.statement.HashPrefixSqlParser
import net.postgis.jdbc.geometry.Geometry
import net.postgis.jdbc.geometry.MultiPolygon
import net.postgis.jdbc.geometry.Point
import net.postgis.jdbc.geometry.Polygon
import org.jdbi.v3.postgres.UUIDArgumentFactory
import skate.configuration.ConnectionPoolConfig
import skate.configuration.DataSourceFactory
import skate.configuration.DatabaseConfig
import skate.configuration.JdbiConfigurator
import skate.generator.DeleteStatement
import skate.generator.Dialect
import skate.generator.InsertStatement
import skate.generator.SelectStatement
import skate.generator.UpdateStatement
import skate.internal.mapper.column.registerJson
import skate.internal.mapper.column.registerPGArrays
import skate.internal.mapper.column.registerPostgisGeometry

/**
 * A database abstraction layer that uses [Jdbi] under the hood
 */
interface Database {
  /**
   * The SQL dialect which is used to generate SQL statements
   */
  val dialect: Dialect

  /**
   * The default query timeout in seconds for all queries
   */
  val defaultQueryTimeoutSeconds: Int

  /**
   * The [Jdbi] instance
   */
  val jdbi: Jdbi

  companion object {
    /**
     * Create a Skate [Database] instance
     *
     * @param config The database config
     * @param poolConfig The config for the connection pool
     * @param factory The factory to use to create a specific data source, defaults to [DataSourceFactory.HIKARI]
     * @param dialect The SQL dialect to use, defaults to [Dialect.POSTGRESQL]
     * @param configurator The [JdbiConfigurator] to use, defaults to [JdbiConfigurator.NOOP]
     * @param jackson The [ObjectMapper] to use for JSONB serialization and deserialization
     */
    fun create(
      config: DatabaseConfig,
      poolConfig: ConnectionPoolConfig,
      factory: DataSourceFactory = DataSourceFactory.HIKARI,
      dialect: Dialect = Dialect.POSTGRESQL,
      configurator: JdbiConfigurator = JdbiConfigurator.NOOP,
      jackson: ObjectMapper
    ): Database {
      val dataSource = factory.create(config, poolConfig)
      return object : Database {
        override val dialect: Dialect = dialect
        override val defaultQueryTimeoutSeconds: Int = configurator.queryTimeoutSeconds
        override val jdbi: Jdbi = Jdbi.create(dataSource).apply {
          registerArgument(UUIDArgumentFactory())
          installPlugin(KotlinPlugin())
          registerPostgisGeometry<Geometry>()
          registerPostgisGeometry<Point>()
          registerPostgisGeometry<Polygon>()
          registerPostgisGeometry<MultiPolygon>()
          registerJson<Map<String, Any>>(jackson)
          registerJson<LinkedHashMap<String, Any>>(jackson)
          registerPGArrays()
          setSqlParser(HashPrefixSqlParser())
          configurator.configure(this)
        }
      }
    }
  }
}

inline fun <reified T : Any> Database.queryRaw(
  sql: String,
  values: List<Any>,
  query: Query? = null,
): List<T> {
  return jdbi.queryRaw(sql, values, query, defaultQueryTimeoutSeconds)
}

inline fun <reified T : Any> SelectStatement.query(
  context: Database,
  timeoutSeconds: Int? = null
): List<T> {
  return context.jdbi.queryRaw(sql, values, query, timeoutSeconds ?: context.defaultQueryTimeoutSeconds)
}

inline fun <reified T : Any> SelectStatement.queryFirst(
  context: Database,
  timeoutSeconds: Int? = null
): T? {
  return query<T>(context, timeoutSeconds).firstOrNull()
}

inline fun <reified T : Any, K> SelectStatement.queryMap(
  context: Database,
  timeoutSeconds: Int? = null,
  keySelector: (T) -> K
): Map<K, T> {
  return query<T>(context, timeoutSeconds).associateBy(keySelector)
}

inline fun <reified T : Any, K> SelectStatement.queryGroup(
  context: Database,
  timeoutSeconds: Int? = null,
  keySelector: (T) -> K
): Map<K, List<T>> {
  return query<T>(context, timeoutSeconds).groupBy(keySelector)
}

inline fun <reified T : Any> SelectStatement.queryEach(
  context: Database,
  timeoutSeconds: Int? = null,
  crossinline action: (T) -> Unit
) {
  return context.jdbi.useHandle<Exception> { handle ->
    handle
      .createQuery(sql)
      .setQueryTimeout(timeoutSeconds ?: context.defaultQueryTimeoutSeconds)
      .bindValues(values)
      .mapResults<T>(query = null)
      .useSequence { it.forEach(action) }
  }
}

inline fun <reified T : Any> SelectStatement.queryEach(
  context: Database,
  fetchSize: Int,
  timeoutSeconds: Int? = null,
  crossinline action: (T) -> Unit
) {
  return context.jdbi.useHandle<Exception> { handle ->
    handle
      .createQuery(sql)
      // To avoid big chunk of memory being fetched.
      // This is useful for doing chunk streaming of results when exhausting memory could be a problem.
      .setFetchSize(fetchSize)
      // Allow concurrent update, this will allow the update methods to be called on the result
      .concurrentUpdatable()
      .setQueryTimeout(timeoutSeconds ?: context.defaultQueryTimeoutSeconds)
      .bindValues(values)
      .mapResults<T>(query = null)
      .useSequence { it.forEach(action) }
  }
}

inline fun <reified T : Any> InsertStatement<T>.query(
  context: Database,
  timeoutSeconds: Int? = null
): List<T> {
  return context.jdbi.withHandle<List<T>, Exception> {
    query(it, timeoutSeconds ?: context.defaultQueryTimeoutSeconds)
  }
}

inline fun <reified T : Any> InsertStatement<T>.queryFirst(
  context: Database,
  timeoutSeconds: Int? = null
): T? {
  return query(context, timeoutSeconds).firstOrNull()
}

inline fun <reified T : Any> UpdateStatement.query(
  context: Database,
  timeoutSeconds: Int? = null
): List<T> {
  // Can't be ::query like other in this file; no generic on UpdateStatement to drive type inference
  return context.jdbi.withHandle<List<T>, Exception> {
    query(it, timeoutSeconds ?: context.defaultQueryTimeoutSeconds)
  }
}

inline fun <reified T : Any> UpdateStatement.queryFirst(
  context: Database,
  timeoutSeconds: Int? = null
): T? {
  return query<T>(context, timeoutSeconds).firstOrNull()
}

fun InsertStatement<*>.execute(
  context: Database,
  timeoutSeconds: Int? = null
): Int {
  return context.execute(timeoutSeconds) { handle, timeout ->
    execute(handle, timeout)
  }
}

fun UpdateStatement.execute(
  context: Database,
  timeoutSeconds: Int? = null
): Int {
  return context.execute(timeoutSeconds) { handle, timeout ->
    execute(handle, timeout)
  }
}

fun DeleteStatement.execute(
  context: Database,
  timeoutSeconds: Int? = null
): Int {
  return context.execute(timeoutSeconds) { handle, timeout ->
    execute(handle, timeout)
  }
}

private fun Database.execute(
  timeoutSeconds: Int?,
  handler: (Handle, Int) -> Int
): Int {
  return jdbi.withHandle<Int, Exception> {
    handler(it, timeoutSeconds ?: defaultQueryTimeoutSeconds)
  }
}

fun Database.executeRaw(
  sql: String
): IntArray {
  return jdbi.withHandle<IntArray, Exception> { handle ->
    handle.createScript(sql).execute()
  }
}

fun Database.nextValue(
  sequence: String
): Long {
  return jdbi.withHandle<Long, Exception> { it.nextValue(sequence, defaultQueryTimeoutSeconds) }
}

