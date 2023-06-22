package skate.configuration

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import net.postgis.jdbc.PGbox2d
import net.postgis.jdbc.PGbox3d
import net.postgis.jdbc.PGgeometry
import org.postgresql.PGConnection
import java.util.Properties
import javax.sql.DataSource

interface DataSourceFactory {
  /**
   * Create a [DataSource] from [databaseConfig] and [poolConfig]
   *
   * @param databaseConfig [DatabaseConfig] to use
   * @param poolConfig [ConnectionPoolConfig] to use
   */
  fun create(databaseConfig: DatabaseConfig, poolConfig: ConnectionPoolConfig): DataSource

  companion object {

    val HIKARI = object : DataSourceFactory {

      override fun create(databaseConfig: DatabaseConfig, poolConfig: ConnectionPoolConfig): DataSource {
        val properties = Properties().apply {
          setProperty("dataSourceClassName", "org.postgresql.ds.PGSimpleDataSource")
          setProperty("dataSource.user", databaseConfig.user)
          setProperty("dataSource.password", databaseConfig.password)
          setProperty("dataSource.databaseName", databaseConfig.database)
          setProperty("dataSource.serverName", databaseConfig.host)
          setProperty("dataSource.prepareThreshold", databaseConfig.prepareThreshold.toString())
          setProperty("dataSource.portNumber", databaseConfig.port.toString())
        }
        val source = HikariDataSource(HikariConfig(properties)).apply {
          connectionTimeout = poolConfig.connectionTimeout
          maximumPoolSize = poolConfig.maximumPoolSize
          maxLifetime = poolConfig.maxLifetime
          minimumIdle = poolConfig.minimumIdle
          idleTimeout = poolConfig.idleTimeout
          leakDetectionThreshold = poolConfig.leakDetectionThreshold
          poolConfig.transactionIsolation?.let {
            transactionIsolation = it
          }
        }
        source.connection.use {
          it.unwrap(PGConnection::class.java).apply {
            addDataType("geometry", PGgeometry::class.java)
            addDataType("box3d", PGbox3d::class.java)
            addDataType("box2d", PGbox2d::class.java)
          }
        }
        return source
      }
    }
  }
}
