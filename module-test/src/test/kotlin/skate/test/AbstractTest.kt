package skate.test

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.MapperFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.PropertyNamingStrategies
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import skate.Database
import skate.configuration.ConnectionPoolConfig
import skate.configuration.DatabaseConfig
import skate.internal.mapper.column.jsonb.JavaTimeModule

abstract class AbstractTest {

  companion object {
    val jackson: ObjectMapper = ObjectMapper().apply {
      propertyNamingStrategy = PropertyNamingStrategies.SNAKE_CASE
      configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
      configure(SerializationFeature.WRITE_ENUMS_USING_TO_STRING, true)
      configure(DeserializationFeature.READ_ENUMS_USING_TO_STRING, true)
      configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS, true)
      configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
      registerKotlinModule()
      registerModule(JavaTimeModule())
      findAndRegisterModules()
    }
  }

  /**
   * This configuration is from docker-compose.yml
   */
  protected val db by lazy {
    Database.create(
      config = DatabaseConfig(
        host = "localhost",
        database = "local",
        user = "local",
        password = "local",
        port = 5432
      ),
      poolConfig = ConnectionPoolConfig(
        maximumPoolSize = 2,
        minimumIdle = 1,
        maxLifetime = 300000,
        connectionTimeout = 30000,
        idleTimeout = 600000
      ),
      jackson = jackson
    )
  }
}
