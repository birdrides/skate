package skate.configuration

import java.util.concurrent.TimeUnit

data class ConnectionPoolConfig(
  val connectionTimeout: Long = TimeUnit.SECONDS.toMillis(30),
  val maximumPoolSize: Int = 2,
  val minimumIdle: Int = maximumPoolSize,
  val maxLifetime: Long = TimeUnit.MINUTES.toMillis(30),
  val idleTimeout: Long = TimeUnit.MINUTES.toMillis(10),
  val leakDetectionThreshold: Long = 60_000L,
  val transactionIsolation: String? = null
)

