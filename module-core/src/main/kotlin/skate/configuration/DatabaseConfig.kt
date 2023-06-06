package skate.configuration

data class DatabaseConfig(
  val host: String,
  val database: String,
  val user: String,
  val password: String? = null,
  val prepareThreshold: Int = 0,
  val port: Int = 5432
)
