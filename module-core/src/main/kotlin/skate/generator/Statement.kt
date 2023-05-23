package skate.generator

import skate.Query
import skate.Update

data class Fragment(
  val sql: String,
  val values: List<Any> = listOf()
)

data class SelectStatement(
  val sql: String,
  val values: List<Any> = listOf(),
  val query: Query? = null
)

data class UpdateStatement(
  val sql: String,
  val values: List<Any> = listOf(),
  val update: Update<*>? = null
)

data class DeleteStatement(
  val sql: String,
  val values: List<Any> = listOf()
)

data class InsertStatement<T : Any>(
  val sql: String,
  val prefix: String,
  val rows: List<T> = listOf(),
  val values: List<Any> = listOf()
)
