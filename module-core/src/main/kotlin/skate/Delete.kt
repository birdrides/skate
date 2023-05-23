package skate

/**
 * Support DELETE FROM table WHERE ...
 */
data class Delete<T : Any>(
  val table: Table<T>,
  val whereClause: Expression<Boolean>? = null
)
