package skate.generator

import skate.Delete
import skate.Insert
import skate.InsertConflictAction
import skate.InsertConflictActionWithProjections
import skate.InsertWithProjections
import skate.Query
import skate.Update

interface Dialect {
  fun generate(query: Query): SelectStatement
  fun <T : Any> generate(insert: Insert<T>): InsertStatement<T>
  fun <T : Any> generate(insert: InsertWithProjections<T>): InsertStatement<T>
  fun <T : Any> generate(insertConflictAction: InsertConflictAction<T>): InsertStatement<T>
  fun <T : Any> generate(insertConflictActionWithProjections: InsertConflictActionWithProjections<T>): InsertStatement<T>
  fun <T : Any> generate(update: Update<T>): UpdateStatement
  fun <T : Any> generate(delete: Delete<T>): DeleteStatement

  companion object {
    val POSTGRESQL: Dialect = Postgresql()
  }
}
