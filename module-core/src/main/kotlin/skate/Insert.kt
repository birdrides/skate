package skate

data class InsertWithoutSource<T : Any>(
  val table: Table<T>,
  val fields: List<InsertField<T>>
)

data class Insert<T : Any>(
  val insert: InsertWithoutSource<T>,
  val source: InsertSource<T>
)

data class InsertWithProjections<T : Any>(
  val insert: Insert<T>,
  val projections: List<Projection>? = null
)

data class InsertField<T : Any>(
  val column: Column<T, *>,
  val attribute: Attribute<T, *>
)

data class InsertConflict<T : Any>(
  val insert: Insert<T>,
  val conflictTargets: List<Column<T, *>>
)

interface InsertConflictAction<T : Any> {
  val conflict: InsertConflict<T>
}

data class InsertConflictDoNothing<T : Any>(
  override val conflict: InsertConflict<T>
) : InsertConflictAction<T>

data class InsertConflictUpdate<T : Any>(
  override val conflict: InsertConflict<T>,
  val updateFields: List<Column<T, *>>
) : InsertConflictAction<T>

data class InsertConflictActionWithProjections<T : Any>(
  val insertConflictAction: InsertConflictAction<T>,
  val projections: List<Projection>? = null
)

// INSERT INTO blah (foo, bar) {InsertSource}
sealed class InsertSource<T>

// INSERT INTO blah (foo, bar) VALUES (rows_0_x, rows_0_y), (rows_1_x, rows_1_y), ...
//                             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
data class InsertSourceFromData<T>(val rows: List<T>) : InsertSource<T>()

// INSERT INTO blah (foo, bar) (SELECT foo, bar FROM ... WHERE ... )
//                             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
data class InsertSourceFromSelect<T>(val sourceQuery: Query) : InsertSource<T>()

data class InsertFromSelect<T : Any>(
  val table: Table<T>,
  val fields: List<InsertField<T>>,
  val sourceQuery: Query,
  val projections: List<Projection>? = null
)
