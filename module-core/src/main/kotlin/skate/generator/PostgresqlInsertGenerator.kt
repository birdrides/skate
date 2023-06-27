package skate.generator

import skate.InsertConflict
import skate.InsertConflictAction
import skate.InsertConflictDoNothing
import skate.InsertConflictUpdate
import skate.InsertSource
import skate.InsertSourceFromData
import skate.InsertSourceFromSelect
import skate.InsertWithoutSource
import skate.Projection

data class InsertFragment<T>(
  val sql: String,
  val rows: List<T> = listOf(),
  val values: List<Any> = listOf()
)

/**
 * Parts of Postgresql dealing with inserts; separate for ease of maintenance rather than any technical reason
 */
internal class PostgresqlInsertGenerator(
  private val owner: Postgresql
) {
  private fun <T : Any> generateInsertSource(
    setup: InsertWithoutSource<T>,
    source: InsertSource<T>
  ): InsertFragment<T> {
    return when (source) {
      is InsertSourceFromData<T> -> generateExplicitInsert(setup, source)
      is InsertSourceFromSelect<T> -> generateSelectForInsert(source)
    }
  }

  private fun <T : Any> generateExplicitInsert(
    setup: InsertWithoutSource<T>,
    insert: InsertSourceFromData<T>
  ): InsertFragment<T> {
    if (insert.rows.isEmpty()) {
      throw IllegalArgumentException("cannot insert without values")
    }

    val prefix = setup.table.sql()

    val columnPlaceholders = (0 until insert.rows.count()).map { index ->
      setup.fields.joinToString(", ", "(", ")") { field ->
        "#$prefix$index.${field.attribute.property.name}"
      }
    }

    return InsertFragment(" VALUES " + columnPlaceholders.joinToString(", "), insert.rows)
  }

  private fun <T : Any> generateInsertConflict(insertConflict: InsertConflict<T>): Fragment {
    val targets = insertConflict.conflictTargets.joinToString(", ") { it.sql() }
    val targetStatement = if (targets.isNullOrEmpty()) "DO" else "($targets) DO"
    return Fragment("ON CONFLICT $targetStatement")
  }

  private fun <T : Any> generateInsertConflictAction(insertConflictAction: InsertConflictAction<T>): Fragment {
    return when (insertConflictAction) {
      is InsertConflictDoNothing<T> -> generateInsertConflictDoNothing()
      is InsertConflictUpdate<T> -> generateInsertConflictUpdate(insertConflictAction)
      else -> throw IllegalArgumentException("Unsupported conflict action")
    }
  }

  private fun <T : Any> generateInsertConflictUpdate(insertConflictUpdate: InsertConflictUpdate<T>): Fragment {
    if (insertConflictUpdate.updateFields.isEmpty()) {
      throw IllegalArgumentException("Cannot update without values")
    }

    val updatedFields = insertConflictUpdate.updateFields
      .joinToString(", ") { "${it.sql()} = EXCLUDED.${it.sql()}" }

    return Fragment("UPDATE SET $updatedFields")
  }

  private fun generateInsertConflictDoNothing(): Fragment {
    return Fragment("NOTHING")
  }

  private fun <T : Any> generateSelectForInsert(insert: InsertSourceFromSelect<T>): InsertFragment<T> {
    val select = owner.generate(insert.sourceQuery)
    return InsertFragment(" (" + select.sql + ")", listOf(), select.values)
  }

  fun <T : Any> generateInsert(
    setup: InsertWithoutSource<T>,
    source: InsertSource<T>,
    projections: List<Projection>?,
    conflictAction: InsertConflictAction<T>?
  ): InsertStatement<T> {
    val table = owner.generateTableAsNoun(setup.table).sql
    val columnNames = setup.fields.map { it.column.sql() }

    val fromFragment = generateInsertSource(setup, source)

    val conflictFragments = conflictAction?.let {
      listOf(
        generateInsertConflict(it.conflict),
        generateInsertConflictAction(it)
      )
    }

    val projectionFragments = projections?.map { owner.generate(it) }

    return InsertStatement(
      "INSERT INTO $table " +
        "(${columnNames.joinToString(", ")})" +
        fromFragment.sql +
        if (conflictFragments != null) {
          " ${conflictFragments.joinToString(" ") { it.sql }}"
        } else {
          ""
        } +
        if (projectionFragments != null) " RETURNING " + projectionFragments.joinToString(", ") { it.sql } else "",
      setup.table.sql(),
      fromFragment.rows,
      fromFragment.values + (
        conflictFragments?.flatMap { it.values }
          ?: listOf()
        ) + (projectionFragments?.flatMap { it.values } ?: listOf())
    )
  }
}
