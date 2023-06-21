package skate.test

import skate.TableName
import skate.all
import skate.and
import skate.coalesce
import skate.column
import skate.columnAs
import skate.generate
import skate.generator.Postgresql
import skate.insert
import skate.values
import skate.where
import java.util.UUID
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import skate.Value
import skate.eq
import skate.execute
import skate.executeRaw
import skate.groupBy
import skate.join
import skate.max
import skate.on
import skate.query
import skate.select

class JoinTest : AbstractTest() {

  private val dialect = Postgresql()

  @BeforeEach
  fun setUp() {
    db.executeRaw(CREATE_TABLE_SQL)
  }

  @AfterEach
  fun tearDown() {
    db.executeRaw(DROP_TABLE_SQL)
  }

  @Test
  fun `join against subquery`() {
    val b3 = Vehicle(model = "b3", distance = 100)

    listOf(
      b3,
      // Ignored because lower distance than b3MaxDistance vehicle
      b3.copy(id = UUID.randomUUID(), distance = 0),
      // Ignored because B2 was not requested model
      Vehicle(model = "b2", distance = 101)
    ).let(::insert)

    val result = Vehicle::class
      .select(Vehicle::class.all())
      .join(
        Vehicle::class
          .select(Vehicle::distance.max("distance"))
          .where(Vehicle::model eq b3.model!!)
          .groupBy(Vehicle::model)
          .on(
            on = and(
              Vehicle::distance eq Vehicle::distance.columnAs("agg"),
              coalesce(Vehicle::model.column(), Value("")) eq Vehicle::model.columnAs("agg")
            ),
            alias = "agg"
          )
      )
      .generate(dialect)
      .query<Vehicle>(db)

    assertThat(result).isEqualTo(listOf(b3))
  }

  private fun insert(entities: List<Vehicle>) {
    Vehicle::class
      .insert()
      .values(entities)
      .generate(dialect)
      .execute(db)
  }

  companion object {
    const val TABLE_NAME = "vehicles"
    private const val CREATE_TABLE_SQL =
      """
        CREATE TABLE $TABLE_NAME (
          id UUID NOT NULL PRIMARY KEY,
          distance INT NOT NULL,
          model TEXT
        );
      """

    private const val DROP_TABLE_SQL = "DROP TABLE $TABLE_NAME;"
  }

  @TableName(TABLE_NAME)
  data class Vehicle(
    val id: UUID = UUID.randomUUID(),
    val distance: Int,
    val model: String?
  )
}
