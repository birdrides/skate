package skate.test

import skate.TableName
import skate.asc
import skate.case
import skate.cast
import skate.column
import skate.eq
import skate.generate
import skate.insert
import skate.isIn
import skate.orderBy
import skate.selectAll
import skate.then
import skate.to
import skate.update
import skate.values
import skate.where
import java.util.UUID
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import skate.TypeSpec
import skate.execute
import skate.executeRaw
import skate.query

class CaseTest : AbstractTest() {

  @BeforeEach
  fun setUp() {
    db.executeRaw(CREATE_TABLE_SQL)
  }

  @AfterEach
  fun tearDown() {
    db.executeRaw(DROP_TABLE_SQL)
  }

  @Test
  fun `case statement can be used as an expression`() {
    val one = CaseTestEntity(UUID.randomUUID(), "1")
    val two = CaseTestEntity(UUID.randomUUID(), "2")
    val three = CaseTestEntity(UUID.randomUUID(), "3")

    insert(one, two, three)

    CaseTestEntity::class
      .update(
        CaseTestEntity::name to case(
          CaseTestEntity::name.eq("1").then(CaseTestEntity::id.column().cast(TypeSpec("text"))),
          fallback = "fallback"
        )
      )
      .where(
        case(
          CaseTestEntity::name.isIn(listOf("1", "2")).then(true)
        )
      )
      .generate(db.dialect)
      .execute(db)

    val result = CaseTestEntity::class
      .selectAll()
      .orderBy(CaseTestEntity::name.asc())
      .generate(db.dialect)
      .query<CaseTestEntity>(db)

    assertThat(result).isEqualTo(
      listOf(
        three,
        one.copy(name = one.id.toString()),
        two.copy(name = "fallback")
      )
    )
  }

  private fun insert(vararg entities: CaseTestEntity) {
    CaseTestEntity::class
      .insert()
      .values(entities.toList())
      .generate(db.dialect)
      .execute(db)
  }

  companion object {
    const val TABLE_NAME = "case_test_entities"
    private const val CREATE_TABLE_SQL =
      """
        CREATE TABLE $TABLE_NAME (
          id UUID NOT NULL PRIMARY KEY,
          name TEXT NOT NULL
        );
      """

    private const val DROP_TABLE_SQL = "DROP TABLE $TABLE_NAME;"
  }

  @TableName(TABLE_NAME)
  data class CaseTestEntity(
    val id: UUID,
    val name: String
  )
}
