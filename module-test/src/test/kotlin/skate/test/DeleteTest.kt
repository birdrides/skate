package skate.test

import skate.TableName
import skate.delete
import skate.eq
import skate.generate
import skate.insert
import skate.selectAll
import skate.values
import skate.where
import java.util.UUID
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import skate.execute
import skate.executeRaw
import skate.query
import skate.queryFirst

class DeleteTest : AbstractTest() {

  @BeforeEach
  fun setUp() {
    db.executeRaw(CREATE_TABLE_SQL)
  }

  @AfterEach
  fun tearDown() {
    db.executeRaw(DROP_TABLE_SQL)
  }

  @Test
  fun `insert and delete happy path`() {
    val one = MyDeleteEntity(UUID.randomUUID(), "one")
    val two = MyDeleteEntity(UUID.randomUUID(), "two")

    insert(one)
    insert(two)

    assertThat(query(one.id)).isEqualTo(one)
    assertThat(query(two.id)).isEqualTo(two)

    MyDeleteEntity::class
      .delete()
      .where(MyDeleteEntity::id eq one.id)
      .generate(db.dialect)
      .execute(db)

    assertThat(query(one.id)).isEqualTo(null)
    assertThat(query(two.id)).isEqualTo(two)
  }

  @Test
  fun `insert and delete many happy path`() {
    val one = MyDeleteEntity(UUID.randomUUID(), "one")
    val two = MyDeleteEntity(UUID.randomUUID(), "d")
    val three = MyDeleteEntity(UUID.randomUUID(), "d")
    val four = MyDeleteEntity(UUID.randomUUID(), "d")

    insert(one)
    insert(two)
    insert(three)
    insert(four)

    assertThat(query("d")).hasSize(3)

    MyDeleteEntity::class
      .delete()
      .where(MyDeleteEntity::name eq "d")
      .generate(db.dialect)
      .execute(db)

    assertThat(query(two.id)).isEqualTo(null)
    assertThat(query(three.id)).isEqualTo(null)
    assertThat(query(four.id)).isEqualTo(null)
    assertThat(query("d")).hasSize(0)
  }

  companion object {
    private const val CREATE_TABLE_SQL =
      """
        CREATE TABLE my_delete_entity (
          id UUID NOT NULL PRIMARY KEY,
          name TEXT NOT NULL
        );
      """

    private const val DROP_TABLE_SQL = "DROP TABLE my_delete_entity;"
  }

  private fun insert(any: MyDeleteEntity) {
    MyDeleteEntity::class
      .insert()
      .values(any)
      .generate(db.dialect)
      .execute(db)
  }

  private fun query(id: UUID): MyDeleteEntity? {
    return MyDeleteEntity::class
      .selectAll()
      .where(MyDeleteEntity::id eq id)
      .generate(db.dialect)
      .queryFirst(db)
  }

  private fun query(name: String): List<MyDeleteEntity> {
    return MyDeleteEntity::class
      .selectAll()
      .where(MyDeleteEntity::name eq name)
      .generate(db.dialect)
      .query(db)
  }

  @TableName("my_delete_entity")
  data class MyDeleteEntity(
    val id: UUID,
    val name: String
  )
}
