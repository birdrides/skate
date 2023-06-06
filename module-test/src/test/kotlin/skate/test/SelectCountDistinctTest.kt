package skate.test

import skate.TableName
import skate.generate
import skate.generator.Postgresql
import skate.insert
import skate.selectCountDistinct
import skate.values
import skate.where
import java.util.UUID
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import skate.eq
import skate.execute
import skate.executeRaw
import skate.queryFirst

class SelectCountDistinctTest : AbstractTest() {

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
  fun `insert and count`() {
    val a = MyCountEntity(UUID.randomUUID(), "one")
    val b = MyCountEntity(UUID.randomUUID(), "one")
    val c = MyCountEntity(UUID.randomUUID(), "two")

    insert(a)
    insert(b)
    insert(c)

    val result = MyCountEntity::class
      .selectCountDistinct(MyCountEntity::name)
      .where(MyCountEntity::name eq "one")
      .generate(dialect)
      .queryFirst<Int>(db)

    assertThat(result).isEqualTo(1)
  }

  private fun insert(any: MyCountEntity) {
    MyCountEntity::class
      .insert()
      .values(any)
      .generate(dialect)
      .execute(db)
  }

  companion object {
    const val TABLE_NAME = "my_count_entity"
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
  data class MyCountEntity(
    val id: UUID,
    val name: String
  )
}
