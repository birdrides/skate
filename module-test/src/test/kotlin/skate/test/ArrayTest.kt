package skate.test

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import skate.TableName
import skate.eq
import skate.execute
import skate.executeRaw
import skate.generate
import skate.generator.Postgresql
import skate.insert
import skate.queryFirst
import skate.selectAll
import skate.values
import skate.where
import java.util.UUID

class ArrayTest : AbstractTest() {

  private val dialect = Postgresql()

  @BeforeEach
  fun setUp() {
    db.executeRaw(DROP_TABLE_SQL)
    db.executeRaw(CREATE_TABLE_SQL)
  }

  @Test
  fun `insert and query sanity check`() {
    val entity = MyArrayEntity(
      UUID.randomUUID(),
      ints = listOf(1, 2),
      longs = listOf(Long.MIN_VALUE, Long.MAX_VALUE),
      floats = listOf(1.0f, 2.0f),
      doubles = listOf(1.0, 2.0)
    )

    MyArrayEntity::class
      .insert()
      .values(entity)
      .generate(dialect)
      .execute(db)

    val result = MyArrayEntity::class
      .selectAll()
      .where(MyArrayEntity::id eq entity.id)
      .generate(dialect)
      .queryFirst<MyArrayEntity>(db)

    assertThat(entity).isEqualTo(result)
  }

  companion object {
    private const val TABLE = "my_array_entity"
    private const val CREATE_TABLE_SQL =
      """
        CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
        CREATE EXTENSION IF NOT EXISTS "postgis";
        CREATE TABLE $TABLE (
          id UUID NOT NULL PRIMARY KEY,
          ints integer[] NOT NULL default array[]::integer[],
          longs bigint[] NOT NULL default array[]::bigint[],
          floats real[] NOT NULL default array[]::real[],
          doubles double precision[] NOT NULL default array[]::double precision[]
        );
      """

    private const val DROP_TABLE_SQL = "DROP TABLE IF EXISTS $TABLE;"
  }

  @TableName(TABLE)
  data class MyArrayEntity(
    val id: UUID,
    val ints: List<Int> = listOf(),
    val longs: List<Long> = listOf(),
    val floats: List<Float> = listOf(),
    val doubles: List<Double> = listOf()
  )
}
