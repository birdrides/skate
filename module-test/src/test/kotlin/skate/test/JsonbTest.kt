package skate.test

import com.fasterxml.jackson.core.type.TypeReference
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import skate.TableName
import skate.eq
import skate.execute
import skate.executeRaw
import skate.generate
import skate.insert
import skate.internal.mapper.column.registerJson
import skate.queryFirst
import skate.selectAll
import skate.values
import skate.where
import java.util.UUID

class JsonbTest : AbstractTest() {

  @BeforeEach
  fun setUp() {
    db.jdbi.registerJson(jackson, object : TypeReference<List<Location>>() {})
    db.executeRaw(DROP_TABLE_SQL)
    db.executeRaw(CREATE_TABLE_SQL)
  }

  @Test
  fun `insert and query sanity check`() {
    val entity = MyJsonbEntity(
      UUID.randomUUID(),
      locations = listOf(Location(), Location()),
      location = Location(),
      uuids = listOf(UUID.randomUUID(), UUID.randomUUID())
    )

    MyJsonbEntity::class
      .insert()
      .values(entity)
      .generate(db.dialect)
      .execute(db)

    val result = MyJsonbEntity::class
      .selectAll()
      .where(MyJsonbEntity::id eq entity.id)
      .generate(db.dialect)
      .queryFirst<MyJsonbEntity>(db)

    assertThat(entity).isEqualTo(result)
  }

  companion object {
    private const val TABLE = "my_json_entity"
    private const val CREATE_TABLE_SQL =
      """
        CREATE TABLE $TABLE (
          id UUID NOT NULL PRIMARY KEY,
          uuids uuid[] NOT NULL default array[]::uuid[],
          locations jsonb DEFAULT '[]'::jsonb NOT NULL,
          location jsonb DEFAULT '{}'::jsonb NOT NULL
        );
      """

    private const val DROP_TABLE_SQL = "DROP TABLE IF EXISTS $TABLE;"
  }

  @TableName(TABLE)
  data class MyJsonbEntity(
    val id: UUID,
    val uuids: List<UUID> = listOf(),
    val locations: List<Location> = listOf(),
    val location: Location = Location()
  )

  data class Location(
    val id: UUID = UUID.randomUUID(),
    val name: String = ""
  )
}
