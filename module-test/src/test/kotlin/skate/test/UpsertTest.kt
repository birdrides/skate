package skate.test

import net.postgis.jdbc.geometry.Point
import skate.TableName
import skate.doNothing
import skate.eq
import skate.generate
import skate.insert
import skate.onConflict
import skate.selectAll
import skate.selectCount
import skate.update
import skate.values
import skate.where
import java.util.UUID
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import skate.ColumnName
import skate.execute
import skate.executeRaw
import skate.generator.toUnderscore
import skate.queryFirst

class UpsertIntegrationTest : AbstractTest() {

  @BeforeEach
  fun setUp() {
    db.executeRaw(CREATE_TABLE_SQL)
  }

  @AfterEach
  fun tearDown() {
    db.executeRaw(DROP_TABLE_SQL)
  }

  val uuidVal = UUID.randomUUID()

  @Test
  fun `update on conflict inserts record if none exist`() {
    val originalEntity = MyEntity(UUID.randomUUID(), "original", 1.0, null, Point(1.0, 10.0, 10.0))

    MyEntity::class
      .insert()
      .values(originalEntity)
      .onConflict(MyEntity::id).update(MyEntity::name)
      .generate(db.dialect)
      .execute(db)

    val insertedEntity = MyEntity::class
      .selectAll()
      .where(MyEntity::id eq originalEntity.id)
      .generate(db.dialect)
      .queryFirst<MyEntity>(db)

    assertEquals(originalEntity, insertedEntity)
  }

  @Test
  fun `update on conflict updates record if one exists`() {
    val originalPoint = Point(1.0, 10.0, 10.0).also {
      it.setSrid(4326)
    }
    val originalEntity = MyEntity(UUID.randomUUID(), "original", 1.0, uuidVal, originalPoint)
    val updatedEntity = MyEntity(originalEntity.id, "updated", 1.0, uuidVal, originalPoint)

    MyEntity::class
      .insert()
      .values(originalEntity)
      .generate(db.dialect)
      .execute(db)

    MyEntity::class
      .insert()
      .values(updatedEntity)
      .onConflict(MyEntity::id).update(MyEntity::name)
      .generate(db.dialect)
      .execute(db)

    val count = MyEntity::class
      .selectCount()
      .generate(db.dialect)
      .queryFirst<Int>(db)

    assertEquals(1, count)

    val result = MyEntity::class
      .selectAll()
      .where(MyEntity::id eq originalEntity.id)
      .generate(db.dialect)
      .queryFirst<MyEntity>(db)

    assertEquals(updatedEntity, result)
    assertEquals(originalPoint, result?.location)
  }

  @Test
  fun `do nothing on conflict leaves record alone`() {
    val originalEntity = MyEntity(UUID.randomUUID(), "original", 1.0, uuidVal, Point(1.0, 10.0, 10.0))
    val updatedEntity = MyEntity(originalEntity.id, "updated", 2.0, uuidVal, Point(1.0, 10.0, 10.0))

    MyEntity::class
      .insert()
      .values(originalEntity)
      .generate(db.dialect)
      .execute(db)

    MyEntity::class
      .insert()
      .values(updatedEntity)
      .onConflict(MyEntity::id).doNothing()
      .generate(db.dialect)
      .execute(db)

    val count = MyEntity::class
      .selectCount()
      .generate(db.dialect)
      .queryFirst<Int>(db)

    assertEquals(1, count)

    val result = MyEntity::class
      .selectAll()
      .where(MyEntity::id eq originalEntity.id)
      .generate(db.dialect)
      .queryFirst<MyEntity>(db)

    assertEquals(originalEntity, result)
  }

  @Test
  fun testSingleLetterUnderscore() {
    val underscore = "fRatio".toUnderscore()

    assertEquals("f_ratio", underscore)
  }

  companion object {
    private const val CREATE_TABLE_SQL =
      """
        CREATE TABLE my_entity (
          id UUID NOT NULL PRIMARY KEY,
          name TEXT NOT NULL,
          f_ratio double precision NOT NULL,
          uuid_val UUID,
          location geometry(PointZ, 4326) NOT NULL
        );
      """

    private const val DROP_TABLE_SQL = "DROP TABLE my_entity;"
  }
}

@TableName("my_entity")
data class MyEntity(
  val id: UUID,
  val name: String,
  @ColumnName("f_ratio")
  val fratio: Double,
  val uuidVal: UUID? = null,
  val location: Point
)
