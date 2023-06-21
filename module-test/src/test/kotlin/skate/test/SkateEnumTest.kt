package skate.test

import skate.array
import skate.contains
import skate.eq
import skate.generate
import skate.insert
import skate.test.SkateTestEnumStel.BAG
import skate.test.SkateTestEnumStel.BAR
import skate.test.SkateTestEnumStel.FOO
import skate.nullable
import skate.selectAll
import skate.to
import skate.update
import skate.values
import skate.where
import java.util.UUID
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.Jdbi
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import skate.execute
import skate.executeRaw
import skate.internal.mapper.column.registerEnum
import skate.query
import skate.queryFirst

enum class SkateTestEnumStew {
  FOO,
  BAR,
  BAG;

  override fun toString(): String = name.toLowerCase()
}

data class SkateTestEnumWrapper(
  val id: UUID = UUID.randomUUID(),
  val item: SkateTestEnumStew
)

enum class SkateTestEnumStnew {
  FOO,
  BAR,
  BAG;

  override fun toString(): String = name.toLowerCase()
}

data class SkateTestNullableEnumWrapper(
  val id: UUID = UUID.randomUUID(),
  val item: SkateTestEnumStnew?
)

enum class SkateTestEnumStel {
  FOO,
  BAR,
  BAG;

  override fun toString(): String = name.toLowerCase()
}

data class SkateTestEnumList(
  val id: UUID = UUID.randomUUID(),
  val items: List<SkateTestEnumStel> = emptyList()
)

enum class SkateTestEnumStnel {
  FOO,
  BAR,
  BAG;

  override fun toString(): String = name.toLowerCase()
}

data class SkateTestNullableEnumList(
  val id: UUID = UUID.randomUUID(),
  val items: List<SkateTestEnumStnel>? = null
)

class SkateEnumTest : AbstractTest() {

  @BeforeEach
  fun beforeEach() {
    db.jdbi
      .registerEnum<SkateTestEnumStnel>("skate_test_enum_stnel")
      .registerEnum<SkateTestEnumStel>("skate_test_enum_stel")
      .registerEnum<SkateTestEnumStnew>("skate_test_enum_stnew")
      .registerEnum<SkateTestEnumStew>("skate_test_enum_stew")
  }

  @Test
  fun `Insert and query and update with non-nullable enum column`() {
    db.executeRaw(
      """
        DROP TABLE IF EXISTS skate_test_enum_wrapper;
        DROP TYPE IF EXISTS skate_test_enum_stew;
        CREATE TYPE skate_test_enum_stew AS ENUM ('foo', 'bar', 'bag');
        CREATE TABLE skate_test_enum_wrapper (
          id uuid NOT NULL DEFAULT uuid_generate_v4() PRIMARY KEY,
          item skate_test_enum_stew NOT NULL
        );
      """.trimIndent()
    )

    try {
      val a = SkateTestEnumWrapper(item = SkateTestEnumStew.FOO)
      val b = SkateTestEnumWrapper(item = SkateTestEnumStew.BAR)
      val c = SkateTestEnumWrapper(item = SkateTestEnumStew.BAG)

      val count: Int = SkateTestEnumWrapper::class.insert()
        .values(a, b, c)
        .generate(db.dialect)
        .execute(db)

      assertThat(count).isEqualTo(3)

      val loadedA = SkateTestEnumWrapper::class.selectAll()
        .where(SkateTestEnumWrapper::id eq a.id)
        .generate(db.dialect)
        .queryFirst<SkateTestEnumWrapper>(db)

      assertThat(loadedA).isNotNull()
      assertThat(loadedA!!.id).isEqualTo(a.id)
      assertThat(loadedA.item).isEqualTo(a.item)

      val loadedB = SkateTestEnumWrapper::class.selectAll()
        .where(SkateTestEnumWrapper::id eq b.id)
        .generate(db.dialect)
        .queryFirst<SkateTestEnumWrapper>(db)

      assertThat(loadedB).isNotNull()
      assertThat(loadedB!!.id).isEqualTo(b.id)
      assertThat(loadedB.item).isEqualTo(b.item)

      val loadedC = SkateTestEnumWrapper::class.selectAll()
        .where(SkateTestEnumWrapper::id eq c.id)
        .generate(db.dialect)
        .queryFirst<SkateTestEnumWrapper>(db)

      assertThat(loadedC).isNotNull()
      assertThat(loadedC!!.id).isEqualTo(c.id)
      assertThat(loadedC.item).isEqualTo(c.item)

      val affected = SkateTestEnumWrapper::class
        .update(
          SkateTestEnumWrapper::item to SkateTestEnumStew.BAG
        )
        .where(SkateTestEnumWrapper::id eq a.id)
        .generate(db.dialect)
        .execute(db)

      assertThat(affected).isEqualTo(1)

      val reloadedA = SkateTestEnumWrapper::class.selectAll()
        .where(SkateTestEnumWrapper::id eq a.id)
        .generate(db.dialect)
        .queryFirst<SkateTestEnumWrapper>(db)

      assertThat(reloadedA).isNotNull()
      assertThat(reloadedA!!.item).isEqualTo(SkateTestEnumStew.BAG)
    } finally {
      db.executeRaw(
        """
          DROP TABLE IF EXISTS skate_test_enum_wrapper;
          DROP TYPE IF EXISTS skate_test_enum_stew;
        """.trimIndent()
      )
    }
  }

  @Test
  fun `Insert and query and update with nullable enum column`() {
    db.executeRaw(
      """
        DROP TABLE IF EXISTS skate_test_nullable_enum_wrapper;
        DROP TYPE IF EXISTS skate_test_enum_stnew;

        CREATE TYPE skate_test_enum_stnew AS ENUM ('foo', 'bar', 'bag');
        CREATE TABLE skate_test_nullable_enum_wrapper (
          id uuid NOT NULL DEFAULT uuid_generate_v4() PRIMARY KEY,
          item skate_test_enum_stnew NULL
        );
      """.trimIndent()
    )

    try {
      val a = SkateTestNullableEnumWrapper(item = SkateTestEnumStnew.FOO)
      val b = SkateTestNullableEnumWrapper(item = SkateTestEnumStnew.BAR)
      val c = SkateTestNullableEnumWrapper(item = null)

      val count: Int = SkateTestNullableEnumWrapper::class.insert()
        .values(a, b, c)
        .generate(db.dialect)
        .execute(db)

      assertThat(count).isEqualTo(3)

      val loadedA = SkateTestNullableEnumWrapper::class.selectAll()
        .where(SkateTestNullableEnumWrapper::id eq a.id)
        .generate(db.dialect)
        .queryFirst<SkateTestNullableEnumWrapper>(db)

      assertThat(loadedA).isNotNull()
      assertThat(loadedA!!.id).isEqualTo(a.id)
      assertThat(loadedA.item).isEqualTo(a.item)

      val loadedB = SkateTestNullableEnumWrapper::class.selectAll()
        .where(SkateTestNullableEnumWrapper::id eq b.id)
        .generate(db.dialect)
        .queryFirst<SkateTestNullableEnumWrapper>(db)

      assertThat(loadedB).isNotNull()
      assertThat(loadedB!!.id).isEqualTo(b.id)
      assertThat(loadedB.item).isEqualTo(b.item)

      val loadedC = SkateTestNullableEnumWrapper::class.selectAll()
        .where(SkateTestNullableEnumWrapper::id eq c.id)
        .generate(db.dialect)
        .queryFirst<SkateTestNullableEnumWrapper>(db)

      assertThat(loadedC).isNotNull()
      assertThat(loadedC!!.id).isEqualTo(c.id)
      assertThat(loadedC.item).isEqualTo(c.item)

      val affected = SkateTestNullableEnumWrapper::class
        .update(
          SkateTestNullableEnumWrapper::item to SkateTestEnumStnew.BAG
        )
        .where(SkateTestNullableEnumWrapper::id eq a.id)
        .generate(db.dialect)
        .execute(db)

      assertThat(affected).isEqualTo(1)

      val reloadedA = SkateTestNullableEnumWrapper::class.selectAll()
        .where(SkateTestNullableEnumWrapper::id eq a.id)
        .generate(db.dialect)
        .queryFirst<SkateTestNullableEnumWrapper>(db)

      assertThat(reloadedA).isNotNull()
      assertThat(reloadedA!!.item).isEqualTo(SkateTestEnumStnew.BAG)

      val affected2 = SkateTestNullableEnumWrapper::class
        .update(
          SkateTestNullableEnumWrapper::item.nullable() to null
        )
        .where(SkateTestNullableEnumWrapper::id eq b.id)
        .generate(db.dialect)
        .execute(db)

      assertThat(affected2).isEqualTo(1)

      val reloadedB = SkateTestNullableEnumWrapper::class.selectAll()
        .where(SkateTestNullableEnumWrapper::id eq b.id)
        .generate(db.dialect)
        .queryFirst<SkateTestNullableEnumWrapper>(db)

      assertThat(reloadedB).isNotNull()
      assertThat(reloadedB!!.item).isNull()
    } finally {
      db.executeRaw(
        """
          DROP TABLE IF EXISTS skate_test_nullable_enum_wrapper;
          DROP TYPE IF EXISTS skate_test_enum_stnew;
        """.trimIndent()
      )
    }
  }

  @Test
  fun `Insert and query and update with non-nullable enum array column`() {
    db.executeRaw(
      """
        DROP TABLE IF EXISTS skate_test_enum_list;
        DROP TYPE IF EXISTS skate_test_enum_stel;
        CREATE TYPE skate_test_enum_stel AS ENUM ('foo', 'bar', 'bag');
        CREATE TABLE skate_test_enum_list (
          id uuid NOT NULL DEFAULT uuid_generate_v4() PRIMARY KEY,
          items skate_test_enum_stel[] NOT NULL
        );
      """.trimIndent()
    )

    try {

      val a = SkateTestEnumList(items = listOf(FOO))
      val b = SkateTestEnumList(items = listOf(BAR, BAG))
      val c = SkateTestEnumList(items = listOf())

      val count: Int = SkateTestEnumList::class.insert()
        .values(a, b, c)
        .generate(db.dialect)
        .execute(db)

      assertThat(count).isEqualTo(3)

      val loadedA = SkateTestEnumList::class.selectAll()
        .where(SkateTestEnumList::id eq a.id)
        .generate(db.dialect)
        .queryFirst<SkateTestEnumList>(db)

      assertThat(loadedA).isNotNull()
      assertThat(loadedA!!.id).isEqualTo(a.id)
      assertThat(loadedA.items).containsExactlyElementsOf(a.items)

      val loadedB = SkateTestEnumList::class.selectAll()
        .where(SkateTestEnumList::id eq b.id)
        .generate(db.dialect)
        .queryFirst<SkateTestEnumList>(db)

      assertThat(loadedB).isNotNull()
      assertThat(loadedB!!.id).isEqualTo(b.id)
      assertThat(loadedB.items).isEqualTo(b.items)

      val loadedC = SkateTestEnumList::class.selectAll()
        .where(SkateTestEnumList::id eq c.id)
        .generate(db.dialect)
        .queryFirst<SkateTestEnumList>(db)

      assertThat(loadedC).isNotNull()
      assertThat(loadedC!!.id).isEqualTo(c.id)
      assertThat(loadedC.items).isEqualTo(c.items)

      // Test querying by containment
      val loadedList = SkateTestEnumList::class.selectAll()
        .where(SkateTestEnumList::items.contains(listOf(BAR)))
        .generate(db.dialect)
        .query<SkateTestEnumList>(db)

      assertThat(loadedList).isNotNull
      assertThat(loadedList.count()).isEqualTo(1)
      assertThat(loadedList[0].id).isEqualTo(b.id)
      assertThat(loadedList[0].items).isEqualTo(b.items)

      // Test updating to a populated list
      val affected = SkateTestEnumList::class
        .update(
          SkateTestEnumList::items to listOf(FOO, BAR, BAG).array()
        )
        .where(SkateTestEnumList::id eq loadedA.id)
        .generate(db.dialect)
        .execute(db)

      assertThat(affected).isEqualTo(1)

      // Test updating to an empty list
      val affected2 = SkateTestEnumList::class
        .update(
          SkateTestEnumList::items to listOf<SkateTestEnumStel>().array()
        )
        .where(SkateTestEnumList::id eq loadedB.id)
        .generate(db.dialect)
        .execute(db)

      assertThat(affected2).isEqualTo(1)

      val loadedAll = SkateTestEnumList::class.selectAll()
        .generate(db.dialect)
        .query<SkateTestEnumList>(db)

      assertThat(loadedAll).isNotNull
      assertThat(loadedAll.count()).isEqualTo(3)
      assertThat(loadedAll.single { it.id == loadedA.id }.items.count()).isEqualTo(3)
      assertThat(loadedAll.single { it.id == loadedB.id }.items.count()).isEqualTo(0)
    } finally {
      db.executeRaw(
        """
          DROP TABLE IF EXISTS skate_test_enum_list;
          DROP TYPE IF EXISTS skate_test_enum_stel;
        """.trimIndent()
      )
    }
  }

  @Test
  fun `Insert and query and update with nullable enum array column`() {
    db.executeRaw(
      """
        DROP TABLE IF EXISTS skate_test_nullable_enum_list;
        DROP TYPE IF EXISTS skate_test_enum_stnel;

        CREATE TYPE skate_test_enum_stnel AS ENUM ('foo', 'bar', 'bag');
        CREATE TABLE skate_test_nullable_enum_list (
          id uuid NOT NULL DEFAULT uuid_generate_v4() PRIMARY KEY,
          items skate_test_enum_stnel[] NULL
        );
      """.trimIndent()
    )

    try {
      val a = SkateTestNullableEnumList(items = listOf())
      val b = SkateTestNullableEnumList(items = listOf(SkateTestEnumStnel.BAR, SkateTestEnumStnel.BAG))
      val c = SkateTestNullableEnumList(items = null)

      val count: Int = SkateTestNullableEnumList::class.insert()
        .values(a, b, c)
        .generate(db.dialect)
        .execute(db)

      assertThat(count).isEqualTo(3)

      val loadedA = SkateTestNullableEnumList::class.selectAll()
        .where(SkateTestNullableEnumList::id eq a.id)
        .generate(db.dialect)
        .queryFirst<SkateTestNullableEnumList>(db)

      assertThat(loadedA).isNotNull()
      assertThat(loadedA!!.id).isEqualTo(a.id)
      assertThat(loadedA.items).containsExactlyElementsOf(a.items)

      val loadedB = SkateTestNullableEnumList::class.selectAll()
        .where(SkateTestNullableEnumList::id eq b.id)
        .generate(db.dialect)
        .queryFirst<SkateTestNullableEnumList>(db)

      assertThat(loadedB).isNotNull()
      assertThat(loadedB!!.id).isEqualTo(b.id)
      assertThat(loadedB.items).isEqualTo(b.items)

      val loadedC = SkateTestNullableEnumList::class.selectAll()
        .where(SkateTestNullableEnumList::id eq c.id)
        .generate(db.dialect)
        .queryFirst<SkateTestNullableEnumList>(db)

      assertThat(loadedC).isNotNull()
      assertThat(loadedC!!.id).isEqualTo(c.id)
      assertThat(loadedC.items).isEqualTo(c.items)

      // Test querying by containment
      val loadedList = SkateTestNullableEnumList::class.selectAll()
        .where(SkateTestNullableEnumList::items.contains(listOf(SkateTestEnumStnel.BAR)))
        .generate(db.dialect)
        .query<SkateTestNullableEnumList>(db)

      assertThat(loadedList).isNotNull
      assertThat(loadedList.count()).isEqualTo(1)
      assertThat(loadedList[0].id).isEqualTo(b.id)
      assertThat(loadedList[0].items).isEqualTo(b.items)

      // Test updating to a value
      val affected = SkateTestNullableEnumList::class
        .update(
          SkateTestNullableEnumList::items to listOf(
            SkateTestEnumStnel.FOO,
            SkateTestEnumStnel.BAR,
            SkateTestEnumStnel.BAG
          ).array()
        )
        .where(SkateTestNullableEnumList::id eq loadedA.id)
        .generate(db.dialect)
        .execute(db)

      assertThat(affected).isEqualTo(1)

      // Test updating to null
      val affected2 = SkateTestNullableEnumList::class
        .update(
          SkateTestNullableEnumList::items.nullable() to null
        )
        .where(SkateTestNullableEnumList::id eq loadedB.id)
        .generate(db.dialect)
        .execute(db)

      assertThat(affected2).isEqualTo(1)

      val loadedAll = SkateTestNullableEnumList::class.selectAll()
        .generate(db.dialect)
        .query<SkateTestNullableEnumList>(db)

      assertThat(loadedAll).isNotNull
      assertThat(loadedAll.count()).isEqualTo(3)
      assertThat(loadedAll.single { it.id == loadedA.id }.items).isNotNull
      assertThat(loadedAll.single { it.id == loadedA.id }.items!!.count()).isEqualTo(3)
      assertThat(loadedAll.single { it.id == loadedB.id }.items).isNull()
    } finally {
      db.executeRaw(
        """
          DROP TABLE IF EXISTS skate_test_nullable_enum_list;
          DROP TYPE IF EXISTS skate_test_enum_stnel;
        """.trimIndent()
      )
    }
  }
}
