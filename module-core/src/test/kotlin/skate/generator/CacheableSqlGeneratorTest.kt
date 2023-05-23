package skate.generator

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import skate.Column
import skate.ColumnName
import skate.IntoField
import skate.Table
import skate.TableName
import skate.column

class CacheableSqlGeneratorTest {

  @TableName("first")
  data class First(
    @ColumnName("a_column")
    val a: Int,

    @ColumnName("b_column")
    val b: String
  )

  @TableName("second")
  data class Second(
    val a: Int,
    val b: String
  )

  @Test
  fun `TABLE correctness`() {
    val g = SqlGenerator.TABLE
    repeat(2) {
      assertThat(g(Table(First::class))).isEqualTo("first")
      assertThat(g(Table(Second::class))).isEqualTo("second")
    }
  }

  @Test
  fun `COLUMN correctness`() {
    val g = SqlGenerator.COLUMN
    repeat(2) {
      assertThat(g(Column(First::a, Table(First::class)))).isEqualTo("\"a_column\"")
      assertThat(g(Column(First::a, Table(First::class)))).isEqualTo("\"a_column\"")
      assertThat(g(Column(First::b, Table(First::class)))).isEqualTo("\"b_column\"")
      assertThat(g(Column(Second::a, Table(Second::class)))).isEqualTo("\"a\"")
      assertThat(g(Column(Second::b, Table(Second::class)))).isEqualTo("\"b\"")
    }
  }

  @Test
  fun `JOIN_START correctness`() {
    val g = SqlGenerator.JOIN_START
    val c: Column<First, Int?> = First::a.column()
    repeat(2) {
      assertThat(g(IntoField(Int::class, c))).isEqualTo("start:a_column")
    }
  }

  @Test
  fun `JOIN_END correctness`() {
    val g = SqlGenerator.JOIN_END
    val c: Column<First, Int?> = First::a.column()
    repeat(2) {
      assertThat(g(IntoField(Int::class, c))).isEqualTo("end:a_column")
    }
  }
}
