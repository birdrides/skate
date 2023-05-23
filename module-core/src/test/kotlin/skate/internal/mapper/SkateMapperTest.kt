package skate.internal.mapper

import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.mapper.reflect.ColumnNameMatcher
import org.jdbi.v3.core.statement.StatementContext
import org.junit.jupiter.api.Test
import java.sql.ResultSet
import java.util.UUID

class SkateMapperTest {

  data class Entity(
    val id: UUID = UUID.randomUUID()
  )

  class TestSkateMapper<T : Any>(
    private val entity: T
  ) : SkateMapper<T> {
    override fun map(rs: ResultSet?, ctx: StatementContext?): T {
      return entity
    }
  }

  @Test
  fun `matches() with any column is a match`() {
    val a = mockk<ColumnNameMatcher>(relaxed = true)
    val b = mockk<ColumnNameMatcher>(relaxed = true)
    val columnName = "c"
    val parameterName = "p"
    every {
      a.columnNameMatches(columnName, parameterName)
    } returns false
    every {
      b.columnNameMatches(columnName, parameterName)
    } returns true

    val mapper = TestSkateMapper(Entity())
    mapper.run {
      assertThat(listOf(a, b).matches(columnName, parameterName)).isEqualTo(true)
      assertThat(listOf(b, a).matches(columnName, parameterName)).isEqualTo(true)
      assertThat(listOf(a).matches(columnName, parameterName)).isEqualTo(false)
    }
  }
}
