package skate.generator

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import skate.column
import skate.literal
import skate.lower
import skate.matches
import skate.notMatches
import test.Vehicle

class MatchesTest {

  private val psql = Postgresql()

  @Test
  fun `matches - column field matches pattern`() {
    assertThat(
      psql.generate(
        Vehicle::code matches "B%D"
      )
    ).isEqualTo(
      Fragment(
        "(\"vehicles\".\"code\" ~ ?)",
        listOf("B%D")
      )
    )
  }

  @Test
  fun `matches - column field to KProperty1 matches pattern`() {
    assertThat(
      psql.generate(
        Vehicle::code.lower() matches "B%D"
      )
    ).isEqualTo(
      Fragment(
        "(lower(\"vehicles\".\"code\") ~ ?)",
        listOf("B%D")
      )
    )
  }

  @Test
  fun `matches - text value matches column field pattern`() {
    assertThat(
      psql.generate(
        literal("ABBA") matches Vehicle::pattern.column()
      )
    ).isEqualTo(
      Fragment(
        "(? ~ \"vehicles\".\"pattern\")",
        listOf("ABBA")
      )
    )
  }

  @Test
  fun `matches - column field not matches pattern`() {
    assertThat(
      psql.generate(
        Vehicle::code notMatches "B%D"
      )
    ).isEqualTo(
      Fragment(
        "(\"vehicles\".\"code\" !~ ?)",
        listOf("B%D")
      )
    )
  }

  @Test
  fun `matches - column field to KProperty1 not matches pattern`() {
    assertThat(
      psql.generate(
        Vehicle::code.lower() notMatches "B%D"
      )
    ).isEqualTo(
      Fragment(
        "(lower(\"vehicles\".\"code\") !~ ?)",
        listOf("B%D")
      )
    )
  }

  @Test
  fun `matches - text value not matcehs column field pattern`() {
    assertThat(
      psql.generate(
        literal("ABBA") notMatches Vehicle::pattern.column()
      )
    ).isEqualTo(
      Fragment(
        "(? !~ \"vehicles\".\"pattern\")",
        listOf("ABBA")
      )
    )
  }
}
