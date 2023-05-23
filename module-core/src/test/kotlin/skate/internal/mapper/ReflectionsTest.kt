@file:Suppress("unused")

package skate.internal.mapper

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import skate.ColumnName
import java.util.UUID

class ReflectionsTest {

  data class BirdEntity(
    val id: UUID = UUID.randomUUID(),

    @field:ColumnName("my_code")
    val code: String = "code",

    @ColumnName("serial_number")
    val serialNumber: String = "123",

    @field:ColumnName("my_speed")
    var speed: String = "123"
  )

  class Hello {
    private var x: Int? = null
    private var y: String? = null

    constructor(x: Int) {
      this.x = x
    }

    constructor(y: String?) {
      this.y = y
    }
  }

  @Test
  fun `propertyName() happy path`() {
    assertThat(BirdEntity::id.propertyName()).isEqualTo("id")
    assertThat(BirdEntity::code.propertyName()).isEqualTo("my_code")
    assertThat(BirdEntity::serialNumber.propertyName()).isEqualTo("serialNumber")
    assertThat(BirdEntity::speed.propertyName()).isEqualTo("my_speed")
  }

  @Test
  fun `findConstructor() data class`() {
    assertThat(BirdEntity::class.findConstructor()).isNotEqualTo(null)
  }

  @Test
  fun `findConstructor() has no primary constructor`() {
    assertThatThrownBy { Hello::class.findConstructor() }.isInstanceOf(IllegalArgumentException::class.java)
  }
}
