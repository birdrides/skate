package skate.internal.mapper

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import skate.Column
import skate.IntoField
import skate.Join
import skate.Query
import skate.column
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors

class SkateMappersTest {

  data class Bird(
    val id: UUID = UUID.randomUUID()
  )

  @Test
  fun `resolve() null query return cacheable mapper`() {
    val mapper = SkateMappers.resolve(Bird::class, query = null)
    assertThat(mapper).isInstanceOf(DefaultSkateMapper::class.java)
    assertThat(SkateMappers.resolve(Bird::class, query = null)).isEqualTo(mapper)
  }

  @Test
  fun `resolve() cache mapper multi-thread`() {
    val n = 10
    val executor = Executors.newFixedThreadPool(n)
    val latch = CountDownLatch(n)
    repeat(n) {
      executor.submit {
        assertThat(SkateMappers.resolve(Bird::class, query = null)).isNotEqualTo(null)
        latch.countDown()
      }
    }
    latch.await()
  }

  @Test
  fun `resolve() join query no cache`() {
    val column: Column<Bird, UUID?> = Bird::id.column()
    val query = Query(
      projections = emptyList(),
      aggregates = emptyList(),
      fromClauses = listOf(
        Join(
          query = Query(
            projections = emptyList(),
            aggregates = emptyList(),
            fromClauses = emptyList()
          ),
          intoField = IntoField(UUID::class, column)
        )
      )
    )
    val mapper = SkateMappers.resolve(Bird::class, query)
    assertThat(mapper).isInstanceOf(JoinSkateMapper::class.java)
  }
}
