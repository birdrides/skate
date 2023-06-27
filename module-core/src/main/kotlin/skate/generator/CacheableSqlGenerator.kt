package skate.generator

import java.util.concurrent.ConcurrentHashMap

/**
 * A simple cache strategy for generated SQL to avoid expensive reflection at runtime.
 * Perhaps one day we can just use annotation processor to just ignore reflection.
 */
internal interface CacheableSqlGenerator<E> : SqlGenerator<E> {
  /**
   * Given an instance E to generate, return a cache key as string
   */
  fun E.key(): String

  companion object {
    /**
     * Default cache implementation backed by [ConcurrentHashMap]. Note that we don't want
     * to use Caffeine cache because [ConcurrentHashMap] is much more lightweight and we don't
     * need any fancy features from a real cache.
     *
     * @param generator The actual implementation of [SqlGenerator]
     * @param key The key supplier functor
     */
    internal inline fun <E : Any> build(
      generator: SqlGenerator<E>,
      crossinline key: (E) -> String
    ): CacheableSqlGenerator<E> {
      return object : CacheableSqlGenerator<E> {

        private val cache = ConcurrentHashMap<String, String>()

        override fun invoke(e: E): String {
          return cache.computeIfAbsent(key(e)) {
            generator(e)
          }
        }

        override fun E.key(): String {
          return key(this)
        }
      }
    }
  }
}
