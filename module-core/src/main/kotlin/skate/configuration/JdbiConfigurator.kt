package skate.configuration

import org.jdbi.v3.core.Jdbi

interface JdbiConfigurator {
  /**
   * Configure [Jdbi] instance
   */
  fun configure(jdbi: Jdbi): Jdbi

  /**
   * The default query timeout in seconds
   */
  val queryTimeoutSeconds: Int

  /**
   * Default [JdbiConfigurator] do nothing but return
   */
  object NOOP : JdbiConfigurator {

    override fun configure(jdbi: Jdbi): Jdbi {
      return jdbi
    }

    override val queryTimeoutSeconds: Int = 10
  }
}
