package skate

import kotlin.reflect.KFunction

interface Factory<out T : Any> {
  val factoryFunction: KFunction<T>
}
