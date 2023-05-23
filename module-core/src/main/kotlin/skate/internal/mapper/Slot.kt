package skate.internal.mapper

import kotlin.reflect.KMutableProperty1
import kotlin.reflect.KParameter

/**
 * Data holder for property and parameters
 */
internal sealed class Slot {
  data class MutableProperty<T>(val property: KMutableProperty1<T, *>) : Slot()
  data class Parameter(val parameter: KParameter) : Slot()
}
