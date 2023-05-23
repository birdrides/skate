@file:Suppress("UNCHECKED_CAST")

package skate.internal.mapper

import skate.ColumnName
import skate.Factory
import kotlin.reflect.KClass
import kotlin.reflect.KFunction
import kotlin.reflect.KMutableProperty1
import kotlin.reflect.KParameter
import kotlin.reflect.KProperty1
import kotlin.reflect.full.companionObject
import kotlin.reflect.full.companionObjectInstance
import kotlin.reflect.full.findAnnotation
import kotlin.reflect.full.primaryConstructor
import kotlin.reflect.jvm.javaField

internal fun <C : Any> KClass<C>.findConstructor(): KFunction<C> {
  return when {
    isSealed && companionObject?.java?.interfaces?.contains(Factory::class.java) == true -> {
      val c = companionObjectInstance as Factory<C>
      c.factoryFunction
    }

    else -> {
      primaryConstructor
        ?: (if (constructors.size == 1) constructors.first() else null)
        ?: throw IllegalArgumentException("A bean, $simpleName was mapped which was not instantiable (cannot find appropriate constructor)")
    }
  }
}

internal fun KParameter.parameterName(): String? {
  return findAnnotation<ColumnName>()?.value ?: name
}

internal fun <C> KMutableProperty1<C, *>.propertyName(): String {
  return javaField?.getAnnotation(ColumnName::class.java)?.value ?: name
}

internal fun <C> KProperty1<C, *>.propertyName(): String {
  return javaField?.getAnnotation(ColumnName::class.java)?.value ?: name
}
