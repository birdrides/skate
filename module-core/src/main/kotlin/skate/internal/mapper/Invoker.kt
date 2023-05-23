package skate.internal.mapper

import org.jdbi.v3.core.statement.StatementContext
import java.lang.reflect.InvocationTargetException
import java.sql.ResultSet
import kotlin.reflect.KClass
import kotlin.reflect.KFunction
import kotlin.reflect.KMutableProperty1
import kotlin.reflect.KParameter
import kotlin.reflect.jvm.isAccessible

internal class Invoker<T : Any>(
  private val type: KClass<T>,
  private val constructor: KFunction<T>,
  private val parameters: Map<KParameter, ValueProvider>,
  private val properties: List<Pair<KMutableProperty1<T, *>, ValueProvider>>
) {
  /**
   * Invoke and return the actual value for a entity class type from a given [ResultSet] query
   * We can't really cache this reflection call this point but in the future we might be able to generate
   * these calls directly from annotation processor.
   */
  operator fun invoke(resultSet: ResultSet, context: StatementContext): T {
    val parametersWithValue = parameters.mapValues { (_, valueProvider) -> valueProvider(resultSet, context) }
    val propertiesWithValue =
      properties.map { (property, valueProvider) -> Pair(property, valueProvider(resultSet, context)) }
    try {
      constructor.isAccessible = true
      val instance = constructor.callBy(parametersWithValue)

      propertiesWithValue.forEach { (prop, value) ->
        prop.isAccessible = true
        prop.setter.call(instance, value)
      }

      return instance
    } catch (e: InvocationTargetException) {
      throw IllegalArgumentException("Error constructing ${type.simpleName} with SkateMapper", e.targetException)
    } catch (e: ReflectiveOperationException) {
      throw IllegalArgumentException("Reflection error constructing ${type.simpleName} with SkateMapper", e)
    }
  }
}
