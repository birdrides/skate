package skate.generator

import skate.ArrayComparisonOperator
import skate.Column
import skate.ComparisonOperator
import skate.DateOperator
import skate.IntoField
import skate.JoinKind
import skate.JsonBOperator
import skate.NumericOperator
import skate.Table

internal fun NumericOperator.sql(): String {
  return SqlGenerator.NUMERIC_OPERATOR(this)
}

internal fun JsonBOperator.sql(): String {
  return SqlGenerator.JSONB(this)
}

internal fun DateOperator.sql(): String {
  return SqlGenerator.DATE_OPERATOR(this)
}

internal fun ComparisonOperator.sql(): String {
  return SqlGenerator.COMPARISON_OPERATOR(this)
}

internal fun ArrayComparisonOperator.sql(): String {
  return SqlGenerator.ARRAY_COMPARISON_OPERATOR(this)
}

internal fun JoinKind.sql(): String {
  return SqlGenerator.JOIN_KIND(this)
}

internal fun Column<*, *>.sql(): String {
  return SqlGenerator.COLUMN(this)
}

internal fun Table<*>.sql(): String {
  return SqlGenerator.TABLE(this)
}

internal fun IntoField<*, *>.joinStart(): String {
  return SqlGenerator.JOIN_START(this)
}

internal fun IntoField<*, *>.joinEnd(): String {
  return SqlGenerator.JOIN_END(this)
}
