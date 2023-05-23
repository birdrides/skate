package skate.generator

fun String.toUnderscore(): String {
  return split(Regex("(?<=[a-z])(?=[A-Z])")).joinToString("_") { it.toLowerCase() }
}
