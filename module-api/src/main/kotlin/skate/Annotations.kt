package skate

@Target(AnnotationTarget.PROPERTY)
annotation class Transient

@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
annotation class TableName(val name: String)

@Target(AnnotationTarget.PROPERTY_GETTER, AnnotationTarget.VALUE_PARAMETER, AnnotationTarget.FIELD)
@Retention(AnnotationRetention.RUNTIME)
annotation class ColumnName(val value: String)
