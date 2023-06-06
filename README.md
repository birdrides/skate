<p align="center">
  <img src="/docs/skate.png" alt="Skate - Not an ORM" width="450">
</p>

<p align="center">
  <a href="https://github.com/birdrides/mockingbird/blob/master/LICENSE.md"><img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="MIT licensed"></a>
  <a href="https://join.slack.com/t/birdopensource/shared_invite/zt-1wcrxb22t-lPw1jDjlpRAYNuefRV2AGw" rel="nofollow"><img src="https://img.shields.io/badge/slack-%23skate-F55C40.svg" alt="#skate Slack channel"></a>
</p>

Skate is a SQL generation and query library for Kotlin. It makes it easy to get your data as Kotlin data classes without the pitfalls of an ORM.

## Entity

An entity is a data class that represents a row in a database table. Use the `@TableName` annotation to specify the table name.
```kotlin
@TableName("users")
data class User(
  override val id: UUID = UUID.randomUUID(),
  val name: String? = null,
  val email: String? = null,
  val createdAt: OffsetDateTime = OffsetDateTime.now(),
  val updatedAt: OffsetDateTime? = null,
) : Entity

```

## Generating SQL

Only the `Postgresql` generator is currently supported. It's easy to add more generators if you need them.
```kotlin
val psql = skate.generator.Postgresql()
```
Use `selectAll` to fetch all fields in a table.
```kotlin
User::class
  .selectAll()
  .where(User::email eq "john@doe.com")
  .generate(psql)
```
```yaml
sql:
  SELECT * FROM "users" WHERE "email" = ?
values:
   ["john@doe"]
```
Use `insert` to add a list of entities to a table.
```kotlin
User::class
  .insert() // can specify which fields to insert here
  .values(User(name = "John Doe", email = "john@doe.com"))
  .generate(psql)
```
```yaml
sql:
  INSERT INTO "users" ("created_at", "email", "id", "name", "updated_at")
    VALUES (#users0.createdAt, #users0.email, #users0.id, #users0.name, #users0.updatedAt)
values:
  [User(...)]
```
Unlike an ORM, `update` requires you to specify exactly which fields to update.
```kotlin
User::class
  .update(
    User::name to "Jane Doe",
    User::email to "jane@doe.com"
  )
  .where(User::email eq "john@doe.com")
  .generate(psql)
```
```yaml
sql:
  UPDATE "users" SET "name" = ?, "email" = ? WHERE ("email" = ?)
values:
  ["Jane Doe", "jane@doe", "john@doe"]
```
Use `delete` to delete rows from a table. But usually you'll want to use `update` to set a `deletedAt` field instead.
```kotlin
Users::class
  .delete()
  .where(User::id.eq(id))
  .generate(psql)
```
```yaml
sql:
  DELETE FROM "users" WHERE ("id" = ?)
values:
  UUID(...)
```

## Querying
Construct a database object for your favorite data source:
```kotlin
val database = Database.create(
  config = DatabaseConfig(
    host = "localhost",
    database = "local",
    user = "local",
    password = "local",
    port = 5432,
  ),
  poolConfig = ConnectionPoolConfig(
    maximumPoolSize = 2,
    minimumIdle = 1,
    maxLifetime = 300000,
    connectionTimeout = 30000,
    idleTimeout = 600000,
  ),
  jackson = jackson
)
```
Executing generated SQL in the database just requires calling either `query` or `execute` depending on whether you want to observe the results.
```kotlin
User::class
  .selectAll()
  .where(User::name.like("John %"))
  .generate(db.dialect)
  .query(db)
```
```kotlin
List<User>(...)
```
