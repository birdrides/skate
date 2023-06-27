package skate.generator

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.ArgumentsProvider
import org.junit.jupiter.params.provider.ArgumentsSource
import net.postgis.jdbc.geometry.Geometry
import net.postgis.jdbc.geometry.Point
import skate.All
import skate.Column
import skate.Delete
import skate.Expression
import skate.IntoField
import skate.Projection
import skate.Table
import skate.TypeSpec
import skate.Value
import skate.alias
import skate.all
import skate.and
import skate.array
import skate.asc
import skate.atTimeZone
import skate.avg
import skate.case
import skate.cast
import skate.charLength
import skate.clusterWithin
import skate.coalesce
import skate.column
import skate.columnAs
import skate.containedBy
import skate.contains
import skate.count
import skate.countAll
import skate.dateTrunc
import skate.desc
import skate.distanceFrom
import skate.distinct
import skate.distinctOn
import skate.div
import skate.doNothing
import skate.eq
import skate.exists
import skate.from
import skate.greatest
import skate.groupBy
import skate.gt
import skate.gte
import skate.insert
import skate.intersects
import skate.interval
import skate.into
import skate.inverse
import skate.isEmpty
import skate.isIn
import skate.isNotEmpty
import skate.isNotIn
import skate.isNotNull
import skate.isNull
import skate.join
import skate.least
import skate.leftJoin
import skate.like
import skate.limit
import skate.literal
import skate.lower
import skate.lt
import skate.lte
import skate.max
import skate.minus
import skate.ne
import skate.notLike
import skate.now
import skate.nullable
import skate.nullsFirst
import skate.nullsLast
import skate.offset
import skate.on
import skate.onConflict
import skate.or
import skate.orderBy
import skate.overlaps
import skate.plus
import skate.project
import skate.projectAll
import skate.projectAs
import skate.random
import skate.returning
import skate.returningAll
import skate.select
import skate.selectAll
import skate.selectCountDistinct
import skate.selectOne
import skate.sum
import skate.then
import skate.times
import skate.to
import skate.unnest
import skate.update
import skate.updateAll
import skate.upper
import skate.value
import skate.values
import skate.where
import skate.within
import skate.withinDistance
import test.Board
import test.Corral
import test.Drop
import test.Invite
import test.LatLonRect
import test.Ride
import test.Task
import test.User
import test.Vehicle
import test.VehicleTrack
import test.Zone
import java.time.OffsetDateTime
import java.util.UUID
import java.util.stream.Stream

class PostgresqlTest {

  private val uuid = UUID.randomUUID()
  private val psql = Postgresql()

  @Test
  fun delete() {
    val id = UUID.randomUUID()
    val delete = Delete(Table(Vehicle::class)).where(Vehicle::id.eq(id))
    assertThat(psql.generate(delete)).isEqualTo(
      DeleteStatement(
        sql = "DELETE FROM \"vehicles\" WHERE (\"vehicles\".\"id\" = ?)",
        values = listOf(id)
      )
    )
  }

  @Test
  fun numeric() {
    assertThat(
      psql.generate(
        (Vehicle::distance + 100 + 300).lte(100)
      )
    ).isEqualTo(
      Fragment(
        "(((\"vehicles\".\"distance\" + ?) + ?) <= ?)",
        listOf(100, 300, 100)
      )
    )

    assertThat(
      psql.generate(
        (Vehicle::distance + 100) * (Vehicle::distance - 100)
      )
    ).isEqualTo(
      Fragment(
        "((\"vehicles\".\"distance\" + ?) * (\"vehicles\".\"distance\" - ?))",
        listOf(100, 100)
      )
    )
  }

  @Test
  fun comparison() {
    assertThat(
      psql.generate(
        Vehicle::id.eq(uuid)
      )
    ).isEqualTo(
      Fragment(
        "(\"vehicles\".\"id\" = ?)",
        listOf(uuid)
      )
    )

    assertThat(
      psql.generate(
        Vehicle::batteryLevel.lte(15)
      )
    ).isEqualTo(
      Fragment(
        "(\"vehicles\".\"battery_level\" <= ?)",
        listOf(15)
      )
    )
  }

  @Test
  fun arrayComparison() {
    val phones = listOf("5058221801", "5052751206")
    assertThat(
      psql.generate(
        Invite::phones.contains(phones)
      )
    ).isEqualTo(
      Fragment(
        "(\"invites\".\"phones\" @> ?)",
        listOf(phones.array())
      )
    )

    assertThat(
      psql.generate(
        Invite::phones.containedBy(phones)
      )
    ).isEqualTo(
      Fragment(
        "(\"invites\".\"phones\" <@ ?)",
        listOf(phones.array())
      )
    )

    assertThat(
      psql.generate(
        Invite::phones.overlaps(phones)
      )
    ).isEqualTo(
      Fragment(
        "(\"invites\".\"phones\" && ?)",
        listOf(phones.array())
      )
    )
  }

  @Test
  fun nullableArrayComparison() {
    val items = listOf("scooter", "bike")
    assertThat(
      psql.generate(
        Vehicle::tags.contains(items)
      )
    ).isEqualTo(
      Fragment(
        "(\"vehicles\".\"tags\" @> ?)",
        listOf(items.array())
      )
    )

    assertThat(
      psql.generate(
        Vehicle::tags.containedBy(items)
      )
    ).isEqualTo(
      Fragment(
        "(\"vehicles\".\"tags\" <@ ?)",
        listOf(items.array())
      )
    )

    assertThat(
      psql.generate(
        Vehicle::tags.overlaps(items)
      )
    ).isEqualTo(
      Fragment(
        "(\"vehicles\".\"tags\" && ?)",
        listOf(items.array())
      )
    )
  }

  @Test
  fun logical() {
    assertThat(
      psql.generate(
        Vehicle::createdAt.gte(now()) and Vehicle::batteryLevel.ne(100)
      )
    ).isEqualTo(
      Fragment(
        "((\"vehicles\".\"created_at\" >= now()) AND (\"vehicles\".\"battery_level\" != ?))",
        listOf(100)
      )
    )

    assertThat(
      psql.generate(
        Vehicle::distance.isNull() or Vehicle::estimatedRange.isNotNull()
      )
    ).isEqualTo(
      Fragment(
        "((\"vehicles\".\"distance\" IS NULL) OR (\"vehicles\".\"estimated_range\" IS NOT NULL))",
        listOf()
      )
    )

    assertThat(
      psql.generate(
        and(
          Vehicle::code.lower().like("B%D"),
          Vehicle::code.upper().notLike("%IR%"),
          Vehicle::serialNumber.like("%/%")
        )
      )
    ).isEqualTo(
      Fragment(
        "((lower(\"vehicles\".\"code\") LIKE ?) AND (upper(\"vehicles\".\"code\") NOT LIKE ?) AND (\"vehicles\".\"serial_number\" LIKE ?))",
        listOf("B%D", "%IR%", "%/%")
      )
    )

    assertThat(
      psql.generate(
        Vehicle::distance.isIn(listOf(100, 200, 300)) or Vehicle::estimatedRange.isNotIn(listOf(1, 2))
      )
    ).isEqualTo(
      Fragment(
        "((\"vehicles\".\"distance\" IN (?, ?, ?)) OR (\"vehicles\".\"estimated_range\" NOT IN (?, ?)))",
        listOf(100, 200, 300, 1, 2)
      )
    )

    assertThat(
      psql.generate(
        Vehicle::distance.eq(100).or(Vehicle::distance.lt(20)).inverse()
      )
    ).isEqualTo(
      Fragment(
        "(NOT ((\"vehicles\".\"distance\" = ?) OR (\"vehicles\".\"distance\" < ?)))",
        listOf(100, 20)
      )
    )
  }

  @Test
  fun empty() {
    assertThat(
      psql.generate(
        Vehicle::tags.isEmpty() or Vehicle::tags.isNotEmpty()
      )
    ).isEqualTo(
      Fragment(
        "((\"vehicles\".\"tags\" = '{}') OR (\"vehicles\".\"tags\" != '{}'))"
      )
    )
  }

  @Test
  fun functions() {
    assertThat(
      psql.generate(
        coalesce(Vehicle::rideId, Vehicle::userId, Vehicle::id)
      )
    ).isEqualTo(
      Fragment(
        "coalesce(\"vehicles\".\"ride_id\", \"vehicles\".\"user_id\", \"vehicles\".\"id\")",
        listOf()
      )
    )

    assertThat(
      psql.generate(
        least(Vehicle::batteryLevel.column(), 0.value())
      )
    ).isEqualTo(
      Fragment("least(\"vehicles\".\"battery_level\", ?)", listOf(0))
    )

    assertThat(
      psql.generate(
        greatest(Vehicle::batteryLevel.plus(50), Vehicle::distance.div(100))
      )
    ).isEqualTo(
      Fragment("greatest((\"vehicles\".\"battery_level\" + ?), (\"vehicles\".\"distance\" / ?))", listOf(50, 100))
    )

    assertThat(
      psql.generate(
        Vehicle::code.lower()
      )
    ).isEqualTo(
      Fragment("lower(\"vehicles\".\"code\")", listOf())
    )

    assertThat(
      psql.generate(
        Vehicle::code.lower()
      )
    ).isEqualTo(
      Fragment("lower(\"vehicles\".\"code\")", listOf())
    )

    assertThat(
      psql.generate(
        Vehicle::createdAt.dateTrunc(interval("5 minutes"))
      )
    ).isEqualTo(
      Fragment("date_trunc(interval '5 minutes', \"vehicles\".\"created_at\")", listOf())
    )

    assertThat(
      psql.generate(
        Vehicle::createdAt.dateTrunc("month")
      )
    ).isEqualTo(
      Fragment("date_trunc(?, \"vehicles\".\"created_at\")", listOf("month"))
    )

    assertThat(
      psql.generate(
        now().dateTrunc(interval("5 minutes"))
      )
    ).isEqualTo(
      Fragment("date_trunc(interval '5 minutes', now())", listOf())
    )

    assertThat(
      psql.generate(
        now().dateTrunc("month")
      )
    ).isEqualTo(
      Fragment("date_trunc(?, now())", listOf("month"))
    )
  }

  @Test
  fun constructors() {
    assertThat(
      psql.generate(
        interval("1 day")
      )
    ).isEqualTo(
      Fragment(
        "interval '1 day'",
        listOf()
      )
    )

    // Quote escaping.
    assertThat(
      psql.generate(
        interval("1 ' day")
      )
    ).isEqualTo(
      Fragment(
        "interval '1 '' day'",
        listOf()
      )
    )
  }

  @Test
  fun postgis() {
    val location = Point(-100.0, 34.0)

    assertThat(
      psql.generate(
        Zone::class.selectAll()
          .where(Zone::region.intersects(location))
      ).let { listOf(it.sql, it.values) }
    ).isEqualTo(
      listOf(
        "SELECT \"zones\".* FROM \"zones\" WHERE st_intersects(\"zones\".\"region\", ?)",
        listOf(location)
      )
    )

    assertThat(
      psql.generate(
        Zone::class.selectAll()
          .where(Zone::region.overlaps(location))
      ).let { listOf(it.sql, it.values) }
    ).isEqualTo(
      listOf(
        "SELECT \"zones\".* FROM \"zones\" WHERE st_overlaps(\"zones\".\"region\", ?)",
        listOf(location)
      )
    )

    assertThat(
      psql.generate(
        Vehicle::class.select(Vehicle::location.clusterWithin(3.0, "gc").unnest())
          .where(
            and(
              Vehicle::location.withinDistance(location, 100),
              Vehicle::locked.ne(true)
            )
          )
      ).let { listOf(it.sql, it.values) }
    ).isEqualTo(
      listOf(
        "SELECT unnest(st_clusterwithin(\"vehicles\".\"location\", ?)) AS gc FROM \"vehicles\" WHERE (st_dwithin(\"vehicles\".\"location\", ?, ?) AND (\"vehicles\".\"locked\" != ?))",
        listOf(3.0, location, 100, true)
      )
    )

    assertThat(
      psql.generate(
        Zone::class.selectAll()
          .where(Zone::region.withinDistance(location, 1000))
      ).let { listOf(it.sql, it.values) }
    ).isEqualTo(
      listOf(
        "SELECT \"zones\".* FROM \"zones\" WHERE st_dwithin(\"zones\".\"region\", ?, ?)",
        listOf(location, 1000)
      )
    )
  }

  @Test
  fun select() {
    assertThat(
      psql.generate(
        Vehicle::class.selectAll()
          .where(Vehicle::distance.gt(100))
          .orderBy(Vehicle::distance.desc())
          .limit(10)
          .offset(20)
      ).let { listOf(it.sql, it.values) }
    ).isEqualTo(
      listOf(
        "SELECT \"vehicles\".* FROM \"vehicles\" WHERE (\"vehicles\".\"distance\" > ?) ORDER BY \"vehicles\".\"distance\" DESC LIMIT 10 OFFSET 20",
        listOf(100)
      )
    )

    assertThat(
      psql.generate(
        Vehicle::class.select(Vehicle::class.projectAll(), Vehicle::distance.div(20).projectAs("d"))
          .where(Vehicle::distance.lt(100))
      ).let { listOf(it.sql, it.values) }
    ).isEqualTo(
      listOf(
        "SELECT \"vehicles\".*, (\"vehicles\".\"distance\" / ?) AS d FROM \"vehicles\" WHERE (\"vehicles\".\"distance\" < ?)",
        listOf(20, 100)
      )
    )

    assertThat(
      psql.generate(
        Vehicle::class.select(
          Vehicle::id,
          Vehicle::distance
        ).where(Vehicle::distance.gt(100))
      ).let { listOf(it.sql, it.values) }
    ).isEqualTo(
      listOf(
        "SELECT \"vehicles\".\"id\", \"vehicles\".\"distance\" FROM \"vehicles\" WHERE (\"vehicles\".\"distance\" > ?)",
        listOf(100)
      )
    )

    assertThat(
      psql.generate(
        Vehicle::class.select(
          Vehicle::id.projectAs("foo"),
          Vehicle::distance.project(),
          Vehicle::distance.gt(100).projectAs("far")
        ).where(Vehicle::distance.gt(100))
      ).let { listOf(it.sql, it.values) }
    ).isEqualTo(
      listOf(
        "SELECT \"vehicles\".\"id\" AS foo, \"vehicles\".\"distance\", (\"vehicles\".\"distance\" > ?) AS far FROM \"vehicles\" WHERE (\"vehicles\".\"distance\" > ?)",
        listOf(100, 100)
      )
    )

    assertThat(
      psql.generate(
        Vehicle::class.selectAll()
          .orderBy(Vehicle::trackedAt.desc().nullsFirst())
      ).sql
    ).isEqualTo(
      "SELECT \"vehicles\".* FROM \"vehicles\" ORDER BY \"vehicles\".\"tracked_at\" DESC NULLS FIRST"
    )

    assertThat(
      psql.generate(
        Vehicle::class.selectAll()
          .orderBy(Vehicle::trackedAt.desc().nullsLast())
      ).sql
    ).isEqualTo(
      "SELECT \"vehicles\".* FROM \"vehicles\" ORDER BY \"vehicles\".\"tracked_at\" DESC NULLS LAST"
    )

    assertThat(
      psql.generate(
        Vehicle::class.selectAll()
          .orderBy(Vehicle::trackedAt.asc())
      ).sql
    ).isEqualTo(
      "SELECT \"vehicles\".* FROM \"vehicles\" ORDER BY \"vehicles\".\"tracked_at\" ASC"
    )
  }

  @Test
  fun random_order() {
    assertThat(
      psql.generate(
        Vehicle::class.selectAll()
          .where(Vehicle::distance.gt(100))
          .random()
          .limit(5)
      ).let { listOf(it.sql, it.values) }
    ).isEqualTo(
      listOf(
        "SELECT \"vehicles\".* FROM \"vehicles\" WHERE (\"vehicles\".\"distance\" > ?) ORDER BY RANDOM() LIMIT 5",
        listOf(100)
      )
    )
  }

  @Test
  fun selectAggregates() {
    assertThat(
      psql.generate(
        Vehicle::class.select(
          countAll(alias = "count"),
          Vehicle::id.count(alias = "id_count"),
          Vehicle::distance.avg()
        ).where(Vehicle::distance.gt(100))
      ).let { listOf(it.sql, it.values) }
    ).isEqualTo(
      listOf(
        "SELECT count(*) AS count, count(\"vehicles\".\"id\") AS id_count, avg(\"vehicles\".\"distance\") FROM \"vehicles\" WHERE (\"vehicles\".\"distance\" > ?)",
        listOf(100)
      )
    )

    assertThat(
      psql.generate(
        Vehicle::class.select(
          countAll(alias = "count"),
          Vehicle::distance.avg()
        ).where(Vehicle::distance.gt(100)).groupBy(Vehicle::batteryLevel)
      ).let { listOf(it.sql, it.values) }
    ).isEqualTo(
      listOf(
        "SELECT count(*) AS count, avg(\"vehicles\".\"distance\"), \"vehicles\".\"battery_level\" FROM \"vehicles\" WHERE (\"vehicles\".\"distance\" > ?) GROUP BY \"vehicles\".\"battery_level\"",
        listOf(100)
      )
    )

    assertThat(
      psql.generate(
        Ride::class.select()
          .where(Ride::currentDistance gt Ride::initialDistance)
          .groupBy(Ride::vehicleId)
      ).sql
    ).isEqualTo(
      "SELECT \"rides\".\"vehicle_id\" FROM \"rides\" WHERE (\"rides\".\"current_distance\" > \"rides\".\"initial_distance\") GROUP BY \"rides\".\"vehicle_id\""
    )

    assertThat(
      psql.generate(
        Ride::class.select(
          countAll(alias = "count")
        ).where(Ride::currentDistance gt Ride::initialDistance)
          .groupBy(Ride::vehicleId.projectAs("foo"))
      ).sql
    ).isEqualTo(
      "SELECT count(*) AS count, \"rides\".\"vehicle_id\" AS foo FROM \"rides\" WHERE (\"rides\".\"current_distance\" > \"rides\".\"initial_distance\") GROUP BY foo"
    )
  }

  @Test
  fun joins() {
    assertThat(
      psql.generate(
        Vehicle::class
          .select(Vehicle::id, Vehicle::distance)
          .where(Vehicle::distance.gt(100))
          .join(
            Board::class.select(Board::firmwareVersion).on(
              (Board::vehicleId eq Vehicle::id) and (Board::protocol eq "4")
            )
          )
      ).let { listOf(it.sql, it.values) }
    ).isEqualTo(
      listOf(
        "SELECT \"vehicles\".\"id\", \"vehicles\".\"distance\", \"boards\".\"firmware_version\" FROM \"vehicles\" JOIN \"boards\" ON ((\"boards\".\"vehicle_id\" = \"vehicles\".\"id\") AND (\"boards\".\"protocol\" = ?)) WHERE (\"vehicles\".\"distance\" > ?)",
        listOf("4", 100)
      )
    )

    assertThat(
      psql.generate(
        Vehicle::class
          .select(Vehicle::id)
          .where(Vehicle::distance.gt(100))
          .leftJoin(
            Board::class.select(Board::firmwareVersion).on(
              Board::vehicleId eq Vehicle::id
            )
          )
      ).let { listOf(it.sql, it.values) }
    ).isEqualTo(
      listOf(
        "SELECT \"vehicles\".\"id\", \"boards\".\"firmware_version\" FROM \"vehicles\" LEFT JOIN \"boards\" ON (\"boards\".\"vehicle_id\" = \"vehicles\".\"id\") WHERE (\"vehicles\".\"distance\" > ?)",
        listOf(100)
      )
    )

    assertThat(
      psql.generate(
        Vehicle::class.selectAll()
          .where(Vehicle::locked eq false)
          .join(
            Task::class.selectAll()
              .into(Vehicle::task)
              .on(Vehicle::taskId eq Task::id)
          )
      ).let { listOf(it.sql, it.values) }
    ).isEqualTo(
      listOf(
        "SELECT \"vehicles\".*, NULL AS \"start:task\", \"tasks\".*, NULL AS \"end:task\" FROM \"vehicles\" JOIN \"tasks\" ON (\"vehicles\".\"task_id\" = \"tasks\".\"id\") WHERE (\"vehicles\".\"locked\" = ?)",
        listOf(false)
      )
    )
  }

  @Test
  fun `joins - can join against subquery`() {
    val result = psql.generate(
      Vehicle::class
        .select(Vehicle::class.all())
        .join(
          Vehicle::class
            .select(Vehicle::distance.max("distance"))
            .where(Vehicle::distance gte 0)
            .groupBy(Vehicle::model)
            .on(
              on = and(
                Vehicle::distance eq Vehicle::distance.columnAs("agg"),
                coalesce(Vehicle::model.column(), Value("")) eq Vehicle::model.columnAs("agg")
              ),
              alias = "agg"
            )
        )
    )

    assertThat(result.sql).isEqualToIgnoringWhitespace(
      """
        SELECT "vehicles".*, "vehicles"."model"
        FROM "vehicles"
        JOIN (
          SELECT max("vehicles"."distance") AS distance,
            "vehicles"."model"
          FROM "vehicles"
          WHERE ("vehicles"."distance" >= ?)
          GROUP BY "vehicles"."model"
        ) "agg" ON (
          ("vehicles"."distance" = "agg"."distance")
            AND (coalesce("vehicles"."model", ?) = "agg"."model")
        )
      """.trimIndent()
    )
    assertThat(result.values).isEqualTo(listOf(0, ""))
  }

  @Test
  fun insert() {
    val vehicle1 = Vehicle()
    val vehicle2 = Vehicle()
    val vehicle3 = Vehicle()

    assertThat(
      psql.generate(
        Vehicle::class.insert(Vehicle::id, Vehicle::distance)
          .values(vehicle1)
      )
    ).isEqualTo(
      InsertStatement(
        "INSERT INTO \"vehicles\" (\"id\", \"distance\") VALUES (#vehicles0.id, #vehicles0.distance)",
        "vehicles",
        listOf(vehicle1)
      )
    )

    assertThat(
      psql.generate(
        Vehicle::class.insert(Vehicle::id, Vehicle::distance)
          .values(vehicle1, vehicle2, vehicle3)
      )
    ).isEqualTo(
      InsertStatement(
        "INSERT INTO \"vehicles\" (\"id\", \"distance\") VALUES (#vehicles0.id, #vehicles0.distance), (#vehicles1.id, #vehicles1.distance), (#vehicles2.id, #vehicles2.distance)",
        "vehicles",
        listOf(vehicle1, vehicle2, vehicle3)
      )
    )

    val metersPerMile = 1609.34
    assertThat(
      psql.generate(
        Vehicle::class.insert(Vehicle::id, Vehicle::distance)
          .values(vehicle1)
          .returning(Vehicle::distance.div(metersPerMile).projectAs("miles"))
      ).let { listOf(it.sql, it.values) }
    ).isEqualTo(
      listOf(
        "INSERT INTO \"vehicles\" (\"id\", \"distance\") VALUES (#vehicles0.id, #vehicles0.distance) RETURNING (\"vehicles\".\"distance\" / ?) AS miles",
        listOf(metersPerMile)
      )
    )

    val user = User(name = "John Doe")
    assertThat(
      psql.generate(
        User::class.insert()
          .values(user)
      )
    ).isEqualTo(
      InsertStatement(
        "INSERT INTO \"users\" (\"created_at\", \"email\", \"id\", \"name\", \"updated_at\") VALUES (#users0.createdAt, #users0.email, #users0.id, #users0.name, #users0.updatedAt)",
        "users",
        listOf(user)
      )
    )
  }

  @Test
  fun insertSelect() {
    assertThat(
      psql.generate(
        Vehicle::class.insert(Vehicle::distance)
          .from(Vehicle::class.select(Vehicle::distance + 10).where(Vehicle::id eq uuid))
          .returning(Vehicle::id)
      )
    ).isEqualTo(
      InsertStatement<Vehicle>(
        "INSERT INTO \"vehicles\" (\"distance\") (SELECT (\"vehicles\".\"distance\" + ?) FROM \"vehicles\" WHERE (\"vehicles\".\"id\" = ?)) RETURNING \"vehicles\".\"id\"",
        "vehicles",
        values = listOf(10, uuid)
      )
    )
  }

  @Test
  fun `insert-select between two tables with manual alias`() {
    val vehicleAlias = Vehicle::class.alias("a_bird")

    // This query is gibberish (inserts zone ID into reviewer ID) as a result of history and you shouldn't find meaning in it.
    assertThat(
      psql.generate(
        Corral::class
          .insert(
            Corral::location,
            Corral::reviewerId,
            Corral::notes,
            Corral::maxQuantity
          )
          .from(
            vehicleAlias.select(
              vehicleAlias[Vehicle::location],
              vehicleAlias[Vehicle::zoneId],
              literal("blah"),
              literal(1)
            )
              .where(vehicleAlias[Vehicle::batteryLevel] lte literal(10))
          )
          .returning(Corral::id)
      )
    ).isEqualTo(
      InsertStatement<Vehicle>(
        "INSERT INTO \"corrals\" (\"location\", \"reviewer_id\", \"notes\", \"max_quantity\") (SELECT \"a_bird\".\"location\", \"a_bird\".\"zone_id\", ?, ? FROM \"vehicles\" \"a_bird\" WHERE (\"a_bird\".\"battery_level\" <= ?)) RETURNING \"corrals\".\"id\"",
        "corrals",
        values = listOf("blah", 1, 10)
      )
    )
  }

  @Test
  fun `insert-select between two tables with lambda syntax`() {
    // This query is gibberish (inserts zone ID into reviewer ID) as a result of history and you shouldn't find meaning in it.
    assertThat(
      psql.generate(
        Corral::class
          .insert(
            Corral::location,
            Corral::reviewerId,
            Corral::notes,
            Corral::maxQuantity
          )
          .from<Vehicle> {
            select(
              this[Vehicle::location],
              this[Vehicle::zoneId],
              literal("blah"),
              literal(1)
            )
              .where(this[Vehicle::batteryLevel] lte literal(10))
          }
          .returning(Corral::id.project())
      )
    ).isEqualTo(
      InsertStatement<Vehicle>(
        "INSERT INTO \"corrals\" (\"location\", \"reviewer_id\", \"notes\", \"max_quantity\") (SELECT \"autogenerated_alias_insert_Vehicle\".\"location\", \"autogenerated_alias_insert_Vehicle\".\"zone_id\", ?, ? FROM \"vehicles\" \"autogenerated_alias_insert_Vehicle\" WHERE (\"autogenerated_alias_insert_Vehicle\".\"battery_level\" <= ?)) RETURNING \"corrals\".\"id\"",
        "corrals",
        values = listOf("blah", 1, 10)
      )
    )
  }

  @Test
  fun update() {
    val serial: String? = null
    var update = psql.generate(
      Vehicle::class
        .updateAll(
          Vehicle::batteryLevel to 100
        )
    )
    var expectedSql = "UPDATE \"vehicles\" SET \"battery_level\" = ?"
    assertThat(update.sql).isEqualTo(expectedSql)
    assertThat(update.values).isEqualTo(listOf(100))

    update = psql.generate(
      Vehicle::class
        .update(
          Vehicle::batteryLevel to 100,
          Vehicle::distance to Vehicle::distance.plus(10),
          Vehicle::code to "FOO",
          Vehicle::serialNumber to serial
        )
        .where(Vehicle::id eq uuid)
    )
    expectedSql =
      "UPDATE \"vehicles\" SET \"battery_level\" = ?, \"distance\" = (\"vehicles\".\"distance\" + ?), \"code\" = ? WHERE (\"vehicles\".\"id\" = ?)"
    assertThat(update.sql).isEqualTo(expectedSql)
    assertThat(update.values).isEqualTo(listOf(100, 10, "FOO", uuid))

    update = psql.generate(
      Vehicle::class
        .update(
          Vehicle::batteryLevel to 200,
          Vehicle::distance to Vehicle::distance.plus(30),
          Vehicle::code to "BAR",
          Vehicle::serialNumber.nullable() to serial
        )
        .where(Vehicle::id eq uuid)
    )
    expectedSql =
      "UPDATE \"vehicles\" SET \"battery_level\" = ?, \"distance\" = (\"vehicles\".\"distance\" + ?), \"code\" = ?, \"serial_number\" = NULL WHERE (\"vehicles\".\"id\" = ?)"
    assertThat(update.sql).isEqualTo(expectedSql)
    assertThat(update.values).isEqualTo(listOf(200, 30, "BAR", uuid))

    update = psql.generate(
      Vehicle::class
        .updateAll(
          Vehicle::batteryLevel to (Vehicle::batteryLevel + 1)
        )
        .returningAll()
    )
    expectedSql = "UPDATE \"vehicles\" SET \"battery_level\" = (\"vehicles\".\"battery_level\" + ?) RETURNING *"
    assertThat(update.sql).isEqualTo(expectedSql)
    assertThat(update.values).isEqualTo(listOf(1))

    val b = Board::class.alias("b")
    update = psql.generate(
      Vehicle::class
        .update(Vehicle::batteryLevel to (Vehicle::batteryLevel + 1))
        .from(Board::class.selectAll().where(Board::iccid.isNotNull()), b)
        .where(b[Board::vehicleId] eq Vehicle::id)
    )
    expectedSql =
      "UPDATE \"vehicles\" SET \"battery_level\" = (\"vehicles\".\"battery_level\" + ?) FROM (SELECT \"boards\".* FROM \"boards\" WHERE (\"boards\".\"iccid\" IS NOT NULL)) \"b\" WHERE (\"b\".\"vehicle_id\" = \"vehicles\".\"id\")"
    assertThat(update.sql).isEqualTo(expectedSql)
    assertThat(update.values).isEqualTo(listOf(1))

    val veh = Vehicle::class.alias("veh")
    val brd = Board::class.alias("brd")
    update = psql.generate(
      veh
        .update(Vehicle::batteryLevel to (Vehicle::batteryLevel + 1))
        .from(
          Board::class
            .selectAll()
            .where(Board::iccid.isNotNull()),
          brd
        )
        .where(brd[Board::vehicleId] eq veh[Vehicle::id])
    )
    expectedSql =
      "UPDATE \"vehicles\" \"veh\" SET \"battery_level\" = (\"vehicles\".\"battery_level\" + ?) FROM (SELECT \"boards\".* FROM \"boards\" WHERE (\"boards\".\"iccid\" IS NOT NULL)) \"brd\" WHERE (\"brd\".\"vehicle_id\" = \"veh\".\"id\")"
    assertThat(update.sql).isEqualTo(expectedSql)
    assertThat(update.values).isEqualTo(listOf(1))

    val drop = Drop::class.alias("d")
    val selected = Drop::class.alias("selected")
    val updateDrop = psql.generate(
      drop.update(Drop::userId to "abc")
        .from(
          Drop::class.selectAll()
            .where(
              and(
                Drop::userId.isNull()
              )
            )
            .join(Corral::class.select(Corral::location).on(Drop::nestId eq Corral::id))
            .orderBy(Drop::releaseLocation.distanceFrom(Point(34.0, -118.0)).asc())
            .limit(3),
          selected
        )
        .where(selected[Drop::id] eq drop[Drop::id])
    )
    expectedSql =
      "UPDATE \"drops\" \"d\" SET \"user_id\" = ? FROM (SELECT \"drops\".*, \"corrals\".\"location\" FROM \"drops\" JOIN \"corrals\" ON (\"drops\".\"nest_id\" = \"corrals\".\"id\") WHERE ((\"drops\".\"user_id\" IS NULL)) ORDER BY st_distance(\"drops\".\"release_location\", ?) ASC LIMIT 3) \"selected\" WHERE (\"selected\".\"id\" = \"d\".\"id\")"
    assertThat(updateDrop.sql).isEqualTo(expectedSql)
    assertThat(updateDrop.values).isEqualTo(listOf("abc", Point(34.0, -118.0)))
  }

  @Test
  fun `update with function`() {
    val now = OffsetDateTime.now()
    val claimDurationMinutes = 30
    val unclaimAtTime = now.plusMinutes(claimDurationMinutes.toLong())
    val userId = UUID.randomUUID()
    val dropId = UUID.randomUUID()
    val update = psql.generate(
      Drop::class
        .update(
          Drop::userId to userId,
          Drop::claimedAt to now,
          Drop::unclaimAt to unclaimAtTime,
          Drop::expireAt to greatest(
            Column(Drop::expireAt, Table(Drop::class)),
            Value(unclaimAtTime)
          )
        )
        .where(
          and(
            Drop::id eq dropId
          )
        )
        .returningAll()
    )
    val expectedSql =
      "UPDATE \"drops\" SET \"user_id\" = ?, \"claimed_at\" = ?, \"unclaim_at\" = ?, \"expire_at\" = greatest(\"drops\".\"expire_at\", ?) WHERE ((\"drops\".\"id\" = ?)) RETURNING *"
    assertThat(update.sql).isEqualTo(expectedSql)
    assertThat(update.values).isEqualTo(listOf(userId, now, unclaimAtTime, unclaimAtTime, dropId))
  }

  @Test
  fun `update with alias`() {
    val id = UUID.randomUUID()
    val b = Vehicle::class.alias("b")

    val update = psql.generate(
      b.update(
        Vehicle::batteryLevel to 100
      ).where(b[Vehicle::id] eq literal(id))
    )
    val expectedSql = "UPDATE \"vehicles\" \"b\" SET \"battery_level\" = ? WHERE (\"b\".\"id\" = ?)"
    assertThat(update.sql).isEqualTo(expectedSql)
    assertThat(update.values).isEqualTo(listOf(100, id))
  }

  @Test
  fun `update from table list`() {
    // This query is nonsense, don't try to make sense of it
    val bt = VehicleTrack::class.alias("bt")

    val update = psql.generate(
      Vehicle::class.update(Vehicle::batteryLevel to (Vehicle::batteryLevel + 10))
        .from(bt)
        .where(bt[VehicleTrack::vehicleId] eq Vehicle::id)
    )
    val expectedSql =
      "UPDATE \"vehicles\" SET \"battery_level\" = (\"vehicles\".\"battery_level\" + ?) FROM \"vehicle_tracks\" \"bt\" WHERE (\"bt\".\"vehicle_id\" = \"vehicles\".\"id\")"
    assertThat(update.sql).isEqualTo(expectedSql)
    assertThat(update.values).isEqualTo(listOf(10))
  }

  @Test
  fun `update from table list with multiple tables`() {
    // This query is nonsense, don't try to make sense of it
    val bt = VehicleTrack::class.alias("bt")
    val z = Zone::class.alias("z")

    val update = psql.generate(
      Vehicle::class.update(Vehicle::batteryLevel to (Vehicle::batteryLevel + 10))
        .from(bt, z)
        .where(
          (bt[VehicleTrack::vehicleId] eq Vehicle::id) and (
            z[Zone::id] eq Vehicle::zoneId and (
              z[Zone::name] eq literal(
                "Santa Monica"
              )
              )
            )
        )
    )

    val expectedSql =
      "UPDATE \"vehicles\" SET \"battery_level\" = (\"vehicles\".\"battery_level\" + ?) FROM \"vehicle_tracks\" \"bt\", \"zones\" \"z\" WHERE ((\"bt\".\"vehicle_id\" = \"vehicles\".\"id\") AND ((\"z\".\"id\" = \"vehicles\".\"zone_id\") AND (\"z\".\"name\" = ?)))"

    assertThat(update.sql).isEqualTo(expectedSql)
    assertThat(update.values).isEqualTo(listOf(10, "Santa Monica"))
  }

  @Test
  fun updateFromReturningIntoFields() {
    val id = UUID.randomUUID()
    val update = psql.generate(
      Vehicle::class.update(Vehicle::batteryLevel to (Vehicle::batteryLevel + 10))
        .from(Table(Board::class))
        .where(
          and(
            Board::vehicleId eq Vehicle::id,
            Vehicle::id eq id
          )
        )
        .returning(Projection(All(Table(Vehicle::class))))
        .returning(IntoField(Board::class, Vehicle::board.column()))
    )
    val expectedSql =
      "UPDATE \"vehicles\" SET \"battery_level\" = (\"vehicles\".\"battery_level\" + ?) FROM \"boards\" WHERE ((\"boards\".\"vehicle_id\" = \"vehicles\".\"id\") AND (\"vehicles\".\"id\" = ?)) RETURNING \"vehicles\".*, NULL AS \"start:board\", \"boards\".*, NULL AS \"end:board\""
    assertThat(update.sql).isEqualTo(expectedSql)
    assertThat(update.values).isEqualTo(listOf(10, id))
  }

  @Test
  fun casts() {
    val boundary = LatLonRect(34.0, 34.01, -118.0, -117.99)
    assertThat(
      psql.generate(
        Corral::class.selectAll()
          .where(
            Corral::location.cast(TypeSpec<Geometry>("geometry")).within(boundary.asPolygon)
          )
      ).let { listOf(it.sql, it.values) }
    ).isEqualTo(
      listOf(
        "SELECT \"corrals\".* FROM \"corrals\" WHERE st_within((\"corrals\".\"location\")::geometry, ?)",
        listOf(boundary.asPolygon)
      )
    )
  }

  @Test
  fun charLength() {
    assertThat(
      psql.generate(
        Corral::class.selectAll()
          .where(
            Corral::notes.charLength() gt 47
          )
      ).let { listOf(it.sql, it.values) }
    ).isEqualTo(
      listOf(
        "SELECT \"corrals\".* FROM \"corrals\" WHERE (char_length(\"corrals\".\"notes\") > ?)",
        listOf(47)
      )
    )
  }

  @Test
  fun isIn() {
    assertThat(
      psql.generate(
        Corral::maxQuantity.isIn(listOf(1, 2, 3))
      )
    ).isEqualTo(
      Fragment(
        "(\"corrals\".\"max_quantity\" IN (?, ?, ?))",
        listOf(1, 2, 3)
      )
    )
  }

  @Test
  fun isInEmptyList() {
    assertThat(
      psql.generate(
        Corral::maxQuantity.isIn(listOf())
      )
    ).isEqualTo(
      Fragment(
        "(((\"corrals\".\"max_quantity\") = NULL) IS NOT NULL)",
        listOf()
      )
    )
  }

  @Test
  fun isNotIn() {
    assertThat(
      psql.generate(
        Corral::maxQuantity.isNotIn(listOf(1, 2, 3))
      )
    ).isEqualTo(
      Fragment(
        "(\"corrals\".\"max_quantity\" NOT IN (?, ?, ?))",
        listOf(1, 2, 3)
      )
    )
  }

  @Test
  fun isNotInEmptyList() {
    assertThat(
      psql.generate(
        Corral::maxQuantity.isNotIn(listOf())
      )
    ).isEqualTo(
      Fragment(
        "(((\"corrals\".\"max_quantity\") = NULL) IS NULL)",
        listOf()
      )
    )
  }

  @Test
  fun isInOrNotInEmptyList() {
    assertThat(
      psql.generate(
        Vehicle::distance.isIn(listOf()) or Vehicle::estimatedRange.isNotIn(listOf())
      )
    ).isEqualTo(
      Fragment(
        "((((\"vehicles\".\"distance\") = NULL) IS NOT NULL) OR (((\"vehicles\".\"estimated_range\") = NULL) IS NULL))",
        listOf()
      )
    )
  }

  @Test
  fun columnOrder() {
    assertThat(
      psql.generate(
        Vehicle::code.asc().nullsFirst()
      )
    ).isEqualTo(
      Fragment(
        "\"vehicles\".\"code\" ASC NULLS FIRST",
        listOf()
      )
    )
  }

  @Test
  fun caseOrder() {
    assertThat(
      psql.generate(
        skate.caseOrder(
          (Vehicle::distance lt 100).then(1),
          (Vehicle::estimatedRange gt 10).then(2)
        )
      )
    ).isEqualTo(
      Fragment(
        "CASE WHEN (\"vehicles\".\"distance\" < ?) THEN ? WHEN (\"vehicles\".\"estimated_range\" > ?) THEN ? END",
        listOf(100, 1, 10, 2)
      )
    )
  }

  @Test
  fun existsTest() {
    assertThat(
      psql.generate(
        exists(
          Ride::class.selectOne().where(Ride::vehicleId eq Vehicle::id)
        )
      )
    ).isEqualTo(
      Fragment(
        "EXISTS (SELECT ? FROM \"rides\" WHERE (\"rides\".\"vehicle_id\" = \"vehicles\".\"id\"))",
        values = listOf(1)
      )
    )
  }

  @Test
  fun notExistsTest() {
    assertThat(
      psql.generate(
        exists(
          Ride::class.selectOne().where(Ride::vehicleId eq Vehicle::id),
          not = true
        )
      )
    ).isEqualTo(
      Fragment(
        "NOT EXISTS (SELECT ? FROM \"rides\" WHERE (\"rides\".\"vehicle_id\" = \"vehicles\".\"id\"))",
        values = listOf(1)
      )
    )
  }

  @Test
  fun distinctOnTest() {
    assertThat(
      psql.generate(
        Vehicle::class.selectAll()
          .distinctOn(
            Vehicle::estimatedRange,
            Vehicle::batteryLevel
          )
          .where(
            and(
              Vehicle::estimatedRange.isNotNull(),
              Vehicle::estimatedRange gt 5
            )
          )
          .orderBy(Vehicle::estimatedRange.desc())
      ).let { listOf(it.sql, it.values) }
    ).isEqualTo(
      listOf(
        "SELECT DISTINCT ON (\"vehicles\".\"estimated_range\", \"vehicles\".\"battery_level\") \"vehicles\".* FROM \"vehicles\" " +
          "WHERE ((\"vehicles\".\"estimated_range\" IS NOT NULL) AND (\"vehicles\".\"estimated_range\" > ?)) ORDER BY \"vehicles\".\"estimated_range\" DESC",
        listOf(5)
      )
    )
  }

  @Test
  fun distinctTest() {
    assertThat(
      psql.generate(
        Vehicle::class.select(Vehicle::id)
          .distinct()
          .where(
            and(
              Vehicle::estimatedRange.isNotNull(),
              Vehicle::estimatedRange gt 5
            )
          )
          .orderBy(Vehicle::estimatedRange.desc())
      ).let { listOf(it.sql, it.values) }
    ).isEqualTo(
      listOf(
        "SELECT DISTINCT \"vehicles\".\"id\" FROM \"vehicles\" " +
          "WHERE ((\"vehicles\".\"estimated_range\" IS NOT NULL) AND (\"vehicles\".\"estimated_range\" > ?)) ORDER BY \"vehicles\".\"estimated_range\" DESC",
        listOf(5)
      )
    )
  }

  @Test
  fun `insertConflict - update`() {
    assertThat(
      psql.generate(
        Vehicle::class
          .insert(Vehicle::id, Vehicle::code)
          .values(Vehicle(code = "insert"))
          .onConflict(Vehicle::id).update(Vehicle::code)
      ).let { listOf(it.sql, it.values) }
    ).isEqualTo(
      listOf(
        """
          INSERT INTO "vehicles" ("id", "code") VALUES (#vehicles0.id, #vehicles0.code) ON CONFLICT ("id") DO UPDATE SET "code" = EXCLUDED."code"
        """.trimIndent(),
        emptyList<Any>()
      )
    )

    assertThat(
      psql.generate(
        Vehicle::class
          .insert(Vehicle::id, Vehicle::code)
          .values(Vehicle(code = "insert"))
          .onConflict(Vehicle::id).update(Vehicle::code)
          .returningAll()
      ).let { listOf(it.sql, it.values) }
    ).isEqualTo(
      listOf(
        """
          INSERT INTO "vehicles" ("id", "code") VALUES (#vehicles0.id, #vehicles0.code) ON CONFLICT ("id") DO UPDATE SET "code" = EXCLUDED."code" RETURNING *
        """.trimIndent(),
        emptyList<Any>()
      )
    )

    assertThat(
      psql.generate(
        Vehicle::class
          .insert(Vehicle::id, Vehicle::code)
          .values(Vehicle(code = "insert"))
          .onConflict(Vehicle::id).update(Vehicle::code)
          .returning(Vehicle::id, Vehicle::code)
      ).let { listOf(it.sql, it.values) }
    ).isEqualTo(
      listOf(
        """
          INSERT INTO "vehicles" ("id", "code") VALUES (#vehicles0.id, #vehicles0.code) ON CONFLICT ("id") DO UPDATE SET "code" = EXCLUDED."code" RETURNING "vehicles"."id", "vehicles"."code"
        """.trimIndent(),
        emptyList<Any>()
      )
    )
  }

  @Test
  fun `insertConflict - update multiple columns`() {
    assertThat(
      psql.generate(
        Vehicle::class
          .insert(Vehicle::id, Vehicle::code, Vehicle::cellular)
          .values(Vehicle(code = "insert", cellular = false))
          .onConflict(Vehicle::id).update(Vehicle::code, Vehicle::cellular)
      ).let { listOf(it.sql, it.values) }
    ).isEqualTo(
      listOf(
        """
          INSERT INTO "vehicles" ("id", "code", "cellular") VALUES (#vehicles0.id, #vehicles0.code, #vehicles0.cellular) ON CONFLICT ("id") DO UPDATE SET "code" = EXCLUDED."code", "cellular" = EXCLUDED."cellular"
        """.trimIndent(),
        emptyList<Any>()
      )
    )
  }

  @Test
  fun `insertConflict - do nothing`() {
    assertThat(
      psql.generate(
        Vehicle::class
          .insert(Vehicle::id)
          .values(Vehicle())
          .onConflict(Vehicle::id).doNothing()
      ).let { listOf(it.sql, it.values) }
    ).isEqualTo(
      listOf(
        """
          INSERT INTO "vehicles" ("id") VALUES (#vehicles0.id) ON CONFLICT ("id") DO NOTHING
        """.trimIndent(),
        emptyList<Any>()
      )
    )

    assertThat(
      psql.generate(
        Vehicle::class
          .insert(Vehicle::id)
          .values(Vehicle())
          .onConflict(Vehicle::id).doNothing()
          .returningAll()
      ).let { listOf(it.sql, it.values) }
    ).isEqualTo(
      listOf(
        """
          INSERT INTO "vehicles" ("id") VALUES (#vehicles0.id) ON CONFLICT ("id") DO NOTHING RETURNING *
        """.trimIndent(),
        emptyList<Any>()
      )
    )

    assertThat(
      psql.generate(
        Vehicle::class
          .insert(Vehicle::id)
          .values(Vehicle())
          .onConflict(Vehicle::id).doNothing()
          .returning(Vehicle::code)
      ).let { listOf(it.sql, it.values) }
    ).isEqualTo(
      listOf(
        """
          INSERT INTO "vehicles" ("id") VALUES (#vehicles0.id) ON CONFLICT ("id") DO NOTHING RETURNING "vehicles"."code"
        """.trimIndent(),
        emptyList<Any>()
      )
    )
  }

  @Test
  fun `insertConflict - multiple conflict targets`() {
    assertThat(
      psql.generate(
        Vehicle::class
          .insert(Vehicle::id)
          .values(Vehicle())
          .onConflict(Vehicle::id, Vehicle::code).doNothing()
      ).let { listOf(it.sql, it.values) }
    ).isEqualTo(
      listOf(
        """
          INSERT INTO "vehicles" ("id") VALUES (#vehicles0.id) ON CONFLICT ("id", "code") DO NOTHING
        """.trimIndent(),
        emptyList<Any>()
      )
    )
  }

  @Test
  fun `insertConflict - NO conflict targets`() {
    assertThat(
      psql.generate(
        Vehicle::class
          .insert(Vehicle::id)
          .values(Vehicle())
          .onConflict().doNothing()
      ).let { listOf(it.sql, it.values) }
    ).isEqualTo(
      listOf(
        """
          INSERT INTO "vehicles" ("id") VALUES (#vehicles0.id) ON CONFLICT DO NOTHING
        """.trimIndent(),
        emptyList<Any>()
      )
    )
  }

  @Test
  fun `selectCountDistinct - simple`() {
    assertThat(
      psql.generate(
        Vehicle::class
          .selectCountDistinct(Vehicle::model, "c")
          .where(null)
      ).let { listOf(it.sql, it.values) }
    ).isEqualTo(
      listOf(
        """
          SELECT count(DISTINCT "model") AS c FROM "vehicles"
        """.trimIndent(),
        emptyList<Any>()
      )
    )
  }

  @Test
  fun `case - simple`() {
    assertThat(
      psql.generate(
        Vehicle::class
          .select(
            case(Vehicle::rideId.isNull().then(Vehicle::location.column()), fallback = false)
          )
      ).let { listOf(it.sql, it.values) }
    ).isEqualTo(
      listOf(
        """
          SELECT CASE WHEN ("vehicles"."ride_id" IS NULL) THEN "vehicles"."location" ELSE ? END FROM "vehicles"
        """.trimIndent(),
        listOf(false)
      )
    )
  }

  @Test
  fun `case - aggregate`() {
    assertThat(
      psql.generate(
        Vehicle::class
          .select(
            case(Vehicle::rideId.isNull().then(1), fallback = 0).sum()
          )
      ).let { listOf(it.sql, it.values) }
    ).isEqualTo(
      listOf(
        """
          SELECT sum(CASE WHEN ("vehicles"."ride_id" IS NULL) THEN ? ELSE ? END) FROM "vehicles"
        """.trimIndent(),
        listOf(1, 0)
      )
    )
  }

  class AtTimeZoneTestArgumentsProvider : ArgumentsProvider {
    override fun provideArguments(context: ExtensionContext?): Stream<out Arguments> {
      return Stream.of(
        Arguments.of(
          Vehicle::createdAt.atTimeZone("America/Los_Angeles"),
          "SELECT (\"vehicles\".\"created_at\" AT TIME ZONE ?) FROM \"vehicles\"",
          listOf("America/Los_Angeles")
        ),
        Arguments.of(
          Vehicle::createdAt.atTimeZone(Zone::timeZone.column()),
          "SELECT (\"vehicles\".\"created_at\" AT TIME ZONE \"zones\".\"time_zone\") FROM \"vehicles\"",
          emptyList<Any>()
        ),
        Arguments.of(
          now().atTimeZone("America/Los_Angeles"),
          "SELECT (now() AT TIME ZONE ?) FROM \"vehicles\"",
          listOf("America/Los_Angeles")
        ),
        Arguments.of(
          now().atTimeZone(Zone::timeZone.column()),
          "SELECT (now() AT TIME ZONE \"zones\".\"time_zone\") FROM \"vehicles\"",
          emptyList<Any>()
        )
      )
    }
  }

  @ParameterizedTest
  @ArgumentsSource(AtTimeZoneTestArgumentsProvider::class)
  fun atTimeZone(
    expression: Expression<Any>,
    expectedSql: String,
    expectedValues: List<Any>
  ) {
    val result = psql.generate(
      Vehicle::class.select(expression)
    )
    assertThat(result.sql).isEqualTo(expectedSql)
    assertThat(result.values).isEqualTo(expectedValues)
  }

  @Test
  fun `atTimeZone - throws an error when invalid time zones are provided`() {
    assertThatThrownBy {
      psql.generate(
        Vehicle::class.select(now().atTimeZone("Foo/Bar"))
      )
    }
      .isInstanceOf(IllegalArgumentException::class.java)
      .hasMessage("'Foo/Bar' is not a valid time zone")
  }
}
