package skate.test

import skate.ColumnName
import skate.TableName
import skate.Transient
import skate.column
import skate.eq
import skate.from
import skate.generate
import skate.insert
import skate.into
import skate.leftJoin
import skate.on
import skate.to
import skate.queryFirst
import skate.returning
import skate.selectAll
import skate.update
import skate.values
import skate.where
import java.util.UUID
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import skate.All
import skate.IntoField
import skate.Projection
import skate.Table
import skate.execute
import skate.executeRaw
import skate.query

class TransientTest : AbstractTest() {

  @BeforeEach
  fun setUp() {
    db.executeRaw(DROP_TABLE_SQL)
    db.executeRaw(CREATE_TABLE_SQL)
  }

  companion object {
    private const val CREATE_TABLE_SQL =
      """
        CREATE TABLE IF NOT EXISTS vehicle (
          id UUID NOT NULL PRIMARY KEY,
          code TEXT
        );

        CREATE TABLE IF NOT EXISTS vehicle_lifecycle (
          id UUID NOT NULL PRIMARY KEY,
          vehicle_id UUID NOT NULL,
          status TEXT
        );
      """

    private const val DROP_TABLE_SQL =
      """
        DROP TABLE IF EXISTS vehicle;
        DROP TABLE IF EXISTS vehicle_lifecycle;
      """
  }

  @Test
  fun `insert with default value from non-null transient column`() {
    val vehicle = Vehicle()
    Vehicle::class
      .insert()
      .values(vehicle)
      .generate((db.dialect))
      .execute(db)

    val lifecycle = VehicleLifecycle(vehicleId = vehicle.id)
    VehicleLifecycle::class
      .insert()
      .values(lifecycle)
      .generate((db.dialect))
      .execute(db)

    val result = Vehicle::class
      .selectAll()
      .where(Vehicle::id eq vehicle.id)
      .leftJoin(
        VehicleLifecycle::class
          .selectAll()
          .into(Vehicle::lifecycle)
          .on(Vehicle::id eq VehicleLifecycle::vehicleId)
      )
      .generate((db.dialect))
      .query<Vehicle>(db)
      .first()

    assertThat(result).isEqualTo(vehicle.copy(code = null, lifecycle = lifecycle))
  }

  @Test
  fun `update record with join returns transient values`() {
    val vehicle = Vehicle()
    Vehicle::class
      .insert()
      .values(vehicle)
      .generate((db.dialect))
      .execute(db)

    val lifecycle = VehicleLifecycle(vehicleId = vehicle.id, status = "available")
    VehicleLifecycle::class
      .insert()
      .values(lifecycle)
      .generate((db.dialect))
      .execute(db)

    val result = db.jdbi.withHandle<Vehicle, Exception> {
      Vehicle::class
        .update(Vehicle::code to "test")
        .from(Table(VehicleLifecycle::class))
        .where(Vehicle::id eq vehicle.id)
        .returning(Projection(All(Table(Vehicle::class))))
        .returning(IntoField(VehicleLifecycle::class, Vehicle::lifecycle.column()))
        .generate((db.dialect))
        .queryFirst(it, 5)
    }

    assertThat(result).isEqualTo(vehicle.copy(code = "test", lifecycle = lifecycle))
  }

  @Test
  fun `update record returning different result set`() {
    val vehicle = Vehicle()
    Vehicle::class
      .insert()
      .values(vehicle)
      .generate((db.dialect))
      .execute(db)

    val result = db.jdbi.withHandle<UUID, Exception> {
      Vehicle::class
        .update(Vehicle::code to "test")
        .where(Vehicle::id eq vehicle.id)
        .returning(Vehicle::id)
        .generate((db.dialect))
        .queryFirst(it, 5)
    }
    assertThat(result).isEqualTo(vehicle.id)
  }

  @TableName("vehicle_lifecycle")
  data class VehicleLifecycle(
    val id: UUID = UUID.randomUUID(),
    val vehicleId: UUID = UUID.randomUUID(),
    val status: String = "unknown"
  )

  @TableName("vehicle")
  data class Vehicle(
    val id: UUID = UUID.randomUUID(),

    @ColumnName("code")
    val nullableCode: String? = null,

    @Transient
    val code: String? = "hello",

    @Transient
    val lifecycle: VehicleLifecycle? = VehicleLifecycle(vehicleId = id, status = "unknown")
  )
}
