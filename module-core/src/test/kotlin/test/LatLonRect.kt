package test

import net.postgis.jdbc.geometry.LinearRing
import net.postgis.jdbc.geometry.Point
import net.postgis.jdbc.geometry.Polygon

data class LatLonRect(
  val minLatitude: Double,
  val maxLatitude: Double,
  val minLongitude: Double,
  val maxLongitude: Double
) {
  val asPolygon: Polygon
    get() {
      return Polygon(
        arrayOf(
          LinearRing(
            arrayOf(
              Point(minLongitude, minLatitude),
              Point(minLongitude, maxLatitude),
              Point(maxLongitude, maxLatitude),
              Point(maxLongitude, minLatitude),
              Point(minLongitude, minLatitude)
            )
          )
        )
      )
    }
}
