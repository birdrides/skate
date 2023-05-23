package test

import org.postgis.LinearRing
import org.postgis.Point
import org.postgis.Polygon

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
