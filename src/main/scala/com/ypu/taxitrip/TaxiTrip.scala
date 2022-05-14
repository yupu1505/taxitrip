package com.ypu.taxitrip

import com.github.tototoshi.csv.CSVReader
import com.ypu.taxitrip.TaxiTrip.{readTripData, readZoneMapFromCsv}
import org.joda.time._
import org.joda.time.format.DateTimeFormat
import scala.util.{Failure, Random, Success, Try}

object gridX {
    implicit val latLngGrid = readZoneMapFromCsv("/Users/upu/moody/taxi_zones.csv").toLatLngGrid()
    implicit val dateTimeGrid = readTripData("/Users/upu/moody/green_tripdata_2021-03.csv").getDateTimeGrid()
}

case class Pair (
   val num1: Double = 0.0,
   val num2: Double = 0.0
) {
    def plus(p:Pair): Pair = {
        Pair(num1 + p.num1, num2 + p.num2)
    }
    def divide(n: Int): Pair = {
        Pair(num1/n, num2/n)
    }
}

case class TripData (
    val trips: List[Trip]
) {
    def getDateTimeGrid(gridSize: Int = 10): DateTimeGrid = {
      val dateTimeMin: DateTime = trips.flatMap(trip=>List(trip.lpep_pickup_datetime, trip.lpep_dropoff_datetime)).foldLeft(new DateTime("2099-05-05T10:11:12.123")){(acc, curr) => if(acc.isAfter(curr)) {curr} else {acc}}
      val dateTimeMax: DateTime = trips.flatMap(trip=>List(trip.lpep_pickup_datetime, trip.lpep_dropoff_datetime)).foldLeft(new DateTime("1999-05-05T10:11:12.123")){(acc, curr) => if(acc.isAfter(curr)) {acc} else {curr}}
      val dateTimeLists: List[List[DateTime]] = Util.gridifyDateTime(dateTimeMin, dateTimeMax, gridSize)
      DateTimeGrid(dateTimeLists.map(dateTimeList =>
        DateTimeCell(dateTimeList(0),dateTimeList(1))
      ))
    }
}

case class Trip (
    val id: Long,
    val VendorID: Int,
    val lpep_pickup_datetime: DateTime,
    val lpep_dropoff_datetime: DateTime,
    val store_and_fwd_flag: String,
    val RatecodeID: Double,
    val PULocationID: Int,
    val DOLocationID: Int,
    val passenger_count: Double,
    val trip_distance: Double,
    val fare_amount: Double,
    val extra: Double,
    val mta_tax: Double,
    val tip_amount: Double,
    val tolls_amount: Double,
    //val ehail_fee: Double, // this field is empty for the trip record
    val improvement_surcharge: Double,
    val total_amount: Double,
    val payment_type: Double,
    val trip_type: Double,
    val congestion_surcharge: Double
) {
    def fromLatLngGridCell(implicit latLngGrid: LatLngGrid): List[LatLngGridCell] = {
        //calculate from PULocationID
        latLngGrid.latLngCells.flatten.filter(latLngCell =>
            latLngCell.locationIds.contains(PULocationID)
        )
    }
    def toLatLngGridCell(implicit latLngGrid: LatLngGrid): List[LatLngGridCell] = {
        //calculate from DOLocationID
        latLngGrid.latLngCells.flatten.filter(latLngCell =>
            latLngCell.locationIds.contains(DOLocationID)
        )
    }
    def fromDateTimeGridCell(implicit dateTimeGrid: DateTimeGrid): DateTimeCell = {
        //calculate from lpep_pickup_datetime
        dateTimeGrid.dateTimeCells.find(dtCell =>
          (dtCell.fromDateTime.isBefore(lpep_pickup_datetime) || dtCell.fromDateTime == lpep_pickup_datetime) &&
            (dtCell.toDateTime.isAfter(lpep_pickup_datetime) || dtCell.toDateTime == lpep_pickup_datetime)
        ).get
    }
    def toDateTimeGridCell(implicit dateTimeGrid: DateTimeGrid): DateTimeCell = {
        //calculate from lpep_dropoff_datetime
        dateTimeGrid.dateTimeCells.find(dtCell =>
          (dtCell.fromDateTime.isBefore(lpep_dropoff_datetime) || dtCell.fromDateTime == lpep_dropoff_datetime) &&
            (dtCell.toDateTime.isAfter(lpep_dropoff_datetime) || dtCell.toDateTime == lpep_dropoff_datetime)
        ).get
    }
}

case class LatLng(
    val lat: Double,
    val lng: Double
) {
  override def toString(): String = {
      s"${lat},${lng}"
  }
}

case class Zone (
    val id: Int,    //its just locationId
    val latlngs: List[LatLng]
)

object LineSegmentCrossDetector {
    private def ccw(A:LatLng,B:LatLng,C:LatLng): Boolean = {
        val l = (C.lng-A.lng) * (B.lat-A.lat)
        val r = (B.lng-A.lng) * (C.lat-A.lat)
        /*if (l>r) {
            println(s"found ${l} > ${r}? ${l > r}")
        }*/
        l > r
    }

    //Return true if line segments AB and CD intersect
    def intersect(A: LatLng,B: LatLng,C: LatLng,D:LatLng): Boolean = {
        ccw(A, C, D) != ccw(B, C, D) && ccw(A, B, C) != ccw(A, B, D)
    }
}

class PolygonOverlapDetector(val poly1: List[LatLng], val poly2: List[LatLng]) {
    val sampleSize = 50
    //randomly select 2 positions on a poly to create a segment, forming a mesh overlay of the polygon
    //if num positions of poly is big (> 100), we sample 100 positions randomly
    def segments(poly: List[LatLng]): List[Tuple2[LatLng,LatLng]] = {
        val sampledPoly = if (poly.length > sampleSize) {
            Random.shuffle((poly).take(sampleSize))
        } else poly
        sampledPoly.flatMap {latlng1 =>
            sampledPoly.map {latlng2 =>
                Tuple2(latlng1, latlng2)
            }
        }.filterNot{tup => tup._1 == tup._2}
    }

    def overlap(): Boolean = {
        //poly1 and poly2 overlap if any segment of segments(poly1) cross any segment in segments(poly2)
        //for resolution purpose, subtract zoneCenter and times 100
        val zoneCenter = {
          val pointsSum = poly1.foldLeft(LatLng(0.0,0.0)){(acc,item) => LatLng(acc.lat+item.lat, acc.lng+item.lng)}
          LatLng(pointsSum.lat/poly1.size, pointsSum.lng/poly1.size)
        }
        //moved by center and *100
        val segPoly1: List[(LatLng,LatLng)] = segments(poly1).map(segmentLatLngPair => {
            Tuple2(
              LatLng(lat = (segmentLatLngPair._1.lat - zoneCenter.lat)*100, lng = (segmentLatLngPair._1.lng  - zoneCenter.lng)*100),
              LatLng(lat = (segmentLatLngPair._2.lat - zoneCenter.lat)*100, lng = (segmentLatLngPair._2.lng - zoneCenter.lng)*100)
            )
        })
        //moved by center and *100
        val segPoly2 = segments(poly2).map(segmentLatLngPair => {
            Tuple2(
              LatLng(lat = (segmentLatLngPair._1.lat - zoneCenter.lat)*100, lng = (segmentLatLngPair._1.lng  - zoneCenter.lng)*100),
              LatLng(lat = (segmentLatLngPair._2.lat - zoneCenter.lat)*100, lng = (segmentLatLngPair._2.lng - zoneCenter.lng)*100)
            )
        })
        //println(s"detect overlap of poly1 with ${segPoly1.size} segments and poly2 with ${segPoly2.size} segments")
        /*segPoly1.flatMap {seg1 =>
            segPoly2.map {seg2 =>
                val res = LineSegmentCrossDetector.intersect(seg1._1,seg1._2,seg2._1,seg2._2)
                println(s"...detect intersect of seg1 ${seg1} and seg2 ${seg2} got result ${res}")
                res
            }
        }.contains(true)*/
        var ol: Boolean = false
        var i: Int = 0
        var j: Int = 0
        /*for (seg1 <- segPoly1) {
          for (seg2 <- segPoly2) {
            val res = LineSegmentCrossDetector.intersect(seg1._1,seg1._2,seg2._1,seg2._2)
            if (res) {
              println(s"...detect intersect of seg1 ${seg1} and seg2 ${seg2} got result ${res}")
              ol = true
              return ol
            }
          }
        }*/
        while (i < segPoly1.size && !ol) {
          val seg1 = segPoly1(i)
          while (j < segPoly2.size && !ol) {
            val seg2 = segPoly2(j)
            val res = LineSegmentCrossDetector.intersect(seg1._1,seg1._2,seg2._1,seg2._2)
            if (res) {
              println(s"...detect intersect of seg1 ${seg1} and seg2 ${seg2} got result ${res}")
              ol = true
            }
            j = j+1
          }
          i = i+1
        }
        if(ol){
          println(s"detect overlap of poly1 ${poly1} and poly2 ${poly2}!")
        }
        ol
    }
}

case class ZoneMap (
    val zoneList: List[Zone]
) {
  def zoneCrossCell(zone: Zone, cell: (LatLng, LatLng)): Boolean = {
      val d = new PolygonOverlapDetector(zone.latlngs, LatLngGridCell(cell._1,cell._2,List()).toPolygon())
      d.overlap()
  }

  def toLatLngGrid(gridSize: Int = 10): LatLngGrid = {
    val latMin: Double = zoneList.flatMap(_.latlngs.map(_.lat)).min
    val latMax: Double = zoneList.flatMap(_.latlngs.map(_.lat)).max
    val lngMin: Double = zoneList.flatMap(_.latlngs.map(_.lng)).min
    val lngMax: Double = zoneList.flatMap(_.latlngs.map(_.lng)).max
    val latCells: List[List[Double]] = Util.gridify(latMin, latMax, gridSize)
    val lngCells: List[List[Double]] = Util.gridify(lngMin, lngMax, gridSize)
    LatLngGrid(latCells.map(latCell =>
        lngCells.map(lngCell =>
            LatLngGridCell(
                LatLng(latCell(0), lngCell(0)),
                LatLng(latCell(1), lngCell(1)),
                locationIds = zoneList.filter(zoneCrossCell(_,Tuple2(LatLng(latCell(0),lngCell(0)),LatLng(latCell(1),lngCell(1))))).map(_.id)
            )
        )
    ))
  }
}

case class LatLngGrid (
    val latLngCells: List[List[LatLngGridCell]]
)

case class LatLngGridCell (
    val lowerleft: LatLng,
    val topright: LatLng,
    val locationIds: List[Int]
) {
    val lowerright = LatLng(topright.lat, lowerleft.lng)
    val topleft = LatLng(lowerleft.lat, topright.lng)

    def toPolygon(): List[LatLng] = {
       List(lowerleft, lowerright, topright, topleft)
    }

    override def toString() : String = {
        s"${lowerleft}, ${topright}, locationsId ${locationIds.mkString(",")}"
    }
}

case class DateTimeGrid (
    val dateTimeCells: List[DateTimeCell]
) {
  override def toString(): String = {
      dateTimeCells.map {_.toString}.mkString("\n")
  }
}

case class DateTimeCell (
    val fromDateTime: DateTime,
    val toDateTime: DateTime
)

case class GridCell3D (
    val latLngGridCell: LatLngGridCell,
    val dateTimeGridCell: DateTimeCell
) {
    override def toString(): String = {
       s"${latLngGridCell.topright},${dateTimeGridCell.toDateTime}"
    }
}

case class GridTrip (
    val fromGridCell: GridCell3D,
    val toGridCell: GridCell3D,
    val trip: Trip
)

object TaxiTrip {

    def main(args: Array[String]) {
        println("read trip data...")
        val tripData = readTripData("/Users/upu/moody/green_tripdata_2021-03.csv")
        val gridCellTrips: List[GridTrip] = tripData.trips.flatMap(trip => {
          val fromDtGridCell = trip.fromDateTimeGridCell(gridX.dateTimeGrid)
          val fromGridCell3ds = trip.fromLatLngGridCell(gridX.latLngGrid).map(GridCell3D(_,fromDtGridCell))
          val toDtGridCell = trip.toDateTimeGridCell(gridX.dateTimeGrid)
          val toGridCell3ds = trip.toLatLngGridCell(gridX.latLngGrid).map(GridCell3D(_,toDtGridCell))
          fromGridCell3ds.flatMap(fromGridCell3D => {
              toGridCell3ds.map(toGridCell3D => {
                  GridTrip(
                      fromGridCell3D,
                      toGridCell3D,
                      trip
                  )
              })
          })
        })

        //see the latlngGrid
        val llg = gridX.latLngGrid
        llg.latLngCells.foreach(llcr =>
          llcr.foreach(llcc =>
            println(llcc)
          )
        )
        //now able to analyze
        /*gridCellTrips.foreach(gridCellTrip => {
            println(gridCellTrip)
        })*/
        println("Q1: The highest 50 spatio-temporal cells, by pickup coordinates, in terms of aggregated passengers count and fare amount.")
        val pcAndFAPairByPickupCell = gridCellTrips.groupBy(_.fromGridCell).map({case (fromCell: GridCell3D, gcTrips: List[GridTrip]) => {
            fromCell -> {
              val pcAndFAPairs = gcTrips.map(_.trip).distinct.map(trip =>
                  Pair(trip.passenger_count,trip.fare_amount)
              )
              pcAndFAPairs.foldLeft(Pair())(_.plus(_))
            }
        }})
        println("top 50 cell by pickup location in terms of passenger_count sum:")
        pcAndFAPairByPickupCell.toList.sortBy(-_._2.num1).take(50).foreach {case (gc3D,pair) => {
            println(s"${gc3D} sum passenger count: ${pair.num1} ")
        }}
        println("top 50 cell by pickup location in terms of fare_amount sum:")
        pcAndFAPairByPickupCell.toList.sortBy(-_._2.num2).take(50).foreach {case (gc3D,pair) => {
            println(s"${gc3D} sum passenger count: ${pair.num2} ")
        }}

        println("Q2:The highest 10 days in terms of trips count across the whole city")
        gridCellTrips.groupBy(_.fromGridCell.dateTimeGridCell.fromDateTime.dayOfYear().get()).map({case (day: Int, gcTrips: List[GridTrip]) => {
            //individualize according to trip id
            day -> gcTrips.map(_.trip).distinct.size
        }}).toList.sortBy(-_._2).take(50).foreach {case (day, numTrips) => {
            println(s"${day} numtrips: ${numTrips}")
        }}

        println("Q3: The highest 50 pickup-dropoff spatial cells pair, Say, if the highest pair is ( l1, l2), that means that trips starting from l1 and ending in l2 are the most common trips")
        gridCellTrips.groupBy(gridcelltrip => (gridcelltrip.fromGridCell.latLngGridCell, gridcelltrip.toGridCell.latLngGridCell))
            .map({case ((fromcell: LatLngGridCell, tocell: LatLngGridCell),gcTrips: List[GridTrip]) =>
                (fromcell: LatLngGridCell, tocell: LatLngGridCell) -> gcTrips.map(_.trip).distinct.size
            }).toList
          .sortBy(-_._2).take(50).foreach {case (fromCellToCell, numTrips) => {
              println(s"${fromCellToCell} numtrips: ${numTrips}")
          }}
    }

    def readTripData(filePath: String): TripData = {
        val reader = CSVReader.open(new java.io.File(filePath))
        val header:Option[List[String]] = reader.readNext()
        val formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
        val trips:Iterator[Trip] = for {
          //line <- reader.readNext()
          linefields <- reader.iterator if linefields.size > 0 && !linefields(0).startsWith("#")
        } yield {
          val linefieldsTrimed = linefields.map(_.trim)
          Trip(
            id = Util.getIdGenerator().generateNextId(),
            VendorID = 0, //linefieldsTrimed(0).toInt,
            lpep_pickup_datetime = formatter.parseDateTime(linefieldsTrimed(1)),
            lpep_dropoff_datetime = formatter.parseDateTime(linefieldsTrimed(2)),
            store_and_fwd_flag = "",//linefieldsTrimed(3),
            RatecodeID = 0.0,//linefieldsTrimed(4).toDouble,
            PULocationID = linefieldsTrimed(5).toInt,
            DOLocationID = linefieldsTrimed(6).toInt,
            passenger_count = Try {linefieldsTrimed(7).toDouble} match {
              case Success(pc) => pc
              case Failure(e) => 1
            },
            trip_distance = linefieldsTrimed(8).toDouble,
            fare_amount = linefieldsTrimed(9).toDouble,
            extra = linefieldsTrimed(10).toDouble,
            mta_tax = linefieldsTrimed(11).toDouble,
            tip_amount = linefieldsTrimed(12).toDouble,
            tolls_amount = linefieldsTrimed(13).toDouble,
            improvement_surcharge = linefieldsTrimed(15).toDouble,
            total_amount = linefieldsTrimed(16).toDouble,
            payment_type = 0.0, //linefieldsTrimed(17).toDouble,
            trip_type = 0.0, //linefieldsTrimed(18).toDouble,
            congestion_surcharge = 0.0 //linefieldsTrimed(19).toDouble
          )
        }
      val res = TripData(trips.toList)
      reader.close()
      res
    }

    def readZoneMapFromCsv(zoneMapCsvFilePath: String): ZoneMap = {
      val reader = CSVReader.open(new java.io.File(zoneMapCsvFilePath))
      val header:Option[List[String]] = reader.readNext()
      val latLngPairRegex = "(\\-[0-9]{2}\\.[0-9]+)[ ]([0-9]{2}\\.[0-9]+)".r
      val zones:Iterator[Zone] = for {
        //line <- reader.readNext()
        linefields <- reader.iterator if linefields.size > 0 && !linefields(0).startsWith("#")
      } yield {
        val linefieldsTrimed = linefields.map(_.trim)
        val locationId = linefieldsTrimed(5).toInt
        val geom = linefieldsTrimed(2)
        val latlngs: List[LatLng] = latLngPairRegex.findAllIn(geom).toList.map(lat_lng => {
          val ll = lat_lng.split(" ").map(_.toDouble)
          LatLng(ll(0),ll(1))
        })
        Zone(id = locationId, latlngs = latlngs)
      }
      val res = ZoneMap(zones.toList)
      reader.close()
      res
    }
}
