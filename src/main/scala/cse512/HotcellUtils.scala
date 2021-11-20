package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.expressions.UserDefinedFunction


object HotcellUtils {
  val coordinateStep = 0.01

  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int =
  {
    // Configuration variable:
    // Coordinate step is the size of each cell on x and y
    var result = 0
    coordinateOffset match
    {
      case 0 => result = Math.floor((inputString.split(",")(0).replace("(","").toDouble/coordinateStep)).toInt
      case 1 => result = Math.floor(inputString.split(",")(1).replace(")","").toDouble/coordinateStep).toInt
      // We only consider the data from 2009 to 2012 inclusively, 4 years in total. Week 0 Day 0 is 2009-01-01
      case 2 => {
        val timestamp = HotcellUtils.timestampParser(inputString)
        result = HotcellUtils.dayOfMonth(timestamp) // Assume every month has 31 days
      }
    }
    return result
  }

  def timestampParser (timestampString: String): Timestamp =
  {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val parsedDate = dateFormat.parse(timestampString)
    val timeStamp = new Timestamp(parsedDate.getTime)
    return timeStamp
  }

  def dayOfYear (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_YEAR)
  }

  def dayOfMonth (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_MONTH)
  }

  def getneighborsCount( minX: Double, minY: Double, minZ: Double, maxX: Double, maxY: Double, maxZ: Double, xval: Double, yval: Double, zval: Double): Double = 
  {
        var position = 0

        if (xval == minX || xval == maxX) {
            position += 1
        }
        if (yval == minY || yval == maxY) {
            position += 1
        }
        if (zval == minZ || zval == maxZ) {
            position += 1
        }

        position match {
            case 1 => 18
            case 2 => 12
            case 3 => 8
            case _ => 27
        }
    }

  
  
  val findNeighbor: UserDefinedFunction =
      udf[Boolean, Int, Int, Int, Int, Int, Int](isNeighbor)

  def isNeighbor(x1: Int, y1: Int, z1: Int, x2: Int, y2: Int, z2: Int): Boolean = 
  {
    if (math.abs(x1 - x2) > 1 || math.abs(y1 - y2) > 1 || math.abs(z1 - z2) > 1) false else true
  }


  def getisOrdScore (avgCount: Double, stdDeviation:Double, countSum:Double, numCells:Double, hotness:Double, neighbors:Double): Double =
  {
    var score = ((hotness) - (avgCount * neighbors)) / (stdDeviation * math.sqrt(((numCells * neighbors ) - math.pow(neighbors, 2)) / (numCells-1 )))
    return score
  }



  // YOU NEED TO CHANGE THIS PART
}