package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._


object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame ={
    println("Running runHotcellAnalysis")
    // Load the original data from a data source
    var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
    pickupInfo.createOrReplaceTempView("nyctaxitrips")
    // pickupInfo.show()

    // Assign cell coordinates based on pickup points
    spark.udf.register("CalculateX",(pickupPoint: String)=>((
      HotcellUtils.CalculateCoordinate(pickupPoint, 0)
      )))
    spark.udf.register("CalculateY",(pickupPoint: String)=>((
      HotcellUtils.CalculateCoordinate(pickupPoint, 1)
      )))
    spark.udf.register("CalculateZ",(pickupTime: String)=>((
      HotcellUtils.CalculateCoordinate(pickupTime, 2)
      )))


    pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
    var newCoordinateName = Seq("x", "y", "z")
    pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
    pickupInfo.show()

    // Define the min and max of x, y, z
    val minX = -74.50/HotcellUtils.coordinateStep
    val maxX = -73.70/HotcellUtils.coordinateStep
    val minY = 40.50/HotcellUtils.coordinateStep
    val maxY = 40.90/HotcellUtils.coordinateStep
    val minZ = 1
    val maxZ = 31
    val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)
    

    var filteredpickupInfo = pickupInfo.select("*").where("x <= " + maxX + " AND x>= " + minX + " AND y<= " + maxY + " AND y>= " + minY + " AND z<= " + maxZ + " AND z>= " + minZ).groupBy("x", "y" , "z").count()
    var withsqCount = filteredpickupInfo.withColumn("sqrCount", pow(col("count"), 2))
    filteredpickupInfo.show()

    var countSum = filteredpickupInfo.agg(sum("count")).first().getLong(0).toDouble
    var countSqrSum = withsqCount.agg(sum("sqrCount")).first().getDouble(0)
    // println( countSum, countSqrSum )

    val avgCount = countSum / numCells
    val stdDeviation = math.sqrt( (countSqrSum / numCells) - (avgCount * avgCount))
    println( avgCount, stdDeviation )

    
    var withhot = filteredpickupInfo.as("table1").crossJoin(filteredpickupInfo.as("table2")).filter(
      HotcellUtils.findNeighbor( col("table1.x"), col("table1.y"), col("table1.z"), col("table2.x"), col("table2.y"), col("table2.z") ))
      .select(col("table1.x"), col("table1.y"),col("table1.z"), col("table2.count")).groupBy("x", "y", "z")
      .agg(sum("count") as "hotness")
    withhot.show()


    var findneighbors = udf((minX: Double, maxX: Double, minY: Double, maxY: Double, minZ: Double, maxZ: Double, xval: Double, yval: Double, zval: Double) => HotcellUtils.getneighborsCount( minX, maxX, minY, maxY, minZ, maxZ, xval, yval, zval))
    var withAjc = withhot.withColumn("neighbors", findneighbors(lit(minX), lit(maxX), lit(minY), lit(maxY), lit(minZ), lit(maxZ), col("x"), col("y"), col("z")))
    withAjc.show()



    var findG = udf((avgCount: Double, stdDeviation:Double, countSum:Double, numCells:Double, hotness:Double, neighbors:Double) => HotcellUtils.getisOrdScore(avgCount, stdDeviation, countSum, numCells, hotness, neighbors))
    var withG = withAjc.withColumn("score", findG(lit(avgCount), lit(stdDeviation), lit(countSum), lit(numCells), col("hotness"), col("neighbors"))).orderBy(desc("score"))
    withG.show()
    
    pickupInfo = withG.select("x", "y", "z")
    pickupInfo.show()
    

    return pickupInfo.coalesce(1) // YOU NEED TO CHANGE THIS PART
  }
}