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

    def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
    {
        // Load the original data from a data source
        var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
        pickupInfo.createOrReplaceTempView("nyctaxitrips")
        //pickupInfo.show()

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
        //pickupInfo.show()

        // Define the min and max of x, y, z
        val minX = -74.50 / HotcellUtils.coordinateStep
        val maxX = -73.70 / HotcellUtils.coordinateStep
        val minY = 40.50 / HotcellUtils.coordinateStep
        val maxY = 40.90 / HotcellUtils.coordinateStep
        val minZ = 1
        val maxZ = 31
        val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

        pickupInfo = pickupInfo.select("x", "y", "z").where("x >= " + minX + " AND y >= " + minY + " AND z >= " + minZ + " AND x <= " + maxX + " AND y <= " + maxY + " AND z <= " + maxZ).orderBy("z", "y", "x")
        var hotCellDataFrame = pickupInfo.groupBy("z", "y", "x").count().withColumnRenamed("count", "hot_cell").orderBy("z", "y", "x")

        // Create a Temporary View for the Hotcell data
        hotCellDataFrame.createOrReplaceTempView("Hotcell")

        // Calculate average of hotcells
        val avg = (hotCellDataFrame.select("hot_cell").agg(sum("hot_cell")).first().getLong(0).toDouble) / numCells

        // Calculate Standard deviation
        val stdDev = scala.math.sqrt((hotCellDataFrame.withColumn("sqr_cell", pow(col("hot_cell"), 2)).select("sqr_cell").agg(sum("sqr_cell")).first().getDouble(0) / numCells) - scala.math.pow(avg, 2))

        // Get all the adjacent hotcells count by comparing the x,y,z coordinates
        var adHotCellNumber = spark.sql("SELECT h1.x AS x, h1.y AS y, h1.z AS z, "
        + "sum(h2.hot_cell) AS cellNumber "
        + "FROM Hotcell AS h1, Hotcell AS h2 "
        + "WHERE (h2.y = h1.y+1 OR h2.y = h1.y OR h2.y = h1.y-1) AND (h2.x = h1.x+1 OR h2.x = h1.x OR h2.x = h1.x-1) AND (h2.z = h1.z+1 OR h2.z = h1.z OR h2.z = h1.z-1)"
        + "GROUP BY h1.z, h1.y, h1.x "
        + "ORDER BY h1.z, h1.y, h1.x" )

        // User defined function to calculate cellNumber of adjacent cells
        var calculateNumberAdjFunc = udf((minX: Int, minY: Int, minZ: Int, maxX: Int, maxY: Int, maxZ: Int, X: Int, Y: Int, Z: Int) => HotcellUtils.computeAdjacentHotcell(minX, minY, minZ, maxX, maxY, maxZ, X, Y, Z))
        var adjacentHotcell = adHotCellNumber.withColumn("adjacentHotcell", calculateNumberAdjFunc(lit(minX), lit(minY), lit(minZ), lit(maxX), lit(maxY), lit(maxZ), col("x"), col("y"), col("z")))

        // User defined function to calculate G (Getis-Ord) based on the calculated information
        var gScoreFunc = udf((numCells: Int , x: Int, y: Int, z: Int, adjacentHotcell: Int, cellNumber: Int, avg: Double, stdDev: Double) => HotcellUtils.GScore(numCells, x, y, z, adjacentHotcell, cellNumber, avg, stdDev))
        var gScoreHotCell = adjacentHotcell.withColumn("gScore", gScoreFunc(lit(numCells), col("x"), col("y"), col("z"), col("adjacentHotcell"), col("cellNumber"), lit(avg), lit(stdDev))).orderBy(desc("gScore")).limit(50)
        //gScoreHotCell.show()

        pickupInfo = gScoreHotCell.select(col("x"), col("y"), col("z"))
        pickupInfo
    }
}