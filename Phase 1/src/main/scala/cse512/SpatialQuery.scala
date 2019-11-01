package cse512

import org.apache.spark.sql.SparkSession
import scala.math.sqrt
import scala.math.pow

object SpatialQuery extends App{

    /**
     * Function to check if the given point (x, y) is in the given rectangle
     * @params:
     * @return:
     */
    def checkPointInRectangle(x1: Float, y1: Float, x2: Float, y2: Float, x: Float, y: Float): Boolean = {
        if (x >= x1 && x <= x2 && y >= y1 && y <= y2)
            return true
        else if (x >= x2 && x <= x1 && y >= y2 && y <= y1)
            return true
        else
            return false
    }

    def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

        val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
        pointDf.createOrReplaceTempView("point")

        // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
        spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=> {
            val rectangleArr = queryRectangle.split(",")
            var x1 = rectangleArr(0).toFloat
            var y1 = rectangleArr(1).toFloat
            var x2 = rectangleArr(2).toFloat
            var y2 = rectangleArr(3).toFloat

            //var pointStr = "-88.304259,32.488903"
            val pointArr = pointString.split(",")
            var x = pointArr(0).toFloat
            var y = pointArr(1).toFloat

            checkPointInRectangle(x1, y1, x2, y2, x, y)
        })

        val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
        resultDf.show()

        return resultDf.count()
    }

    def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

        val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
        pointDf.createOrReplaceTempView("point")

        val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
        rectangleDf.createOrReplaceTempView("rectangle")

        // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
        spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>{
            val rectangleArr = queryRectangle.split(",")
            var x1 = rectangleArr(0).toFloat
            var y1 = rectangleArr(1).toFloat
            var x2 = rectangleArr(2).toFloat
            var y2 = rectangleArr(3).toFloat

            //var pointStr = "-88.304259,32.488903"
            val pointArr = pointString.split(",")
            var x = pointArr(0).toFloat
            var y = pointArr(1).toFloat

            checkPointInRectangle(x1, y1, x2, y2, x, y)
        })

        val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
        resultDf.show()

        return resultDf.count()
    }

    def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

        val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
        pointDf.createOrReplaceTempView("point")

        // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
        spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>{
            val point1Arr = pointString1.split(",")
            var x1 = point1Arr(0).toFloat
            var y1 = point1Arr(1).toFloat

            val point2Arr = pointString2.split(",")
            var x2 = point2Arr(0).toFloat
            var y2 = point2Arr(1).toFloat

            // Calculate the Euclidean distance between two points
            var calDistance = sqrt(pow(x1 - x2, 2) + pow(y1 - y2, 2))
            if (calDistance <= distance.toFloat)
                true
            else
                false
        })


        val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
        resultDf.show()

        return resultDf.count()
    }

    def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

        val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
        pointDf.createOrReplaceTempView("point1")

        val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
        pointDf2.createOrReplaceTempView("point2")

        // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
        spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>{
            val point1Arr = pointString1.split(",")
            var x1 = point1Arr(0).toDouble
            var y1 = point1Arr(1).toDouble

            val point2Arr = pointString2.split(",")
            var x2 = point2Arr(0).toDouble
            var y2 = point2Arr(1).toDouble

            // Calculate the Euclidean distance between two points
            var calDistance = sqrt(pow(x1 - x2, 2) + pow(y1 - y2, 2))
            if (calDistance <= distance)
                true
            else
                false
        })
        val resultDf = spark.sql("select count(*) from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
        resultDf.show()

        return resultDf.count()
    }
}
