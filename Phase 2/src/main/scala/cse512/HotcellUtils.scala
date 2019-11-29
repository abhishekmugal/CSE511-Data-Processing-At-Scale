package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

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

    // YOU NEED TO CHANGE THIS PART
    /**
     * Function to calculate the adjacent hot cells for a given cell
     * @param minX - min value of X
     * @param minY - min value of Y
     * @param minZ - min value of Z
     * @param maxX - max value of X
     * @param maxY - max value of Y
     * @param maxZ - max value of Z
     * @param X - x coordinate of a given point
     * @param Y - y coordinate of a given point
     * @param Z - z coordinate of a given point
     * @return 18 - if point lies on x-boundary
     *         12 - if point lies on x and y boundary
     *         8 - if point lies on x, y, and z boundary
     *         27 - otherwise
     */
    def computeAdjacentHotcell( minX: Int, minY: Int, minZ: Int, maxX: Int, maxY: Int, maxZ: Int, X: Int, Y: Int, Z: Int): Int = {
        var count = 0

        // Cell is on X-boundary
        if (X == minX || X == maxX) {
            count += 1
        }
        // Cell is on X-boundary and Y-boundary
        if (Y == minY || Y == maxY) {
            count += 1
        }
        // Cell is on X-boundary, Y-boundary, and Z-boundary
        if (Z == minZ || Z == maxZ) {
            count += 1
        }

        count match {
            case 1 => 18
            case 2 => 12
            case 3 => 8
            case _ => 27
        }
    }

    /**
     * Function to calculate the Getis-ord (G) score for a given point
     * @param numCells - total number of cells
     * @param x - x coordinate
     * @param y - y coordinate
     * @param z - z coordinate
     * @param adjacentHotcell - adjacent hotcells calculated by the user-defined calculateAdjacentHotcell() function
     * @param number - Hotcell number for given x and y coordinate
     * @param avg - average of the hotcell for all the points
     * @param std - standard deviation
     * @return - G score
     */
    def GScore(numCells: Int, x: Int, y: Int, z: Int, adjacentHotcell: Int, cellNumber: Int , avg: Double, stdDev: Double): Double = {
        var adjHotCell: Double = adjacentHotcell.toDouble
        var numOfCells: Double = numCells.toDouble
        (cellNumber.toDouble - (avg * adjHotCell)) / (stdDev * math.sqrt((( adjHotCell * numOfCells) - (adjHotCell * adjHotCell)) / (numOfCells - 1.0)))
    }

}