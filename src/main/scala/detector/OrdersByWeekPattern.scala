package detector

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.joda.time.DateTime

object OrdersByWeekPattern {

	/**
	  * Tests for a pattern in the purchase frequency by week (not including first and last weeks).
	  * (Assumes time zone data is already normalized to UTC.)
	  *
	  * @param data	Dataframe to search for a pattern on.
	  * @return		Search result as `Option[String]`.  (`None` = no pattern)
	  */
	def Go(data: DataFrame): Option[String] = {
		var newDf = data  // Generate the data
			.select("datetime")
			.withColumn("week", window(col("datetime"), "7 days").cast(StringType))
			.groupBy("week")
			.agg(count("week").as("count"))
			.orderBy(col("week"))
		val firstRow = newDf.head(1)(0).getString(0)
		val lastRow = newDf.tail(1)(0).getString(0)
		newDf = newDf.where("week != '" + firstRow + "' AND week != '" + lastRow + "'")  // Remove the first and last rows since they're likely not complete weeks
		if (PatternDetector.testMode)  // If we're in test mode...
			newDf.show(false)  // ...show the data
		val ndev = PatternDetector.deviation1F(newDf)  // Check the data for a pattern
		if (ndev > 1.0 + PatternDetector.marginOfError) {  // Pattern detected
			val filename = PatternDetector.saveDataFrameAsCSV(newDf, "OrdersByWeek.csv")  // Write the data to a file
			if (ndev < 2)
				Option("Found possible pattern (" + ((ndev - 1) * 100) + "% chance)\nFilename: " + filename)
			else
				Option("Found pattern (100% chance)\nFilename: " + filename)
		} else  // No pattern detedted
			None
	}
}
