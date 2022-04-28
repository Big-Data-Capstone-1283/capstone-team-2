package detector

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.joda.time.DateTime

object OrdersByHourOfDayPattern {

	/**
	  * Tests for a pattern in the average purchase frequency by time of day (average is per day rounded to nearest integer).
	  * (Assumes time zone data is already normalized to UTC.)
	  *
	  * @param data	Dataframe to search for a pattern on.
	  * @return		Search result as `Option[String]`.  (`None` = no pattern)
	  */
	def Go(data: DataFrame): Option[String] = {
		var newDf = data  // Generate the data
			.select("datetime")
			.withColumn("hour_of_day", date_format(col("datetime"), "H"))  // Create a column with the hour of day (0 to 23) for each date
			.groupBy("hour_of_day")
			.agg(round(count("hour_of_day").as("count") / PatternDetector.numberOfDays).cast(LongType).as("average_orders_per_hour"))
			.orderBy(col("hour_of_day").cast(LongType))
		if (PatternDetector.testMode)  // If we're in test mode...
			newDf.show(24, false)  // ...show the data
		val ndev = PatternDetector.deviation1F(newDf)  // Check the data for a pattern
		if (ndev > 1.0 + PatternDetector.marginOfError) {  // Pattern detected
			val filename = PatternDetector.saveDataFrameAsCSV(newDf, "AverageOrdersByHourOfDay.csv")  // Write the data to a file
			if (ndev < 2)
				Option("Found possible pattern (" + ((ndev - 1) * 100) + "% chance)\nFilename: " + filename)
			else
				Option("Found pattern (100% chance)\nFilename: " + filename)
		} else  // No pattern detedted
			None
	}
}
