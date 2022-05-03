package detector

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.joda.time.DateTime

object OrdersByHourOfDayPattern {

	/**
	  * Tests for a pattern in the average purchase frequency by time of day (average is per day rounded to nearest integer).
	  * Data columns saved as long values instead of integers.  (Assumes time zone data is already normalized to UTC.)
	  *
	  * @param data	Dataframe to search for a pattern on.
	  * @return		Search result as `Option[String]`.  (`None` = no pattern)
	  */
	def Go(data: DataFrame): Option[String] = {
		var newDf = data  // Generate the "average_count_per_hour" and "average_total_per_hour" data
			.select("datetime", "qty")
			.withColumn("hour_of_day", date_format(col("datetime"), "H"))  // Create a column with the hour of day (0 to 23) for each date
			.groupBy("hour_of_day")
			.agg((count("hour_of_day") / PatternDetector.numberOfDays).cast(DoubleType).as("average_count_per_hour"),
			     (sum("qty") / PatternDetector.numberOfDays).cast(DoubleType).as("average_total_per_hour"))
		var newDfSucc = data  // Generate the "average_total_successful_per_hour" data
			.select("datetime", "qty")
			.where("payment_txn_success = 'Y'")
			.withColumn("temp_hour_of_day", date_format(col("datetime"), "H"))  // Create a column with the hour of day (0 to 23) for each date
			.groupBy("temp_hour_of_day")
			.agg((sum("qty") / PatternDetector.numberOfDays).cast(DoubleType).as("average_total_successful_per_hour"))
		newDf = newDf  // Merge the two dataframes
			.join(newDfSucc,newDf("hour_of_day") === newDfSucc("temp_hour_of_day"), "full")
			.drop("temp_hour_of_day")
			.orderBy(col("hour_of_day").cast(LongType))
		if (PatternDetector.testMode)  // If we're in test mode...
			newDf.show(24, false)  // ...show the data
		val ndev = PatternDetector.getDeviationDouble(newDf, 1, Seq(0))  // Check the data for a pattern
		var filename = ""
		if (ndev > 1.0 + PatternDetector.marginOfError) {  // Pattern detected
			filename = PatternDetector.saveDataFrameAsCSV(newDf, "AverageOrdersByHourOfDay.csv")  // Write the data to a file
			if (ndev < 2)
				Option("Found possible pattern (" + ((ndev - 1) * 100) + "% chance)\nFilename: " + filename)
			else
				Option("Found pattern (100% chance)\nFilename: " + filename)
		} else {  // No pattern detected
			if (PatternDetector.forceCSV) {
				filename = PatternDetector.saveDataFrameAsCSV(newDf, "AverageOrdersByHourOfDay.csv")  // Write the data to a file
				if (PatternDetector.testMode)  // If we're in test mode...
					println(s"Data force-saved as: $filename\n")  // ...show the filename
			}
			None
		}
	}
}
