package detector

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.joda.time.DateTime

object IncomeByDayOfWeekPattern {

	/**
	  * Tests for a pattern in the average income by day of week.  (Assumes pre-normalized time zone data.)
	  *
	  * @param data	Dataframe to search for a pattern on.
	  * @return		Search result as `Option[String]`.  (`None` = no pattern)
	  */
	def Go(data: DataFrame): Option[String] = {
		var newDf = data  // Generate the "average_count" and "average_total" data
			.select("datetime", "price", "qty")
			.where("payment_txn_success = 'Y'")
			.withColumn("day_of_week", date_format(col("datetime"), "EEEE"))  // Create a column with the day of week name for each date
			.groupBy("day_of_week")
			.agg(count("day_of_week").as("count"), sum(col("price") * col("qty")).as("total_income"))
			.withColumn("average_count",  // Normalize the data by finding the average for each day of the week
					when(col("day_of_week").isInCollection(PatternDetector.minDaysSeq),  // Find average for minimum counted days
						round(col("count") / PatternDetector.minCount).cast(LongType))  // *Note: The average is rounded and then cast to LongType
					.otherwise(  // Find average for maximum counted days
						round(col("count") / PatternDetector.maxCount).cast(LongType)))  // *Note: The average is rounded and then cast to LongType
			.withColumn("average_total_income",  // Normalize the data by finding the average for each day of the week
					when(col("day_of_week").isInCollection(PatternDetector.minDaysSeq),  // Find average for minimum total days
						round(col("total_income") / PatternDetector.minCount))  // *Note: The average is rounded and then cast to LongType
					.otherwise(  // Find average for maximum counted days
						round(col("total_income") / PatternDetector.maxCount)))  // *Note: The average is rounded and then cast to LongType
			.drop("count", "total_income")  // Drop the un-normalized data
			.withColumn("daynum_of_week", coalesce(PatternDetector.daymapCol(col("day_of_week")), lit("")))  // Create a column to order by
			.orderBy(col("daynum_of_week"))
		if (PatternDetector.testMode)  // If we're in test mode...
			newDf.show(false)  // ...show the data
		val ndev = PatternDetector.getDeviationDouble(newDf, 2, Seq(0))  // Check the data for a pattern
		var filename = ""
		if (ndev > 1.0 + PatternDetector.marginOfError) {  // Pattern detected
			filename = PatternDetector.saveDataFrameAsCSV(newDf, "IncomeByDayOfWeek.csv")  // Write the data to a file
			if (ndev < 2)
				Option("Found possible pattern (" + ((ndev - 1) * 100) + "% chance)\nFilename: " + filename)
			else
				Option("Found pattern (100% chance)\nFilename: " + filename)
		} else {  // No pattern detected
			if (PatternDetector.forceCSV) {
				filename = PatternDetector.saveDataFrameAsCSV(newDf, "IncomeByDayOfWeek.csv")  // Write the data to a file
				if (PatternDetector.testMode)  // If we're in test mode...
					println(s"Data force-saved as: $filename\n")  // ...show the filename
			}
			None
		}
	}
}
