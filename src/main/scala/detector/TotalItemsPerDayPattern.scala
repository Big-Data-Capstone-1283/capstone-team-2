package detector

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.joda.time.DateTime

object TotalItemsPerDayPattern {

	/**
	  * Tests for a pattern in the total purchase amount (including quantity) by day of week.  (Assumes pre-normalized time zone data.)
	  *
	  * @param data	Dataframe to search for a pattern on.
	  * @return		Search result as `Option[String]`.  (`None` = no pattern)
	  */
	def Go(data: DataFrame): Option[String] = {
		var newDf = data  // Generate the data
			.select("datetime", "qty")
			.withColumn("day_of_week", date_format(col("datetime"), "EEEE"))  // Create a column with the day of week name for each date
			.groupBy("day_of_week")
			.agg(sum("qty").as("count"))
			.withColumn("average_total",  // Normalize the data by finding the average for each day of the week
					when(col("day_of_week").isInCollection(PatternDetector.minDaysSeq),  // Find average for minimum counted days
						round(col("count") / PatternDetector.minCount).cast(LongType))  // *Note: The average is rounded and then cast to LongType
					.otherwise(  // Find average for maximum counted days
						round(col("count") / PatternDetector.maxCount).cast(LongType)))  // *Note: The average is rounded and then cast to LongType
			.withColumn("daynum_of_week", coalesce(PatternDetector.daymapCol(col("day_of_week")), lit("")))  // Create a column to order by
			.drop("count")  // Drop the un-normalized data
			.orderBy(col("daynum_of_week"))
			//.drop("daynum_of_week")  // Remove ordering column since it was only needed for sorting the rows
		if (PatternDetector.testMode)  // If we're in test mode...
			newDf.show()  // ...show the data
		val ndev = PatternDetector.deviation1F(newDf)  // Check the data for a pattern
		if (ndev > 1.0 + PatternDetector.marginOfError) {  // Pattern detected
			val filename = PatternDetector.saveDataFrameAsCSV(newDf, "TotalOrdersPerDayOfWeek.csv")  // Write the data to a file
			if (ndev < 2)
				Option("Found possible pattern (" + ((ndev - 1) * 100) + "% chance)\nFilename: " + filename)
			else
				Option("Found pattern (100% chance)\nFilename: " + filename)
		} else  // No pattern detedted
			None
	}
}
