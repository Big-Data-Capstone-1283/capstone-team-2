package detector

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.joda.time.DateTime

object OrdersByMonthPattern {

	/**
	  * Tests for a pattern in the purchase frequency by month (average is per day rounded to nearest integer).  (Assumes pre-normalized time zone data.)
	  *
	  * @param data	Dataframe to search for a pattern on.
	  * @return		Search result as `Option[String]`.  (`None` = no pattern)
	  */
	def Go(data: DataFrame): Option[String] = {
		var newDf = data  // Generate the data
			.select("datetime")
			.withColumn("year_and_month", date_format(col("datetime"), "yyyy-MM"))  // Create a column with the year and month for each date
			.groupBy("year_and_month")
			.agg(round(count("year_and_month") / PatternDetector.daysPerMonthCol(col("year_and_month"))).cast(LongType).as("average_per_day"))  // Averages counts per day by length of month
			.orderBy(col("year_and_month"))
		if (PatternDetector.testMode)  // If we're in test mode...
			newDf.show()  // ...show the data
		val ndev = PatternDetector.deviation1F(newDf)  // Check the data for a pattern
		if (ndev > 1.0 + PatternDetector.marginOfError) {  // Pattern detected
			val filename = PatternDetector.saveDataFrameAsCSV(newDf, "DayOfWeekRates.csv")  // Write the data to a file
			if (ndev < 2)
				Option("Found possible pattern (" + ((ndev - 1) * 100) + "% chance)\nFilename: " + filename)
			else
				Option("Found pattern (100% chance)\nFilename: " + filename)
		} else  // No pattern detedted
			None
	}
}
