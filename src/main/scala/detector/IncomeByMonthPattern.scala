package detector

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.joda.time.DateTime

object IncomeByMonthPattern {

	/**
	  * Tests for a pattern in the average income by month (average is per day rounded to nearest integer).
	  * (Assumes time zone data is already normalized to UTC.)
	  *
	  * @param data	Dataframe to search for a pattern on.
	  * @return		Search result as `Option[String]`.  (`None` = no pattern)
	  */
	def Go(data: DataFrame): Option[String] = {
		var newDf = data  // Generate the "average_count_per_day" and "average_income_per_day" data
			.select("datetime", "price", "qty")
			.where("payment_txn_success = 'Y'")
			.withColumn("year_and_month", date_format(col("datetime"), "yyyy-MM"))  // Create a column with the year and month for each date
			.groupBy("year_and_month")
			.agg(round(count("year_and_month") / PatternDetector.daysPerMonthCol(col("year_and_month"))).cast(LongType).as("average_count_per_day"),  // Averages counts per day by length of month
				 round(sum(col("qty") * col("price")) / PatternDetector.daysPerMonthCol(col("year_and_month"))).as("average_income_per_day"))  // Averages income per day by length of month
			.orderBy("year_and_month")
		if (PatternDetector.testMode)  // If we're in test mode...
			newDf.show(false)  // ...show the data
		val ndev = PatternDetector.getDeviationDouble(newDf, 2, Seq(0))  // Check the data for a pattern
		var filename = ""
		if (ndev > 1.0 + PatternDetector.marginOfError) {  // Pattern detected
			filename = PatternDetector.saveDataFrameAsCSV(newDf, "AvgIncomePerDayByMonth.csv")  // Write the data to a file
			if (ndev < 2)
				Option("Found possible pattern (" + ((ndev - 1) * 100) + "% chance)\nFilename: " + filename)
			else
				Option("Found pattern (100% chance)\nFilename: " + filename)
		} else {  // No pattern detected
			if (PatternDetector.forceCSV) {
				filename = PatternDetector.saveDataFrameAsCSV(newDf, "AvgIncomePerDayByMonth.csv")  // Write the data to a file
				if (PatternDetector.testMode)  // If we're in test mode...
					println(s"Data force-saved as: $filename\n")  // ...show the filename
			}
			None
		}
	}
}
