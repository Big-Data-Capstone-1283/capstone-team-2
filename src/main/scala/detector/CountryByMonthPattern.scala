package detector

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.joda.time.DateTime

object CountryByMonthPattern {

	/**
	  * Tests for a pattern in the average purchase frequency for each country by month (average is per day rounded to nearest integer).
	  * (Assumes time zone data is already normalized to UTC.)
	  *
	  * @param data	Dataframe to search for a pattern on.
	  * @return		Search result as `Option[String]`.  (`None` = no pattern)
	  */
	def Go(data: DataFrame): Option[String] = {
		var newDf = data  // Generate the "average_count_per_day" and "average_total_per_day" data
			.select("datetime", "country", "qty")
			.withColumn("year_and_month", date_format(col("datetime"), "yyyy-MM"))  // Create a column with the year and month for each date
			.groupBy("country","year_and_month")
			.agg(round(count("year_and_month") / PatternDetector.daysPerMonthCol(col("year_and_month"))).cast(LongType).as("average_count_per_day"),  // Averages counts per day by length of month
				 round(sum("qty") / PatternDetector.daysPerMonthCol(col("year_and_month"))).cast(LongType).as("average_total_per_day"))  // Averages totals per day by length of month
		var newDfSucc = data  // Generate the "average_total_successful_per_day" data
			.select("datetime", "qty", "country")
			.where("payment_txn_success = 'Y'")
			.withColumn("temp_year_and_month", date_format(col("datetime"), "yyyy-MM"))  // Create a column with the year and month for each date
			.withColumnRenamed("country", "temp_country")
			.groupBy("temp_country", "temp_year_and_month")
			.agg(round(sum("qty") / PatternDetector.daysPerMonthCol(col("temp_year_and_month"))).cast(LongType).as("average_total_successful_per_day"))  // Averages totals per day by length of month
		newDf = newDf  // Merge the two dataframes
			.join(newDfSucc, newDf("year_and_month") === newDfSucc("temp_year_and_month")  && newDf("country") === newDfSucc("temp_country"), "full")
			.drop("temp_year_and_month", "temp_country")
			.orderBy("year_and_month", "country")
		if (PatternDetector.testMode)  // If we're in test mode...
			newDf.show(false)  // ...show the data
		val ndev = PatternDetector.deviation2F(newDf)  // Check the data for a pattern
		var filename = ""
		if (ndev > 1.0 + PatternDetector.marginOfError) {  // Pattern detected
			filename = PatternDetector.saveDataFrameAsCSV(newDf, "CountryByMonth.csv")  // Write the data to a file
			if (ndev < 2)
				Option("Found possible pattern (" + ((ndev - 1) * 100) + "% chance)\nFilename: " + filename)
			else
				Option("Found pattern (100% chance)\nFilename: " + filename)
		} else {  // No pattern detected
			if (PatternDetector.forceCSV) {
				filename = PatternDetector.saveDataFrameAsCSV(newDf, "CountryByMonth.csv")  // Write the data to a file
				if (PatternDetector.testMode)  // If we're in test mode...
					println(s"Data force-saved as: $filename\n")  // ...show the filename
			}
			None
		}
	}
}
