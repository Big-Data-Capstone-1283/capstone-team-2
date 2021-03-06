package detector

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.joda.time.DateTime

object IncomeByWeekPattern {

	/**
	  * Tests for a pattern in the income by week (not including first and last weeks).
	  * (Assumes time zone data is already normalized to UTC.)
	  *
	  * @param data	Dataframe to search for a pattern on.
	  * @return		Search result as `Option[String]`.  (`None` = no pattern)
	  */
	def Go(data: DataFrame): Option[String] = {
		var newDf = data  // Generate the "count" and "total" data
			.select("datetime", "price", "qty")
			.where("payment_txn_success = 'Y'")
			.withColumn("week", window(col("datetime"), "7 days").cast(StringType))
			.groupBy("week")
			.agg(count("week").as("count"), sum(col("qty") * col("price")).as("total_income"))
			.withColumn("average_income_per_order", col("total_income") / col("count"))
			.orderBy(col("week"))
		val firstRow = newDf.head(1)(0).getString(0)  // Get the first week's name
		val lastRow = newDf.tail(1)(0).getString(0)  // Get the last week's name
		newDf = newDf.where("week != '" + firstRow + "' AND week != '" + lastRow + "'")  // Remove the first and last rows since they're likely not complete weeks
		if (PatternDetector.testMode)  // If we're in test mode...
			newDf.show(false)  // ...show the data
		val ndev = PatternDetector.getDeviationDouble(newDf, 2, Seq(0))  // Check the data for a pattern
		var filename = ""
		if (ndev > 1.0 + PatternDetector.marginOfError) {  // Pattern detected
			filename = PatternDetector.saveDataFrameAsCSV(newDf, "IncomeByWeek.csv")  // Write the data to a file
			if (ndev < 2)
				Option("Found possible pattern (" + ((ndev - 1) * 100) + "% chance)\nFilename: " + filename)
			else
				Option("Found pattern (100% chance)\nFilename: " + filename)
		} else {  // No pattern detected
			if (PatternDetector.forceCSV) {
				filename = PatternDetector.saveDataFrameAsCSV(newDf, "IncomeByWeek.csv")  // Write the data to a file
				if (PatternDetector.testMode)  // If we're in test mode...
					println(s"Data force-saved as: $filename\n")  // ...show the filename
			}
			None
		}
	}
}
