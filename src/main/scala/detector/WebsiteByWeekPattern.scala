package detector

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object WebsiteByWeekPattern {

	/**
	  * Tests for a pattern in the purchase frequency by website and week (not including first and last weeks).
	  * (Assumes time zone data is already normalized to UTC.)
	  *
	  * @param data	Dataframe to search for a pattern on.
	  * @return		Search result as `Option[String]`.  (`None` = no pattern)
	  */
	def Go(data: DataFrame): Option[String] = {
		var newDf = data  // Generate the "count" and "total" data
			.select("datetime", "ecommerce_website_name", "qty")
			.withColumn("week", window(col("datetime"), "7 days").cast(StringType))
			.groupBy("ecommerce_website_name", "week")
			.agg(count("week").as("count"), sum("qty").as("total"))
			.orderBy("week")
		val firstRow = newDf.head(1)(0).getString(1)  // Get the first week's name
		val lastRow = newDf.tail(1)(0).getString(1)  // Get the last week's name
		newDf = newDf.where("week != '" + firstRow + "' AND week != '" + lastRow + "'")  // Remove the first and last rows since they're likely not complete weeks
		var newDfSucc = data  // Generate the "total_successful" data
			.select("datetime", "qty", "ecommerce_website_name")
			.where("payment_txn_success = 'Y'")
			.withColumn("temp_week", window(col("datetime"), "7 days").cast(StringType))
			.withColumnRenamed("ecommerce_website_name", "temp_website")
			.groupBy("temp_website", "temp_week")
			.agg(sum("qty").as("total_successful"))
		newDfSucc = newDfSucc.where("temp_week != '" + firstRow + "' AND temp_week != '" + lastRow + "'")  // Remove the first and last rows since they're likely not complete weeks
		newDf = newDf  // Merge the two dataframes
			.join(newDfSucc,newDf("week") === newDfSucc("temp_week") && newDf("ecommerce_website_name") === newDfSucc("temp_website"), "full")
			.drop("temp_week", "temp_website")
			.orderBy("ecommerce_website_name", "week")
		if (PatternDetector.testMode)  // If we're in test mode...
			newDf.show(false)  // ...show the data
		val ndev = PatternDetector.deviation2F(newDf)  // Check the data for a pattern
		var filename = ""
		if (ndev > 1.0 + PatternDetector.marginOfError) {  // Pattern detected
			filename = PatternDetector.saveDataFrameAsCSV(newDf, "WebsiteByWeek.csv")  // Write the data to a file
			if (ndev < 2)
				Option("Found possible pattern (" + ((ndev - 1) * 100) + "% chance)\nFilename: " + filename)
			else
				Option("Found pattern (100% chance)\nFilename: " + filename)
		} else {  // No pattern detected
			if (PatternDetector.forceCSV) {
				filename = PatternDetector.saveDataFrameAsCSV(newDf, "WebsiteByWeek.csv")  // Write the data to a file
				if (PatternDetector.testMode)  // If we're in test mode...
					println(s"Data force-saved as: $filename\n")  // ...show the filename
			}
			None
		}
	}
}
