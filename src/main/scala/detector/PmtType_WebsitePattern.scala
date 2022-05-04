package detector

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object PmtType_WebsitePattern {

	/**
	  * Tests for a pattern in the purchase frequency by payment type and website.
	  *
	  * @param data	Dataframe to search for a pattern on.
	  * @return		Search result as `Option[String]`.  (`None` = no pattern)
	  */
	def Go(data: DataFrame): Option[String] = {
		var newDf = data  // Generate the "count" and "total" data
			.select("payment_type", "ecommerce_website_name", "qty")
			.groupBy("payment_type", "ecommerce_website_name")
			.agg(count("payment_type").as("count"), sum("qty").as("total"))
		var newDfSucc = data  // Generate the "total_successful" data
			.select(col("payment_type").as("temp_payment_type"), col("ecommerce_website_name").as("temp_website"), col("qty"))
			.where("payment_txn_success = 'Y'")
			.groupBy("temp_payment_type", "temp_website")
			.agg(sum("qty").as("total_successful"))
		newDf = newDf
			.join(newDfSucc, newDf("payment_type") === newDfSucc("temp_payment_type") && newDf("ecommerce_website_name") === newDfSucc("temp_website"), "full")
			.drop("temp_payment_type", "temp_website")
			.orderBy("payment_type", "ecommerce_website_name")
		if (PatternDetector.testMode)  // If we're in test mode...
			newDf.show(false)  // ...show the data
		val ndev = PatternDetector.deviation2F(newDf)  // Check the data for a pattern
		var filename = ""
		if (ndev > 1.0 + PatternDetector.marginOfError) {  // Pattern detected
			filename = PatternDetector.saveDataFrameAsCSV(newDf, "PmtType_WebsiteRates.csv")  // Write the data to a file
			if (ndev < 2)
				Option("Found possible pattern (" + ((ndev - 1) * 100) + "% chance)\nFilename: " + filename)
			else
				Option("Found pattern (100% chance)\nFilename: " + filename)
		} else {  // No pattern detected
			if (PatternDetector.forceCSV) {
				filename = PatternDetector.saveDataFrameAsCSV(newDf, "PmtType_WebsiteRates.csv")  // Write the data to a file
				if (PatternDetector.testMode)  // If we're in test mode...
					println(s"Data force-saved as: $filename\n")  // ...show the filename
			}
			None
		}
	}
}
