package detector

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Website_CountryPattern {

	/**
	  * Tests for a pattern in the purchase frequency by website and country.
	  *
	  * @param data	Dataframe to search for a pattern on.
	  * @return		Search result as `Option[String]`.  (`None` = no pattern)
	  */
	def Go(data: DataFrame): Option[String] = {
		var newDf = data  // Generate the "count" and "total" data
			.select("ecommerce_website_name", "country", "qty")
			.groupBy("ecommerce_website_name", "country")
			.agg(count("ecommerce_website_name").as("count"), sum("qty").as("total"))
		var newDfSucc = data  // Generate the "total_successful" data
			.select(col("ecommerce_website_name").as("temp_ecommerce_website_name"), col("country").as("temp_country"), col("qty"))
			.where("payment_txn_success = 'Y'")
			.groupBy("temp_ecommerce_website_name", "temp_country")
			.agg(sum("qty").as("total_successful"))
		newDf = newDf  // Merge the two dataframes
			.join(newDfSucc, newDf("ecommerce_website_name") === newDfSucc("temp_ecommerce_website_name") && newDf("country") === newDfSucc("temp_country"), "full")
			.drop("temp_ecommerce_website_name", "temp_country")
			.orderBy("ecommerce_website_name", "country")
		if (PatternDetector.testMode)  // If we're in test mode...
			newDf.show(false)  // ...show the data
		val ndev = PatternDetector.deviation2F(newDf)  // Check the data for a pattern
		var filename = ""
		if (ndev > 1.0 + PatternDetector.marginOfError) {  // Pattern detected
			filename = PatternDetector.saveDataFrameAsCSV(newDf, "Website_CountryRate.csv")  // Write the data to a file
			if (ndev < 2)
				Option("Found possible pattern (" + ((ndev - 1) * 100) + "% chance)\nFilename: " + filename)
			else
				Option("Found pattern (100% chance)\nFilename: " + filename)
		} else {  // No pattern detected
			if (PatternDetector.forceCSV) {
				filename = PatternDetector.saveDataFrameAsCSV(newDf, "Website_CountryRate.csv")  // Write the data to a file
				if (PatternDetector.testMode)  // If we're in test mode...
					println(s"Data force-saved as: $filename\n")  // ...show the filename
			}
			None
		}
	}
}
