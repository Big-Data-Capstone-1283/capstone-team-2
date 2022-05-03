package detector

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object PmtType_CityPattern {

	/**
	  * Tests for a pattern in the purchase frequency by payment type and country.
	  *
	  * @param data	Dataframe to search for a pattern on.
	  * @return		Search result as `Option[String]`.  (`None` = no pattern)
	  */
	def Go(data: DataFrame): Option[String] = {
		var newDf = data  // Generate the "count" and "total" data
			.select("payment_type", "country", "city", "qty")
			.groupBy("payment_type", "country", "city")
			.agg(count("payment_type").as("count"), sum("qty").as("total"))
		var newDfSucc = data  // Generate the "total_successful" data
			.select(col("payment_type").as("temp_payment_type"), col("country").as("temp_country"), col("city").as("temp_city"), col("qty"))
			.where("payment_txn_success = 'Y'")
			.groupBy("temp_payment_type", "temp_country", "temp_city")
			.agg(sum("qty").as("total_successful"))
		newDf = newDf
			.join(newDfSucc, newDf("payment_type") === newDfSucc("temp_payment_type") && newDf("country") === newDfSucc("temp_country") && newDf("city") === newDfSucc("temp_city"), "full")
			.drop("temp_payment_type", "temp_country", "temp_city")
			.orderBy("payment_type", "country", "city")
		if (PatternDetector.testMode)  // If we're in test mode...
			newDf.show(false)  // ...show the data
		val ndev = PatternDetector.getDeviationLong(newDf, 3, Seq(0, 1, 2))  // Check the data for a pattern
		var filename = ""
		if (ndev > 1.0 + PatternDetector.marginOfError) {  // Pattern detected
			filename = PatternDetector.saveDataFrameAsCSV(newDf, "PmtType_CityRates.csv")  // Write the data to a file
			if (ndev < 2)
				Option("Found possible pattern (" + ((ndev - 1) * 100) + "% chance)\nFilename: " + filename)
			else
				Option("Found pattern (100% chance)\nFilename: " + filename)
		} else {  // No pattern detected
			if (PatternDetector.forceCSV) {
				filename = PatternDetector.saveDataFrameAsCSV(newDf, "PmtType_CityRates.csv")  // Write the data to a file
				if (PatternDetector.testMode)  // If we're in test mode...
					println(s"Data force-saved as: $filename\n")  // ...show the filename
			}
			None
		}
	}
}
