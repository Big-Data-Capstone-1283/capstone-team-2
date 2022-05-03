package detector

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object PaymentTypePattern {

	/**
	  * Tests for a pattern in the purchase frequency by payment type.
	  *
	  * @param data	Dataframe to search for a pattern on.
	  * @return		Search result as `Option[String]`.  (`None` = no pattern)
	  */
	def Go(data: DataFrame): Option[String] = {
		var newDf = data  // Generate the data
			.select("payment_type", "qty")
			.groupBy("payment_type")
			.agg(count("payment_type").as("count"), sum("qty").as("total"))
			.orderBy("count")
		var newDfSucc = data  // Generate the "total_successful" data
			.select(col("payment_type").as("temp_payment_type"), col("qty"))
			.where("payment_txn_success = 'Y'")
			.groupBy("temp_payment_type")
			.agg(sum("qty").as("total_successful"))
			.drop("qty")
		newDf = newDf
			.join(newDfSucc, newDf("payment_type") === newDfSucc("temp_payment_type"), "full")
			.drop("temp_payment_type")
		if (PatternDetector.testMode)  // If we're in test mode...
			newDf.show(false)  // ...show the data
		val ndev = PatternDetector.deviation1F(newDf)  // Check the data for a pattern
		var filename = ""
		if (ndev > 1.0 + PatternDetector.marginOfError) {  // Pattern detected
			val filename = PatternDetector.saveDataFrameAsCSV(newDf, "PaymentTypeRates.csv")  // Write the data to a file
			if (ndev < 2)
				Option("Found possible pattern (" + ((ndev - 1) * 100) + "% chance)\nFilename: " + filename)
			else
				Option("Found pattern (100% chance)\nFilename: " + filename)
		} else {  // No pattern detected
			if (PatternDetector.forceCSV) {
				filename = PatternDetector.saveDataFrameAsCSV(newDf, "PaymentTypeRates.csv")  // Write the data to a file
				if (PatternDetector.testMode)  // If we're in test mode...
					println(s"Data force-saved as: $filename\n")  // ...show the filename
			}
			None
		}
	}
}
