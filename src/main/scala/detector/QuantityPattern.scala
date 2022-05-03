package detector

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object QuantityPattern {

	/**
	  * Tests for a pattern in the purchase quantities.
	  *
	  * @param data	Dataframe to search for a pattern on.
	  * @return		Search result as `Option[String]`.  (`None` = no pattern)
	  */
	def Go(data: DataFrame): Option[String] = {
		var newDf = data  // Generate the "count" data
			.select("qty")
			.groupBy("qty")
			.agg(count("qty").as("count"))
			.select(col("qty").as("quantity").cast(StringType), col("count"))  // Convert the "qty" column to StringType for the deviation tester
		var newDfSucc = data  // Generate the "count_successful" data
			.select("qty")
			.where("payment_txn_success = 'Y'")
			.groupBy("qty")
			.agg(count("qty").as("count_successful"))
			.select(col("qty").as("temp_quantity").cast(StringType), col("count_successful"))  // Convert the "qty" column to StringType for the deviation tester
		newDf = newDf  // Merge the two dataframes
			.join(newDfSucc, newDf("quantity") === newDfSucc("temp_quantity"), "full")
			.drop("temp_quantity")
			.orderBy(col("quantity").cast(LongType))
		if (PatternDetector.testMode)  // If we're in test mode...
			newDf.show(false)  // ...show the data
		val ndev = PatternDetector.deviation1F(newDf)  // Check the data for a pattern
		var filename = ""
		if (ndev > 1.0 + PatternDetector.marginOfError) {  // Pattern detected
			filename = PatternDetector.saveDataFrameAsCSV(newDf, "QuantityRates.csv")  // Write the data to a file
			if (ndev < 2)
				Option("Found possible pattern (" + ((ndev - 1) * 100) + "% chance)\nFilename: " + filename)
			else
				Option("Found pattern (100% chance)\nFilename: " + filename)
		} else {  // No pattern detected
			if (PatternDetector.forceCSV) {
				filename = PatternDetector.saveDataFrameAsCSV(newDf, "QuantityRates.csv")  // Write the data to a file
				if (PatternDetector.testMode)  // If we're in test mode...
					println(s"Data force-saved as: $filename\n")  // ...show the filename
			}
			None
		}
	}
}
