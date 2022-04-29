package detector

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object ProdCat_PmtTypePattern {

	/**
	  * Tests for a pattern in the purchase frequency by payment type and product category.
	  *
	  * @param data	Dataframe to search for a pattern on.
	  * @return		Search result as `Option[String]`.  (`None` = no pattern)
	  */
	def Go(data: DataFrame): Option[String] = {
		var newDf = data  // Generate the "count" and "total" data
			.select("product_category", "payment_type", "qty")
			.groupBy("product_category", "payment_type")
			.agg(count("product_category").as("count"), sum("qty").as("total"))
		var newDfSucc = data  // Generate the "total_successful" data
			.select(col("product_category").as("temp_product_category"), col("payment_type").as("temp_payment_type"), col("qty"))
			.where("payment_txn_success = 'Y'")
			.groupBy("temp_product_category", "temp_payment_type")
			.agg(sum("qty").as("total_successful"))
		newDf = newDf  // Merge the two dataframes
			.join(newDfSucc, newDf("product_category") === newDfSucc("temp_product_category") && newDf("payment_type") === newDfSucc("temp_payment_type"), "full")
			.drop("temp_product_category", "temp_payment_type")
			.orderBy("product_category", "payment_type")
		if (PatternDetector.testMode)  // If we're in test mode...
			newDf.show(false)  // ...show the data
		val ndev = PatternDetector.deviation2F(newDf)  // Check the data for a pattern
		var filename = ""
		if (ndev > 1.0 + PatternDetector.marginOfError) {  // Pattern detected
			filename = PatternDetector.saveDataFrameAsCSV(newDf, "ProdCat_PmtType.csv")  // Write the data to a file
			if (ndev < 2)
				Option("Found possible pattern (" + ((ndev - 1) * 100) + "% chance)\nFilename: " + filename)
			else
				Option("Found pattern (100% chance)\nFilename: " + filename)
		} else {  // No pattern detected
			if (PatternDetector.forceCSV) {
				filename = PatternDetector.saveDataFrameAsCSV(newDf, "ProdCat_PmtType.csv")  // Write the data to a file
				if (PatternDetector.testMode)  // If we're in test mode...
					println(s"Data force-saved as: $filename\n")  // ...show the filename
			}
			None
		}
	}
}
