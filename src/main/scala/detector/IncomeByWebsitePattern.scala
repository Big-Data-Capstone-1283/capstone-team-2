package detector

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object IncomeByWebsitePattern {

	/**
	  * Tests for a pattern in the income (price * qty) by ecommerce website name.
	  *
	  * @param data	Dataframe to search for a pattern on.
	  * @return		Search result as `Option[String]`.  (`None` = no pattern)
	  */
	def Go(data: DataFrame): Option[String] = {
		var newDf = data  // Generate the "income" data
			.select("ecommerce_website_name", "price", "qty")
			.where("payment_txn_success = 'Y'")
			.groupBy("ecommerce_website_name")
			.agg(count("ecommerce_website_name").as("count"), sum(col("qty") * col("price")).as("total_income"))
			.withColumn("average_income_per_order", col("total_income") / col("count"))
			.orderBy("ecommerce_website_name")
		if (PatternDetector.testMode)  // If we're in test mode...
			newDf.show()  // ...show the data
		val ndev = PatternDetector.deviation1F(newDf)  // Check the data for a pattern
		if (ndev > 1.0 + PatternDetector.marginOfError) {  // Pattern detected
			val filename = PatternDetector.saveDataFrameAsCSV(newDf, "WebsiteTotalValue.csv")  // Write the data to a file
			if (ndev < 2)
				Option("Found possible pattern (" + ((ndev - 1) * 100) + "% chance)\nFilename: " + filename)
			else
				Option("Found pattern (100% chance)\nFilename: " + filename)
		} else  // No pattern detected
			None
	}
}
