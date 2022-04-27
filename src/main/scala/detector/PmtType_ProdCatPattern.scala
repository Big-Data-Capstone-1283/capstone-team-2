package detector

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object PmtType_ProdCatPattern {

	/**
	  * Tests for a pattern in the purchase frequency by payment type and product category.
	  *
	  * @param data	Dataframe to search for a pattern on.
	  * @return		Search result as `Option[String]`.  (`None` = no pattern)
	  */
	def Go(data: DataFrame): Option[String] = {
		var newDf = data  // Generate the data
			.select("payment_type", "product_category")
			.groupBy("payment_type", "product_category")
			.agg(count("payment_type"))
			.withColumnRenamed("count(payment_type)", "count")
			.orderBy("payment_type", "product_category")
		if (PatternDetector.testMode)  // If we're in test mode...
			newDf.show()  // ...show the data
		val ndev = PatternDetector.deviation2F(newDf)  // Check the data for a pattern
		if (ndev > 1.0 + PatternDetector.marginOfError) {  // Pattern detected
			val filename = PatternDetector.saveDataFrameAsCSV(newDf, "PmtType_ProdCat.csv")  // Write the data to a file
			if (ndev < 2)
				Option("Found possible pattern (" + ((ndev - 1) * 100) + "% chance)\nFilename: " + filename)
			else
				Option("Found pattern (100% chance)\nFilename: " + filename)
		} else  // No pattern detedted
			None
	}
}
