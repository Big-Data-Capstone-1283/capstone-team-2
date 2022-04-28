package detector

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object ProdCat_CountryPattern {

	/**
	  * Tests for a pattern in the purchase frequency by product category and country.
	  *
	  * @param data	Dataframe to search for a pattern on.
	  * @return		Search result as `Option[String]`.  (`None` = no pattern)
	  */
	def Go(data: DataFrame): Option[String] = {
		var newDf = data  // Generate the data
			.select("product_category", "country")
			.groupBy("product_category", "country")
			.agg(count("product_category"))
			.withColumnRenamed("count(product_category)", "count")
			.orderBy("product_category", "country")
		if (PatternDetector.testMode)  // If we're in test mode...
			newDf.show()  // ...show the data
		val ndev = PatternDetector.deviation2F(newDf)  // Check the data for a pattern
		if (ndev > 1.0 + PatternDetector.marginOfError) {  // Pattern detected
			val filename = PatternDetector.saveDataFrameAsCSV(newDf, "ProdCat_CountryRates.csv")  // Write the data to a file
			if (ndev < 2)
				Option("Found possible pattern (" + ((ndev - 1) * 100) + "% chance)\nFilename: " + filename)
			else
				Option("Found pattern (100% chance)\nFilename: " + filename)
		} else  // No pattern detedted
			None
	}
}
