package detector

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object TotalItemsPerWebsite {

	/**
	  * Tests for a pattern in the total purchase amount (including quantity) by website.
	  *
	  * @param data	Dataframe to search for a pattern on.
	  * @return		Search result as `Option[String]`.  (`None` = no pattern)
	  */
	def Go(data: DataFrame): Option[String] = {
		var newDf = data  // Generate the data
			.select("ecommerce_website_name", "qty")
			.groupBy("ecommerce_website_name")
			.agg(sum("qty").as("total"))
			.orderBy(desc("total"))
		if (PatternDetector.testMode)  // If we're in test mode...
			newDf.show()  // ...show the data
		val ndev = PatternDetector.deviation1F(newDf)  // Check the data for a pattern
		if (ndev > 1.0 + PatternDetector.marginOfError) {  // Pattern detected
			val filename = PatternDetector.saveDataFrameAsCSV(newDf, "TotalOrdersPerWebsite.csv")  // Write the data to a file
			if (ndev < 2)
				Option("Found possible pattern (" + ((ndev - 1) * 100) + "% chance)\nFilename: " + filename)
			else
				Option("Found pattern (100% chance)\nFilename: " + filename)
		} else  // No pattern detedted
			None
	}
}