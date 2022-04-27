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
		var newDf = data  // Generate the data
			.select("ecommerce_website_name", "country")
			.groupBy("ecommerce_website_name", "country")
			.agg(count("ecommerce_website_name").as("count"))
			.orderBy("ecommerce_website_name", "country")
		if (PatternDetector.testMode)  // If we're in test mode...
			newDf.show()  // ...show the data
		val ndev = PatternDetector.deviation2F(newDf)  // Check the data for a pattern
		if (ndev > 1.0 + PatternDetector.marginOfError) {  // Pattern detected
			val filename = PatternDetector.saveDataFrameAsCSV(newDf, "Website_CountryRate.csv")  // Write the data to a file
			if (ndev < 2)
				Option("Found possible pattern (" + ((ndev - 1) * 100) + "% chance)\nFilename: " + filename)
			else
				Option("Found pattern (100% chance)\nFilename: " + filename)
		} else  // No pattern detedted
			None
	}
}
