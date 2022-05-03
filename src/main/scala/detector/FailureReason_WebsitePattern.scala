package detector

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object FailureReason_WebsitePattern {

	/**
	  * Tests for a pattern in the failure reason by website.
	  *
	  * @param data	Dataframe to search for a pattern on.
	  * @return		Search result as `Option[String]`.  (`None` = no pattern)
	  */
	def Go(data: DataFrame): Option[String] = {
		var newDf = data  // Generate the "count" data, other totals aren't necessary as we're only checking failures
			.select("failure_reason", "ecommerce_website_name")
			.where("payment_txn_success = 'N'")
			.groupBy("ecommerce_website_name", "failure_reason")
			.agg(count("failure_reason").as("count"))
			.orderBy(desc("count"), asc("ecommerce_website_name"), asc("failure_reason"))
		if (PatternDetector.testMode)  // If we're in test mode...
			newDf.show(false)  // ...show the data
		val ndev = PatternDetector.deviation2F(newDf)  // Check the data for a pattern
		var filename = ""
		if (ndev > 1.0 + PatternDetector.marginOfError) {  // Pattern detected
			filename = PatternDetector.saveDataFrameAsCSV(newDf, "FailureReasonByWebsite.csv")  // Write the data to a file
			if (ndev < 2)
				Option("Found possible pattern (" + ((ndev - 1) * 100) + "% chance)\nFilename: " + filename)
			else
				Option("Found pattern (100% chance)\nFilename: " + filename)
		} else {  // No pattern detected
			if (PatternDetector.forceCSV) {
				filename = PatternDetector.saveDataFrameAsCSV(newDf, "FailureReasonByWebsite.csv")  // Write the data to a file
				if (PatternDetector.testMode)  // If we're in test mode...
					println(s"Data force-saved as: $filename\n")  // ...show the filename
			}
			None
		}
	}
}
