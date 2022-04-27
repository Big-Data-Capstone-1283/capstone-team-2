package detector

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object TxnSuccess_PmtTypePattern {

	/**
	  * Tests for a pattern in the purchase frequency by transaction success and payment type.
	  *
	  * @param data	Dataframe to search for a pattern on.
	  * @return		Search result as `Option[String]`.  (`None` = no pattern)
	  */
	def Go(data: DataFrame): Option[String] = {
		var newDf = data  // Generate the data
			.select("payment_txn_success", "payment_type")
			.groupBy( "payment_type", "payment_txn_success")
			.agg(count("payment_txn_success").as("count"))
			.orderBy("count")
		if (PatternDetector.testMode)  // If we're in test mode...
			newDf.show()  // ...show the data
		val ndev = PatternDetector.deviation2F(newDf)  // Check the data for a pattern
		if (ndev > 1.0 + PatternDetector.marginOfError) {  // Pattern detected
			val filename = PatternDetector.saveDataFrameAsCSV(newDf, "TxnSuccess_PmtType.csv")  // Write the data to a file
			if (ndev < 2)
				Option("Found possible pattern (" + ((ndev - 1) * 100) + "% chance)\nFilename: " + filename)
			else
				Option("Found pattern (100% chance)\nFilename: " + filename)
		} else  // No pattern detedted
			None
	}
}
