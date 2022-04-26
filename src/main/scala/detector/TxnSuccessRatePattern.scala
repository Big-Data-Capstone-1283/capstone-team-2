package detector

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object TxnSuccessRatePattern {
	def Go(data: DataFrame): Option[String] = {
		var newDf = data  // Generate the data
			.select("payment_txn_success")
			.groupBy("payment_txn_success")
			.agg(count("payment_txn_success"))
			.withColumnRenamed("count(payment_txn_success)", "count")
		if (PatternDetector.testMode)  // If we're in test mode...
			newDf.show()  // ...show the data
		val ndev = PatternDetector.deviation1F(newDf)  // Check the data for a pattern
		if (ndev > 1.0 + PatternDetector.marginOfError) {  // Pattern detected
			val filename = PatternDetector.saveDataFrameAsCSV(newDf, "TxnSuccessRate.csv")  // Write the data to a file
			if (ndev < 2)
				Option("Found possible pattern (" + ((ndev - 1) * 100) + "% chance)\nFilename: " + filename)
			else
				Option("Found pattern (100% chance)\nFilename: " + filename)
		} else  // No pattern detected
			None
	}
}
