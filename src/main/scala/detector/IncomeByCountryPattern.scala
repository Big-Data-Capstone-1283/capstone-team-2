package detector

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object IncomeByCountryPattern {

	/**
	  * Tests for a pattern in the income (price * qty) by country.
	  *
	  * @param data	Dataframe to search for a pattern on.
	  * @return		Search result as `Option[String]`.  (`None` = no pattern)
	  */
	def Go(data: DataFrame): Option[String] = {
		var newDf = data  // Generate the "income" data
			.select("country", "price", "qty")
			.where("payment_txn_success = 'Y'")
			.groupBy("country")
			.agg(count("country").as("count"), sum(col("qty") * col("price")).as("total_income"))
			.withColumn("average_income_per_order", col("total_income") / col("count"))
			.orderBy("country")
		if (PatternDetector.testMode)  // If we're in test mode...
			newDf.show(false)  // ...show the data
		val ndev = PatternDetector.getDeviationDouble(newDf, 3, Seq(0))  // Check the "average_income" data for a pattern
		var filename = ""
		if (ndev > 1.0 + PatternDetector.marginOfError) {  // Pattern detected
			filename = PatternDetector.saveDataFrameAsCSV(newDf, "Income_Country.csv")  // Write the data to a file
			if (ndev < 2)
				Option("Found possible pattern (" + ((ndev - 1) * 100) + "% chance)\nFilename: " + filename)
			else
				Option("Found pattern (100% chance)\nFilename: " + filename)
		} else {  // No pattern detected
			if (PatternDetector.forceCSV) {
				filename = PatternDetector.saveDataFrameAsCSV(newDf, "Income_Country.csv")  // Write the data to a file
				if (PatternDetector.testMode)  // If we're in test mode...
					println(s"Data force-saved as: $filename\n")  // ...show the filename
			}
			None
		}
	}
}
