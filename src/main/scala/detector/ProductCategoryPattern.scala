package detector

import org.apache.spark.sql.{ SparkSession, SaveMode, Row, DataFrame }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object ProductCategoryPattern {
	def Go(data: DataFrame): Option[String] = {
		var newDf = data  // Generate the data
			.select("product_category")
			.groupBy("product_category")
			.agg(count("product_category"))
			.withColumnRenamed("count(product_category)", "count")
		if (PatternDetector.testMode)  // If we're in test mode...
			newDf.show()  // ...show the data
		val ndev = PatternDetector.deviation1F(newDf)  // Check the data for a pattern
		if (ndev > 1.0 + PatternDetector.marginOfError) {  // Pattern detected
			val filename = PatternDetector.saveDataFrameAsCSV(newDf, "ProductCategoryRates.csv")  // Write the data to a file
			if (ndev < 2)
				Option("Found possible pattern (" + ((ndev - 1) * 100) + "% chance)\nFilename: " + filename)
			else
				Option("Found pattern (100% chance)\nFilename: " + filename)
		} else  // No pattern detedted
			None
	}
}
