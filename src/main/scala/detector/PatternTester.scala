package detector

import org.apache.log4j.{ Level, Logger }
import org.apache.spark.sql.{ SparkSession, SaveMode, Row, DataFrame }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._  // { StringType, StructField, StructType, DataFrame }
import org.apache.parquet.format.IntType
import org.joda.time.DateTime
import java.sql.Timestamp
import scala.collection.mutable.ArrayBuffer
//import scala.util.Try
import producer.WeightedRandomizer_Test.WeightedRandomizer

object PatternTester {
	var spark:SparkSession = null

	def main (args: Array[String]): Unit = {

		Logger.getLogger("org").setLevel(Level.ERROR)  // Hide most of the initial non-error log messages
		spark = SparkSession  // Create the Spark session
			.builder()
			.appName("Proj3")
			.config("spark.master", "local[*]")
			.getOrCreate()
		spark.sparkContext.setLogLevel("ERROR")  // Hide further non-error messages

		/*
		var colNames = Seq(col("order_id"), col("customer_id"), col("customer_name"), col("product_id"), col("product_name"),
						   col("product_category"), col("payment_type"), col("qty"), col("price"), col("datetime"), col("country"), col("city"),
						   col("ecommerce_website_name"), col("payment_txn_id"), col("payment_txn_success"), col("failure_reason"))
		*/
		val tableStructure = StructType(Array(  // Describe the data
			StructField("order_id", StringType, false),					//  0 = order_id
			StructField("customer_id", StringType, false),				//  1 = customer_id
			StructField("customer_name", StringType, false),			//  2 = customer_name
			StructField("product_id", StringType, false),				//  3 = product_id
			StructField("product_name", StringType, false),				//  4 = product_name
			StructField("product_category", StringType, false),			//  5 = product_category
			StructField("payment_type", StringType, false),				//  6 = payment_type
			StructField("qty", IntegerType, false),						//  7 = qty - IntegerType
			StructField("price", StringType, false),					//  8 = price
			StructField("datetime", TimestampType, false),				//  9 = datetime - TimestampType
			StructField("country", StringType, false),					// 10 = country
			StructField("city", StringType, false),						// 11 = city
			StructField("ecommerce_website_name", StringType, false),	// 12 = ecommerce_website_name
			StructField("payment_txn_id", StringType, false),			// 13 = payment_txn_id
			StructField("payment_txn_success", StringType, false),		// 14 = payment_txn_success
			StructField("failure_reason", StringType, false)			// 15 = failure_reason
		))
		var sampleData = Seq.empty[Row]  // Array of rows used for generating sample data
		var tempRow = Row.empty
		var rowDat = Seq.empty[Any]
		var df:DataFrame = null

		// Generate a pattern for country frequency
		sampleData = Seq.empty[Row]
		for (i <- 1 to 1000) {
			rowDat = Seq.empty[Any]
			for (j <- 0 to 15) {
				if (j == 0)
					rowDat = rowDat :+ i.toString  // order_id
				else if (j == 6)
					rowDat = rowDat :+ WeightedRandomizer(Map("Credit Card" -> 210, "Internet Banking" -> 40, "UPI" -> 30, "Wallet" -> 150))  // Add "payment_type" data with a pattern
					// rowDat = rowDat :+ WeightedRandomizer(Map("Credit Card" -> 1, "Internet Banking" -> 1, "UPI" -> 1, "Wallet" -> 1))  // Add "payment_type" data with no pattern
				else if (j == 7)
					rowDat = rowDat :+ 1  // Add "qty" data with no pattern
				else if (j == 9)
					rowDat = rowDat :+ new Timestamp((new DateTime)
										.withYear(2021)
										.withMonthOfYear(10)
										.withDayOfMonth(31)
										.withHourOfDay(13)
										.withMinuteOfHour(45)
										.withSecondOfMinute(0)
										.getMillis())  // Add "datetime" data with no pattern
				else if (j == 10)
					// rowDat = rowDat :+ WeightedRandomizer(Map("US" -> 330, "Australia" -> 26, "UK" -> 67, "Canada" -> 38, "New Zealand" -> 5))  // Add "country" data with a pattern
					rowDat = rowDat :+ WeightedRandomizer(Map("US" -> 15, "Australia" -> 10, "UK" -> 10, "Canada" -> 10, "New Zealand" -> 10))  // Add "country" data with slight pattern
					// rowDat = rowDat :+ WeightedRandomizer(Map("US" -> 1, "Australia" -> 1, "UK" -> 1, "Canada" -> 1, "New Zealand" -> 1))  // Add "country" data with no pattern
				else
					rowDat = rowDat :+ ""
			}
			tempRow = Row.fromSeq(rowDat)
			sampleData = sampleData :+ tempRow
		}
		df = spark.createDataFrame(spark.sparkContext.parallelize(sampleData, 1), tableStructure)  // Create a dataframe from the sample data
		df.show()  // Verify it was created properly
		df.printSchema()  // Verify it was created properly

		PatternDetector.Go(df)  // Run the pattern detector on the data
	}
}
