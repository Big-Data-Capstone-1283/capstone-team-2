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
import producer.WeightedRandomizer

object PatternTester {
	var spark:SparkSession = null

	def main (args: Array[String]): Unit = {

		// Spark setup
		Logger.getLogger("org").setLevel(Level.ERROR)  // Hide most of the initial non-error log messages
		spark = SparkSession  // Create the Spark session
			.builder()
			.appName("Proj3")
			.config("spark.master", "local[*]")
			.getOrCreate()
		spark.sparkContext.setLogLevel("ERROR")  // Hide further non-error messages

		// Data generation setup
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
		var rowData = Seq.empty[Any]
		var product_category = ""
		var payment_type = ""
		var country = ""
		var city = ""
		var website = ""
		var transaction_success = ""
		var failure_reason = ""
		var datetimestamp = new Timestamp(0)
		var datemap = Map[Int, Int]()
		var datearr = Array.ofDim[DateTime](90)
		var timemap = Map[Int, Int]()
		var month = 1
		var day = 0
		var hour = 0
		for (i <- 0 to 89) {  // Build up date data
			day += 1
			if (month == 1 && day == 32) {
				day = 1
				month = 2
			}
			if (month == 2 && day == 29) {
				day = 1
				month = 3
			}
			datearr(i) = (new DateTime).withYear(2022).withMonthOfYear(month).withDayOfMonth(day)
			if (i % 7 < 2)  // Dates with pattern on Sat/Sun
				datemap += (i -> 120)
				// datemap += Map(i -> 100)  // Dates with no pattern
			else
				datemap += (i -> 100)
		}
		for (i <- 0 to 23) {  // Build up time data
			if (i == 12 || i == 20 || i == 21)  // Times with pattern at 12 noon - 1pm and 8pm - 10pm (exclusive)
				timemap += (i -> 120)
				// timemap += Map(i -> 100)  // Times with no pattern
			else
				timemap += (i -> 100)
		}

		// Generate test data to verify that the pattern testers are working properly
		sampleData = Seq.empty[Row]
		println("Generating test data... ")
		for (i <- 1 to 10000) {  // Generate this many rows of data
			rowData = Seq.empty[Any]
			country = WeightedRandomizer(Map("US" -> 330, "Australia" -> 26, "UK" -> 67, "Canada" -> 38, "New Zealand" -> 5))  // "country" data with a pattern
			// country = WeightedRandomizer(Map("US" -> 15, "Australia" -> 10, "UK" -> 10, "Canada" -> 10, "New Zealand" -> 10))  // "country" data with slight pattern
			// country = WeightedRandomizer(Map("US" -> 1, "Australia" -> 1, "UK" -> 1, "Canada" -> 1, "New Zealand" -> 1))  // "country" data with no pattern
			city = WeightedRandomizer(Map("Alpha" -> 10, "Bravo" -> 9, "Charlie" -> 8, "Delta" -> 7, "Echo" -> 6))  // "city" data with a pattern
			// city = WeightedRandomizer(Map("Alpha" -> 1, "Bravo" -> 1, "Charlie" -> 1, "Delta" -> 1, "Echo" -> 1))  // "city" data with no pattern
			country match {  // "product_category" data
				case "US" => 			product_category = WeightedRandomizer(Map("electronics" -> 5, "tools" -> 1, "clothing" -> 1, "books" -> 1, "food" -> 1))
				case "Australia" =>		product_category = WeightedRandomizer(Map("electronics" -> 1, "tools" -> 1, "clothing" -> 1, "books" -> 1, "food" -> 1))
				case "UK" =>			product_category = WeightedRandomizer(Map("electronics" -> 1, "tools" -> 1, "clothing" -> 1, "books" -> 1, "food" -> 1))
				case "Canada" =>		product_category = WeightedRandomizer(Map("electronics" -> 1, "tools" -> 1, "clothing" -> 1, "books" -> 1, "food" -> 1))
				case "New Zealand" =>	product_category = WeightedRandomizer(Map("electronics" -> 1, "tools" -> 1, "clothing" -> 1, "books" -> 1, "food" -> 1))
			}
			// product_category = WeightedRandomizer(Map("Electronics" -> 60, "Tools" -> 20, "Clothing" -> 45, "Books" -> 30, "Food" -> 100)) // Add "product_category" data with no pattern
          	// product_category = WeightedRandomizer(Map("Electronics" -> 1, "Tools" -> 1, "Clothing" -> 1, "Books" -> 1, "Food" -> 1)) // Add "product_category" data with no pattern
			country match {  // "payment_type" data
				case "US" => 			payment_type = WeightedRandomizer(Map("Credit Card" -> 210, "Internet Banking" -> 40, "UPI" -> 30, "Wallet" -> 150))
				case "Australia" =>		payment_type = WeightedRandomizer(Map("Credit Card" -> 210, "Internet Banking" -> 40, "UPI" -> 30, "Wallet" -> 50))
				case "UK" =>			payment_type = WeightedRandomizer(Map("Credit Card" -> 210, "Internet Banking" -> 40, "UPI" -> 30, "Wallet" -> 150))
				case "Canada" =>		payment_type = WeightedRandomizer(Map("Credit Card" -> 210, "Internet Banking" -> 40, "UPI" -> 30, "Wallet" -> 150))
				case "New Zealand" =>	payment_type = WeightedRandomizer(Map("Credit Card" -> 210, "Internet Banking" -> 40, "UPI" -> 30, "Wallet" -> 150))
			}
			// payment_type = WeightedRandomizer(Map("Credit Card" -> 210, "Internet Banking" -> 40, "UPI" -> 30, "Wallet" -> 150))  // Add "payment_type" data with a pattern
			// payment_type = WeightedRandomizer(Map("Credit Card" -> 1, "Internet Banking" -> 1, "UPI" -> 1, "Wallet" -> 1))  // Add "payment_type" data with no pattern
			country match {  // "website" data
				case "US" => 			website = WeightedRandomizer(Map("Amazon" -> 100, "Etsy" -> 15, "eBay" -> 30, "Alibaba" -> 5, "Walmart" -> 60))
				case "Australia" =>		website = WeightedRandomizer(Map("Amazon" -> 100, "Etsy" -> 15, "eBay" -> 30, "Alibaba" -> 10, "Walmart" -> 60))
				case "UK" =>			website = WeightedRandomizer(Map("Amazon" -> 100, "Etsy" -> 15, "eBay" -> 30, "Alibaba" -> 10, "Walmart" -> 60))
				case "Canada" =>		website = WeightedRandomizer(Map("Amazon" -> 100, "Etsy" -> 15, "eBay" -> 30, "Alibaba" -> 10, "Walmart" -> 60))
				case "New Zealand" =>	website = WeightedRandomizer(Map("Amazon" -> 100, "Etsy" -> 15, "eBay" -> 30, "Alibaba" -> 20, "Walmart" -> 60))
			}
			// website = WeightedRandomizer(Map("Amazon" -> 100, "Etsy" -> 15, "eBay" -> 30, "Alibaba" -> 10, "Walmart" -> 60))  // Add "ecommerce_website_name" data with no pattern
			// website = WeightedRandomizer(Map("Amazon" -> 1, "Etsy" -> 1, "eBay" -> 1, "Alibaba" -> 1, "Walmart" -> 1))  // Add "ecommerce_website_name" data with no pattern
			transaction_success = WeightedRandomizer(Map("Y" -> 95, "N" -> 5))  // Add "payment_txn_success" data with pattern
			// transaction_success = WeightedRandomizer(Map("Y" -> 1, "N" -> 1))  // Add "payment_txn_success" data with no pattern
			if (transaction_success == "N")
				failure_reason = WeightedRandomizer(Map("Invalid transaction data" -> 100, "Connection dropped" -> 60, "Payment system failure" -> 30, "Unknown error" -> 20, "Explosion" -> 15))
			else
				failure_reason = ""
			day = WeightedRandomizer(datemap)
			hour = WeightedRandomizer(timemap)
			while (day == 71 && hour == 2)  // Fix for daylight savings time (skips from 1:59.59am to 3:00.00am on Mar. 13rd, 2022)
				hour = WeightedRandomizer(timemap)
			datetimestamp = new Timestamp(datearr(day).withHourOfDay(hour).getMillis())
			for (j <- 0 to 15) {
				if (j == 0)
					rowData = rowData :+ i.toString  // order_id
				else if (j == 5)
					rowData = rowData :+ product_category
				else if (j == 6)
					rowData = rowData :+ payment_type
				else if (j == 7)
					rowData = rowData :+ WeightedRandomizer(Map(1 -> 92, 2 -> 5, 3 -> 1, 4 -> 1, 5 -> 1))  // Add "qty" data with a pattern
					// rowData = rowData :+ 1  // Add "qty" data with no pattern
				else if (j == 8)
					rowData = rowData :+ "100"  // Add "price" data with no pattern
				else if (j == 9)
					rowData = rowData :+ datetimestamp
					/*
					rowData = rowData :+ new Timestamp((new DateTime)
										.withYear(2022)
										.withMonthOfYear(1)
										.withDayOfMonth(31)
										.withHourOfDay(13)
										.withMinuteOfHour(45)
										.withSecondOfMinute(0)
										.getMillis())  // Add "datetime" data with no pattern
					*/
				else if (j == 10)
					rowData = rowData :+ country
				else if (j == 11)
					rowData = rowData :+ city
				else if (j == 12)
					rowData = rowData :+ website
				else if (j == 14)
					rowData = rowData :+ transaction_success
				else if (j == 15)
					rowData = rowData :+ failure_reason
				else
					rowData = rowData :+ ""  // Add all remaining columns with no pattern
			}
			tempRow = Row.fromSeq(rowData)
			sampleData = sampleData :+ tempRow
		}
		println("Complete!\n")
		val df = spark.createDataFrame(spark.sparkContext.parallelize(sampleData, 1), tableStructure)  // Create a dataframe from the sample data
		df.show()  // Verify it was created properly
		df.printSchema()  // Verify it was created properly

		PatternDetector.Go(df)  // Run the pattern detector on the data
	}
}
