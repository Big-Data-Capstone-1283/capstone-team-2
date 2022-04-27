package com.revature.main

import com.revature.main.KafkaProcesser.{saveDataFrameAsCSV, spark}
import com.revature.main.kafkaToSpark.FORCE_TIMER_PRINT
import com.revature.main.mySparkUtils._
import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, expr, split}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.io.File
import scala.util.Try

object KafkaProcesser {
	var spark:SparkSession = null

	def date_format(value: Any, str: String) = ???

	/**
	  * Main program section.  Sets up Spark session, runs queries, and then closes the session.
	  *
	  * @param args	Executable's paramters (ignored).
	  */
	def main (args: Array[String]): Unit = {
		// Start the Spark session
		System.setProperty("hadoop.home.dir", "C:\\hadoop")
		Logger.getLogger("org").setLevel(Level.ERROR)  // Hide most of the initial non-error log messages
		spark = SparkSession.builder
			.appName("Proj2")
			.config("spark.master", "local[*]")
			.enableHiveSupport()
			.getOrCreate()
		spark.sparkContext.setLogLevel("ERROR")  // Hide further non-error messages
		spark.sql("SET hive.exec.dynamic.partition.mode=nonstrict")
		println("Created Spark session.\n")
		// Create the database if needed
		spark.sql("CREATE DATABASE IF NOT EXISTS proj2")
		spark.sql("USE proj2")
		// Run the "getUniqueCountries" query

		kafkaToSpark.kafkaProcessor()
		//bq1.kafkaReader()

    //dq2.countryCasesVsDeath()

		// End Spark session
		spark.stop()
		println("Transactions complete.")
	}

	/**
		* Gets a list of filenames in the given directory, filtered by optional matching file extensions.
		*
		* @param dir			Directory to search.
		* @param extensions	Optional list of file extensions to find.
		* @return				List of filenames with paths.
		*/
	def getListOfFiles(dir: File, extensions: List[String]): List[File] = {
		dir.listFiles.filter(_.isFile).toList.filter { file => extensions.exists(file.getName.endsWith(_)) }
	}

	/**
		* Moves/renames a file.
		*
		* @param oldName	Old filename and path.
		* @param newName	New filename.
		* @return			Success or failure.
		*/
	def mv(oldName: String, newName: String) = {
		Try(new File(oldName).renameTo(new File(newName))).getOrElse(false)
	}

	def saveDataFrameAsCSV(df: DataFrame, filename: String): String = {
		df.coalesce(1).write.options(Map("header"->"true", "delimiter"->",")).mode(SaveMode.Overwrite).format("csv").save("tempCSVDir")
		val curDir = System.getProperty("user.dir")
		val srcDir = new File(curDir + "/tempCSVDir")
		val files = getListOfFiles(srcDir, List("csv"))
		var srcFilename = files(0).toString()
		val destFilename = curDir + "/" + filename
		FileUtils.deleteQuietly(new File(destFilename))  // Clear out potential old copies
		mv(srcFilename, destFilename)  // Move and rename file
		FileUtils.deleteQuietly(srcDir)  // Delete temp directory
		destFilename
	}
}

object kafkaToSpark {
	//Constant to switch csv write on/off for debug purposes
	val WRITE_TO_CSV_ON:Boolean=true
	//Constant to force Timers to print for debug purposes
	val FORCE_TIMER_PRINT:Boolean=false

	var tempsHistAvg:DataFrame=null
	var covidAvgDaily:DataFrame=null
	var populationNormalization:DataFrame=null
	var joined:DataFrame=null
	var preProcessed:DataFrame=null
	var processedData:DataFrame=null


	/**
		* extracts columns from Kafka output
		*/
	def kafkaProcessor(): Unit = {
		// Read "covid_19_data.csv" data as a dataframe
		println("Dataframe read from CSV:")
		startTimer()
		var df = spark.read.format("csv").option("header", "false").option("inferSchema", "true")
			.load("kafka.csv")
		df=df.select(df.columns(1))
		df=df.withColumn("csv", expr("substring(_c1, 2, length(_c1)-2)"))
		df=df.select(df.columns(1))

		val df2 = df.select(
			split(col("csv"),",").getItem(0).as("order_id"),
			split(col("csv"),",").getItem(1).as("customer_id"),
			split(col("csv"),",").getItem(2).as("customer_name"),
			split(col("csv"),",").getItem(3).as("product_id"),
			split(col("csv"),",").getItem(4).as("product_name"),
			split(col("csv"),",").getItem(5).as("product_category"),
			split(col("csv"),",").getItem(6).as("payment_type"),
			split(col("csv"),",").getItem(7).as("qty"),
			split(col("csv"),",").getItem(8).as("price"),
			split(col("csv"),",").getItem(9).as("datetime"),
			split(col("csv"),",").getItem(10).as("country"),
			split(col("csv"),",").getItem(11).as("city"),
			split(col("csv"),",").getItem(12).as("ecommerce_website_namne"),
			split(col("csv"),",").getItem(13).as("payment_txn_id"),
			split(col("csv"),",").getItem(14).as("payment_txn_success"),
			split(col("csv"),",").getItem(14).as("failure_reason")
		)
			.drop("csv")

		df2.printSchema()
		df2.printLength
		stopTimer()
		df2.show(5,false)

		//Write the data out as a file to be used for visualization
		processedData=df2
		csvWriterHelper(processedData, "KafkaProcessed.csv")
	}

	/**
		* Allows write to csv to be one line in other methods, reducing overall code. Method called ios still the csv writer from the Project2 driver
		*/
	def csvWriterHelper(df: DataFrame, filename:String):Unit={
		if (WRITE_TO_CSV_ON) {
			// Write the data out as a file to be used for visualization
			startTimer(s"Save $filename as file")
			saveDataFrameAsCSV(df, filename)
			println(s"Saved as: $filename")
			stopTimer("Save $filename")
		}
	}

}

object mySparkUtils {
	var startTime:Double=0.0
	var transTime:Double=0.0

	//implicit class adding the method "Dataframe.printLength"
	implicit class DataframeImplicit(df: org.apache.spark.sql.DataFrame) {
		def printLength ={println(s"Table length: ${df.count()}")}
	}

	def startTimer(a:String="TimedAction", print:Boolean=true): Unit = {
		val action=a.capitalize
		startTime = System.currentTimeMillis()
		if (print || FORCE_TIMER_PRINT){
			println(s"$action timer started at $startTime")
		}
	}

	def stopTimer(a:String="TimedAction", print:Boolean=true): Unit ={
		val action=a.capitalize
		transTime = (System.currentTimeMillis() - startTime) / 1000d
		if (print || FORCE_TIMER_PRINT){
			println(s"$action completed in $transTime seconds")
		}
	}
}
