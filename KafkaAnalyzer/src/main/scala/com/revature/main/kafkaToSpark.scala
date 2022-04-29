package com.revature.main

import com.revature.main.KafkaProcesser.{saveDataFrameAsCSV, spark}
import com.revature.main.kafkaToSpark.{FORCE_TIMER_PRINT, kafkaProcessor}
import com.revature.main.mySparkUtils._
import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, expr, split, to_date, to_timestamp}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, TimestampType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.io.{File, PrintWriter}
import scala.Console.{RED, RESET, UNDERLINED, WHITE_B, YELLOW_B}
import scala.collection.mutable
import scala.io.Source

object kafkaToSpark {
  //Constant to switch csv write on/off for debug purposes
  val WRITE_TO_CSV_ON: Boolean = true
  //Constant to force Timers to print for debug purposes
  val FORCE_TIMER_PRINT: Boolean = false

  var tempsHistAvg: DataFrame = null
  var covidAvgDaily: DataFrame = null
  var populationNormalization: DataFrame = null
  var joined: DataFrame = null
  var preProcessed: DataFrame = null
  var processedData: DataFrame = null

  var startingTotalRows:Int=0
  var rowsBeforeTransaction:Int=0
  var rowsAfterTransaction:Int=0
  var totalRemovedCount: Int=0

  var removalMap:mutable.Map[String,Int]=scala.collection.mutable.Map[String, Int]()
  removalMap+="Total Removed Count"->0


  def testColumnCount():Unit={
  // Read "covid_19_data.csv" data as a dataframe
    val INPUT_FILENAME="KafkaPreProcessed_SingleColumn.csv"
    val filename = removeAllQuotesFromFile(INPUT_FILENAME)
    println("Dataframe read from CSV:")
    startTimer()
    var df = spark.read.format("csv").option("header", "false").option("inferSchema", "true")
      .load(filename)
    startingTotalRows=df.count().toInt
    rowsBeforeTransaction=df.count().toInt
    val rawDataColumnCount=df.columns.length
    println(s"There are ${formatText_HIGHLIGHT_UNDERLINE(rawDataColumnCount)} columns in the raw data")
    if (rawDataColumnCount!=16){
      if (rawDataColumnCount>16) println("The raw data contains rows with MORE columns than the defined schema")
      if (rawDataColumnCount<16) println("The raw data contains rows with LESS columns than the defined schema")
      var badFieldsCount=0
      for (i<-16 until rawDataColumnCount) {
        var colName="_c"+i.toString
        df=df.filter(df.col(colName).isNull)
      }
      for (i<-rawDataColumnCount until 16 by -1) {
        var colName="_c"+i.toString
        df=df.drop(colName)
      }
    } else {
      println("The raw data conforms to the schema in regards to column count")
    }
    rowsAfterTransaction=df.count().toInt
    rowRemovalTracker("Extra Columns")

    //REMOVE ROWS WITH INVALID CHARACTERS
    rowsBeforeTransaction=rowsAfterTransaction
    for (i<-0 to 15) {
      var colName = "_c" + i.toString
      if (i==4||i==9) {
        df = df.filter(!col(colName).rlike(raw"[\^<>;|\[\]{}?!#%^&*@+=_\/]"))
      } else {
        df = df.filter(!col(colName).rlike(raw"[\^<>;|\[\]{}?!#%^&*@+=_\/]"))
      }
    }
    rowsAfterTransaction=df.count().toInt
    rowRemovalTracker("Illegal Characters")


    //RENAME COLUMNS
    df = df.select(
      col("_c0").as("order_id"),
      col("_c1").as("customer_id"),
      col("_c2").as("customer_name"),
      col("_c3").as("product_id"),
      col("_c4").as("product_name"),
      col("_c5").as("product_category"),
      col("_c6").as("payment_type"),
      col("_c7").as("qty"),
      col("_c8").as("price"),
      col("_c9").as("datetime"),
      col("_c10").as("country"),
      col("_c11").as("city"),
      col("_c12").as("ecommerce_website_name"),
      col("_c13").as("payment_txn_id"),
      col("_c14").as("payment_txn_success"),
      col("_c15").as("failure_reason")
    )
    df.show()

    //REMOVE ROWS WHERE SITE NOT EQUAL WWW.AMAZON.BR
    rowsBeforeTransaction=rowsAfterTransaction

    df=df.filter(col("ecommerce_website_name").like("www.amazon.com.br"))

    rowsAfterTransaction=df.count().toInt
    rowRemovalTracker("Invalid Website")


    //REMOVE ROWS WITH INVALID DATE_TIME
    rowsBeforeTransaction=rowsAfterTransaction
    df=df.withColumn("datetime",to_timestamp(col("datetime")))
    df=df.filter(col("datetime").isNotNull)
    df.printSchema()
    rowsAfterTransaction=df.count().toInt
    rowRemovalTracker("Invalid DateTime")

    //REMOVE ROWS WITH INVALID TYPE
    rowsBeforeTransaction=rowsAfterTransaction
    df=df.withColumn("qty",col("qty").cast(IntegerType))
    df=df.filter(col("qty").isNotNull)

    rowsAfterTransaction=df.count().toInt
    rowRemovalTracker("Invalid Type")

    df=df.groupBy("payment_txn_id")


    //REMOVE ROWS WITH NULL VALUE EXCEPT failure_reason
//    rowsBeforeTransaction=rowsAfterTransaction
//
//    rowsAfterTransaction=df.count().toInt
//    rowRemovalTracker("     ")

    //REMOVE WHERE CUSTOMER NAME CONTAINS NUMBERS

    //REMOVE WHERE CUSTOMER NAME CONTAINS MORE THAN ONE SPACE

    //REMOVE WHERE payment_txn_success NOT EQUAL 'Y' or 'N'

    //REMOVE WHERE ORDER ID NOT UNIQUE

    //REMOVE LOWER COUNT ON CID WHERE (GROUPBY CUSTOMER ID & GROUPBY CUSTOMER NAME) RETURNS MORE THAN ONE ROW

    //REMOVE WHERE COUNTRY IS NOT A REAL COUNTRY

    //REMOVE WHERE CITY IS NOT A REAL CITY

    //REMOVE WHERE paymentReason NOT EQUAL TO
                      /**
                      {
                     "Connection Interrupted",
                     "PayPal Service Down",
                     "Server Maintenance",
                     "Card Information Incorrect",
                     "Fraud",
                     "Out of Funds",
                     "Invalid Routing Number",
                     "Bank Account Suspended",
                     "Incorrect Credentials",
                     "Card Expired"
                      }
                      */

    //OUTPUT FINDINGS AND SAVE FINALIZED DATAFRAME AS .CSV
    df.printSchema()
    println(df.count())
    println(s"Total bad rows removed: $totalRemovedCount")
    saveDataFrameAsCSV(df,"KafkaCleanedData.csv")

    val dfForPatternDetector=df
    .withColumn("order_id",col("order_id").cast(StringType))
    .withColumn("customer_id",col("customer_id").cast(StringType))
    .withColumn("customer_name",col("customer_name").cast(StringType))
    .withColumn("product_id",col("product_id").cast(StringType))
    .withColumn("product_name",col("product_name").cast(StringType))
    .withColumn("product_category",col("product_category").cast(StringType))
    .withColumn("payment_type",col("payment_type").cast(StringType))
    .withColumn("qty",col("qty").cast(IntegerType))
    .withColumn("price",col("price").cast(StringType))
    .withColumn("datetime",col("datetime").cast(TimestampType))
    .withColumn("country",col("country").cast(StringType))
    .withColumn("city",col("city").cast(StringType))
    .withColumn("ecommerce_website_name",col("ecommerce_website_name").cast(StringType))
    .withColumn("payment_txn_id",col("payment_txn_id").cast(StringType))
    .withColumn("payment_txn_success",col("payment_txn_success").cast(StringType))
    .withColumn("failure_reason",col("failure_reason").cast(StringType))

    csvWriterHelper(dfForPatternDetector, "csvForPatternTester.csv")
  }

  def rowRemovalTracker(rowRemovalReason:String):Unit={
    val removedCount = rowsBeforeTransaction - rowsAfterTransaction
    removalMap+= rowRemovalReason -> removedCount
    totalRemovedCount = startingTotalRows - rowsAfterTransaction
      removalMap("Total Removed Count")= totalRemovedCount
    println(removalMap)
  }

  private def formatText_HIGHLIGHT_UNDERLINE(input:Any):String={
    val input_Stringed=input.toString
    s"${RESET}${WHITE_B}${RED}${UNDERLINED}$input${RESET}"
  }

  private def removeAllQuotesFromFile(INPUT_FILENAME:String):String={
    //val f1 is original filename
    val inputFile= INPUT_FILENAME
    val outputFile = new File("KafkaPreProcessed_CommasRemoved.csv") // Temporary File
    val w = new PrintWriter(outputFile)
    Source.fromFile(inputFile).getLines
      .map { x => x.replaceAll("[\"]","") }
      .foreach(x => w.println(x))
    w.close()
    outputFile.getName
  }


  def testDateTime():Unit={
    //row count before

    //filter date range between 2000 at earliest

    //row count after

    //rows removed
  }

  def getDistinctCountriesCities():Unit={
    var df=processedData
    var distinctCountries=df.select("country").distinct().count()
    var distinctCities=df.select("city").distinct().count()

    println(s"Distinct Countries count = $distinctCountries")
    println(s"Distinct Cities count = $distinctCities")

  }

  def scrubInvalidCharacters():Unit={

  }

  /**
    * extracts columns from Kafka output
    */
  def kafkaProcessor(): Unit = {
    // Read "covid_19_data.csv" data as a dataframe
    println("Dataframe read from CSV:")
    startTimer()
    var df = spark.read.format("csv").option("header", "false").option("inferSchema", "true")
      .load("kafka.csv")
    df = df.select(df.columns(1))
    df = df.withColumn("csv", expr("substring(_c1, 2, length(_c1)-2)"))
    df = df.select(df.columns(1))
    saveDataFrameAsCSV(df,"KafkaPreProcessed_SingleColumn.csv",false)

    val df2 = df.select(
      split(col("csv"), ",").getItem(0).as("order_id"),
      split(col("csv"), ",").getItem(1).as("customer_id"),
      split(col("csv"), ",").getItem(2).as("customer_name"),
      split(col("csv"), ",").getItem(3).as("product_id"),
      split(col("csv"), ",").getItem(4).as("product_name"),
      split(col("csv"), ",").getItem(5).as("product_category"),
      split(col("csv"), ",").getItem(6).as("payment_type"),
      split(col("csv"), ",").getItem(7).as("qty"),
      split(col("csv"), ",").getItem(8).as("price"),
      split(col("csv"), ",").getItem(9).as("datetime"),
      split(col("csv"), ",").getItem(10).as("country"),
      split(col("csv"), ",").getItem(11).as("city"),
      split(col("csv"), ",").getItem(12).as("ecommerce_website_name"),
      split(col("csv"), ",").getItem(13).as("payment_txn_id"),
      split(col("csv"), ",").getItem(14).as("payment_txn_success"),
      split(col("csv"), ",").getItem(15).as("failure_reason")
    )
      .drop("csv")

//    df2.printSchema()
//    df2.printLength
    stopTimer()
//    df2.show(5, false)

    //Write the data out as a file to be used for visualization
    processedData = df2
    csvWriterHelper(processedData, "KafkaProcessed_CorrectSchema_Unclean.csv")
  }


  /**
    * Allows write to csv to be one line in other methods, reducing overall code. Method called ios still the csv writer from the Project2 driver
    */
  def csvWriterHelper(df: DataFrame, filename: String,additionalPRINTLN:String=""): Unit = {
    if (WRITE_TO_CSV_ON) {
      // Write the data out as a file to be used for visualization
      startTimer(s"Save $filename as file")
      saveDataFrameAsCSV(df, filename)
      if (!additionalPRINTLN.isEmpty) {
        println(additionalPRINTLN)
      }
      println(s"Saved as: $filename")
      stopTimer("Save $filename")
    }
  }

}
