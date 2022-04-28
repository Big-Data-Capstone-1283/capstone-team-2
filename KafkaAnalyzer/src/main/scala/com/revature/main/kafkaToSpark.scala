package com.revature.main

import com.revature.main.KafkaProcesser.{saveDataFrameAsCSV, spark}
import com.revature.main.kafkaToSpark.{FORCE_TIMER_PRINT, kafkaProcessor}
import com.revature.main.mySparkUtils._
import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, expr, split}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

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

  def testColumnCount():Unit={
  // Read "covid_19_data.csv" data as a dataframe
    println("Dataframe read from CSV:")
    startTimer()
    var df = spark.read.format("csv").option("header", "false").option("inferSchema", "true")
      .load("kafkaPreProcessed.csv")
    df.printSchema()
    df.printLength
    stopTimer()
    df.show(5, false)
  }

  def testDateTime():Unit={

    //row count before

    //filter date range between 2000 at earliest


    //row count after

    //rows removed
  }

  def getDistinctCountriesCities():Unit={

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
    csvWriterHelper(df, "KafkaPreprocessed.csv")

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
      split(col("csv"), ",").getItem(12).as("ecommerce_website_namne"),
      split(col("csv"), ",").getItem(13).as("payment_txn_id"),
      split(col("csv"), ",").getItem(14).as("payment_txn_success"),
      split(col("csv"), ",").getItem(15).as("failure_reason")
    )
      .drop("csv")

    df2.printSchema()
    df2.printLength
    stopTimer()
    df2.show(5, false)

    //Write the data out as a file to be used for visualization
    processedData = df2
    csvWriterHelper(processedData, "KafkaProcessed.csv")
  }


  /**
    * Allows write to csv to be one line in other methods, reducing overall code. Method called ios still the csv writer from the Project2 driver
    */
  def csvWriterHelper(df: DataFrame, filename: String): Unit = {
    if (WRITE_TO_CSV_ON) {
      // Write the data out as a file to be used for visualization
      startTimer(s"Save $filename as file")
      saveDataFrameAsCSV(df, filename)
      println(s"Saved as: $filename")
      stopTimer("Save $filename")
    }
  }

}
