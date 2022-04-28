package consumer

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger.{ProcessingTime,Continuous}

object Consumer extends App {

  //Suppress logging
  Logger.getLogger("org").setLevel(Level.ERROR)
  //INITIATE SPARK SESSION//
  val spark = SparkSession
    .builder
    .appName("Kafka Streaming")
    .config("spark.master", "local[4]")
    .config("spark.streaming.stopGracefullyOnShutdown","true")
    .config("spark.sql.shuffle.partitions",3)
    .enableHiveSupport()
    .getOrCreate()
  //suppress logging
  Logger.getLogger("org").setLevel(Level.ERROR)
  spark.sparkContext.setLogLevel("ERROR")
  println("Created Spark Session")

  /** Spark Streaming is an extension of the core Spark API to process real-time data from sources like Kafka
   * -First add the depency:
   * // https://mvnrepository.com/artifact/org.apache.spark/spark-sql-kafka-0-10
   * libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.2.1" % Test
   * - Option startingOffsets earliest is used to read all data available in the Kafka at the start of the query, we might want to use latest to read only data that has not been processed.
   * - Info collected from:
   * => https://sparkbyexamples.com/spark/spark-streaming-with-kafka/
   * => https://spark.apache.org/docs/2.2.0/structured-streaming-kafka-integration.html
   * In case you se the error "Unable to find batch output/_spark_metadata/0" ... delete checkpoint folder and run the program again
   */

  val df = spark.readStream
    .format("kafka")
    //Provided the Kafka server, in our case it should be EC2
    //change bootstrap server to below to reach EC2
    // ec2-3-93-174-172.compute-1.amazonaws.com:9092
    // 3.93.174.172
    //172.25.145.45:9092
    //Point to Amazon EC2 Zookeeper/Broker
    .option("kafka.bootstrap.servers", "ec2-3-93-174-172.compute-1.amazonaws.com:9092")
    //Subscribe to the other team's topic
    .option("subscribe", "team1")
    //startingOffsets "earliest" returns no key value table at all
    //latest
    //earliest works for team2 data
    .option("startingOffsets", "earliest") // From starting
    .option("failOnDataLoss", false)
    .load()

  df.printSchema()


  //Since the value is in binary, first we need to convert the binary value to String using selectExpr()
  val topicStringDF = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
  val justValue = df.selectExpr("CAST(value AS STRING)")
  //need to strip "[]" square brackets
  //use stripPrefix/stripSuffix
  justValue.writeStream
    .format("console")
    .outputMode("append")
    //.option("checkpointLocation", "testCheckpoint/")
    //.trigger(ProcessingTime("2 seconds"))
    .trigger(Continuous("2 second"))
    //.option("truncate", false)
    .start()
    //.awaitTermination()


//UNCOMMENT TO SAVE TO CSV
    //Convert DF to csv
  topicStringDF.writeStream
    .format("csv")
    .trigger(ProcessingTime("2 seconds"))
    .option("checkpointLocation", "checkpoint/")
    .option("path", "output")
    .outputMode("append")
    .start()
    .awaitTermination() //waits for the termination signal from usera

}
