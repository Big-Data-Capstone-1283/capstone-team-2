package consumer

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger.ProcessingTime

object Consumer extends App {

  //INITIATE SPARK SESSION//
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession
    .builder
    .appName("Kafka Streaming")
    .config("spark.master", "local")
    .enableHiveSupport()
    .getOrCreate()

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
    .option("kafka.bootstrap.servers", "ec2-3-93-174-172.compute-1.amazonaws.com:9092")
    //Subscribe to the other team topic
    .option("subscribe", "team1")
    //startingOffsets "earliest" returns no key value table at all
    .option("startingOffsets", "latest") // From starting
    .option("failOnDataLoss", false)
    .load()

  df.printSchema()


  //Since the value is in binary, first we need to convert the binary value to String using selectExpr()
  val topicStringDF = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

  topicStringDF.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", false)
    .start()


  topicStringDF.writeStream
    .format("csv")
    .trigger(ProcessingTime("2 seconds"))
    .option("checkpointLocation", "checkpoint/")
    .option("path", "output")
    .outputMode("append")
    .start()
    .awaitTermination() //waits for the termination signal from usera

}
