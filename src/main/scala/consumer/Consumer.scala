package consumer

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger.{ProcessingTime,Continuous}

object Consumer extends App {

  //Suppress logging
  Logger.getLogger("org").setLevel(Level.ERROR)
  //INITIATE SPARK SESSION//
  val spark = SparkSession                                    //Entry point to all functionality in Spark
    .builder                                                  //Method that creates a basic SparkSession
    .appName("Kafka Streaming")                        //Name of the application
    .config("spark.master", "local[4]")                       //'master' -> running on cluster, local[x] -> # of partitions and # of cores
    .config("spark.streaming.stopGracefullyOnShutdown","true")//Spark shuts down the StreamingContext gracefully on JVM shutdown rather than immediately
    .config("spark.sql.shuffle.partitions",3)                 //Number of partitions to use when shuffling data for joins or aggregations
    .enableHiveSupport()                                      //Enables Spark SQL to access metadata of Hive tables
    .getOrCreate()                                            //Returns a SparkSession object if already exists, create new one if not exists
  //suppress logging
  Logger.getLogger("org").setLevel(Level.ERROR)        //Suppress logging
  spark.sparkContext.setLogLevel("ERROR")                     //Suppress logging
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
    // ec2-3-93-174-172.compute-1.amazonaws.com:9092
    //OR
    // 3.93.174.172
    //Point to Amazon EC2 Zookeeper/Broker
    .option("kafka.bootstrap.servers", "ec2-3-93-174-172.compute-1.amazonaws.com:9092")
    //Subscribe to the other team's topic
    .option("subscribe", "team1") //The topic list to subscribe.
    //startingOffsets "earliest" returns no key value table at all
    //latest
    //earliest works for team2 data
    .option("startingOffsets", "earliest") //The start point when a query is started, "earliest" => earliest offsets, "latest" => latest offsets
    .option("failOnDataLoss", false)       //Whether to fail the query when it's possible that the data is loss
    .load()

  df.printSchema()     //Prints the schema to console


  //Since the value is in binary, first we need to convert the binary value to String using selectExpr()
  val topicStringDF = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

  topicStringDF.writeStream                    //Start the streaming computation
    .format("console")                 //Console sink
    .outputMode("append")          //Specify what gets written to the output sink, output only new rows
    //.option("checkpointLocation", "testCheckpoint/")
    //.trigger(ProcessingTime("2 seconds"))
    .trigger(Continuous("2 second"))  //Timing of streaming continuous data processing
    //.option("truncate", false)
    .start()
    //.awaitTermination()  //Uncomment if commented out csv sink and using console sink only

//UNCOMMENT TO SAVE TO CSV
    //Convert DF to csv
  topicStringDF.writeStream
    .format("csv")
    .trigger(ProcessingTime("2 seconds"))
    .option("checkpointLocation", "checkpoint/")
    .option("path", "output")
    .outputMode("append")
    .start()
    .awaitTermination() //waits for the termination signal from user

}
