package consumer

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object csvConverter {
  def main(args: Array[String]): Unit = {

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



    //Crate a dataframe from the multiple batches of information and rename column
    val df = spark.read
      //.options(Map("delimiter"->"["))
      //.csv("testTeam2Dummy")
      .csv("output")
      //.select("_c1")
      .withColumnRenamed("_c1","theirString")


    df.printSchema()     //Printing the schema (key,value)
    df.show( 100, false)
    //convert the df to an Array
    val rowArray = df.select("theirString").collect.map(_.toSeq)

    //COUNTER
    def commaCounter(rowString:String) = {
      val rowMap = scala.collection.mutable.HashMap.empty[String,Int]
      for (symbol <- rowString) {
        if (rowMap.contains(symbol.toString)) {
          rowMap(symbol.toString) = rowMap(symbol.toString) + 1
        } else {
          rowMap.+=((symbol.toString, 1))
        }
      }
      //map returns count of character
      if (rowMap.contains(",")) {
        rowMap(",")
      }
      else {
        0
      }
    }

    //for (i <- rowArray) {commaCounter(i)}
    var correctColNum:Array[String] = Array()
    for (i<-rowArray) {
      //Number of commas should be fifteen
      if (commaCounter(i.mkString(",")) == 15)
        correctColNum = correctColNum :+ i.mkString(",")
    }

    for (i <- correctColNum) {println(i)}

  }
}
