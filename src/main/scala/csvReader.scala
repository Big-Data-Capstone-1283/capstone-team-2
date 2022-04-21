import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object csvReader {

  def main(args: Array[String]): Unit ={
    
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExamples.com")
      .config("spark.master", "local")
      .config("spark.logConf", "true")
      .enableHiveSupport()
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("INFO")
    spark.sparkContext.setLogLevel("ERROR")


    val dfPurchases: DataFrame = spark.read.option("header",true)
      .csv("src/main/data/purchase_examples.csv")

    dfPurchases.show()


}
}


