package consumer
import org.apache.spark._
import detector.PatternDetector
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}


object csvReader {

  def main(args: Array[String]): Unit = {

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

    //Create dataFrame based on sample purchases
    val dfPurchases: DataFrame = spark.read.option("header", false)
      .csv("src/main/data/Team1Data_with_errors.txt")
    //println(dfPurchases.count())

    val filter1 = dfPurchases.filter(dfPurchases("_c0").isNotNull &&
      dfPurchases("_c1").isNotNull && dfPurchases("_c2").isNotNull && dfPurchases("_c3").isNotNull &&
      dfPurchases("_c4").isNotNull && dfPurchases("_c5").isNotNull && dfPurchases("_c6").isNotNull &&
      dfPurchases("_c7").isNotNull && dfPurchases("_c8").isNotNull && dfPurchases("_c9").isNotNull &&
      dfPurchases("_c10").isNotNull && dfPurchases("_c11").isNotNull && dfPurchases("_c12").isNotNull &&
      dfPurchases("_c13").isNotNull && dfPurchases("_c14").isNotNull)
    //println(filter1.count())

    val filter2 = filter1.filter(!filter1("_c0").contains("|") && !filter1("_c1").contains("|") &&
      !filter1("_c2").contains("|") && !filter1("_c3").contains("|") && !filter1("_c4").contains("|")
      && !filter1("_c5").contains("|") && !filter1("_c6").contains("|") && !filter1("_c7").contains("|") &&
      !filter1("_c8").contains("|") &&  !filter1("_c9").contains("|") && !filter1("_c10").contains("|") &&
      !filter1("_c11").contains("|") && !filter1("_c12").contains("|") && !filter1("_c13").contains("|") &&
      !filter1("_c14").contains("|") && !filter1("_c15").contains("|"))
    //println(filter2.count())

    val filter3 = filter2.filter(!filter2("_c0").contains("{") && !filter2("_c1").contains("{") &&
      !filter2("_c2").contains("{") && !filter2("_c3").contains("{") && !filter2("_c4").contains("{")
      && !filter2("_c5").contains("{") && !filter2("_c6").contains("{") && !filter2("_c7").contains("{") &&
      !filter2("_c8").contains("{") &&  !filter2("_c9").contains("{") && !filter2("_c10").contains("{") &&
      !filter2("_c11").contains("{") && !filter2("_c12").contains("{") && !filter2("_c13").contains("{") &&
      !filter2("_c14").contains("{") && !filter2("_c15").contains("{"))
    //println(filter3.count())

    val filter4 = filter3.filter(!filter3("_c0").contains("}") && !filter3("_c1").contains("}") &&
      !filter3("_c2").contains("}") && !filter3("_c3").contains("}") && !filter3("_c4").contains("}")
      && !filter3("_c5").contains("}") && !filter3("_c6").contains("}") && !filter3("_c7").contains("}") &&
      !filter3("_c8").contains("}") &&  !filter3("_c9").contains("}") && !filter3("_c10").contains("}") &&
      !filter3("_c11").contains("}") && !filter3("_c12").contains("}") && !filter3("_c13").contains("}") &&
      !filter3("_c14").contains("}") && !filter3("_c15").contains("}"))
    //println(filter4.count())

    val filter5 = filter4.filter(!filter4("_c0").contains("[") && !filter4("_c1").contains("[") &&
      !filter4("_c2").contains("[") && !filter4("_c3").contains("[") && !filter4("_c4").contains("[")
      && !filter4("_c5").contains("[") && !filter4("_c6").contains("[") && !filter4("_c7").contains("[") &&
      !filter4("_c8").contains("[") &&  !filter4("_c9").contains("[") && !filter4("_c10").contains("[") &&
      !filter4("_c11").contains("[") && !filter4("_c12").contains("[") && !filter4("_c13").contains("[") &&
      !filter4("_c14").contains("[") && !filter4("_c15").contains("["))
    //println(filter5.count())

    val filter6 = filter5.filter(!filter5("_c0").contains("]") && !filter5("_c1").contains("]") &&
      !filter5("_c2").contains("]") && !filter5("_c3").contains("]") && !filter5("_c4").contains("]")
      && !filter5("_c5").contains("]") && !filter5("_c6").contains("]") && !filter5("_c7").contains("]") &&
      !filter5("_c8").contains("]") &&  !filter5("_c9").contains("]") && !filter5("_c10").contains("]") &&
      !filter5("_c11").contains("]") && !filter5("_c12").contains("]") && !filter5("_c13").contains("]") &&
      !filter5("_c14").contains("]") && !filter5("_c15").contains("]"))
    //println(filter6.count())

    val filter7 = filter6.filter(filter6("_c12") === "www.amazon.com.br")

    val convertedDF = filter7.select(
      filter7("_c0").cast(StringType).as("order_id"),
      filter7("_c1").cast(StringType).as("customer_id"),
      filter7("_c2").cast(StringType).as("customer_name"),
      filter7("_c3").cast(StringType).as("product_id"),
      filter7("_c4").cast(StringType).as("product_name"),
      filter7("_c5").cast(StringType).as("product_category"),
      filter7("_c6").cast(StringType).as("payment_type"),
      filter7("_c7").cast(IntegerType).as("qty"),
      filter7("_c8").cast(StringType).as("price"),
      filter7("_c9").cast(TimestampType).as("datetime"),
      filter7("_c10").cast(StringType).as("country"),
      filter7("_c11").cast(StringType).as("city"),
      filter7("_c12").cast(StringType).as("ecommerce_website_name"),
      filter7("_c13").cast(StringType).as("payment_txn_id"),
      filter7("_c14").cast(StringType).as("payment_txn_success"),
      filter7("_c15").cast(StringType).as("failure_reason"))
    //println(convertedDF.count())

    val cleanDF = convertedDF.filter(convertedDF("datetime") >= "2000-01-01")
    println(dfPurchases.count()-cleanDF.count() + " rows were removed")
    PatternDetector.Go(cleanDF)
  }
}
