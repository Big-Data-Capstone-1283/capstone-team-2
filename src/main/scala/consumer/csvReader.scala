package consumer

import org.apache.log4j.{ Level, Logger }
import org.apache.spark._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import detector.PatternDetector

object csvReader {

  def main(args: Array[String]): Unit = {

	Logger.getLogger("org").setLevel(Level.ERROR)  // Hide most of the initial non-error log messages
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

    //Create dataFrame based on purchases
    val dfPurchases: DataFrame = spark.read.option("header", false)
      .csv("src/main/data/Team1Data_with_errors.txt")
    //println(dfPurchases.count())

    //Remove rows with null values except for failure reasons
    val filter1 = dfPurchases.filter(dfPurchases("_c0").isNotNull &&
      dfPurchases("_c1").isNotNull && dfPurchases("_c2").isNotNull && dfPurchases("_c3").isNotNull &&
      dfPurchases("_c4").isNotNull && dfPurchases("_c5").isNotNull && dfPurchases("_c6").isNotNull &&
      dfPurchases("_c7").isNotNull && dfPurchases("_c8").isNotNull && dfPurchases("_c9").isNotNull &&
      dfPurchases("_c10").isNotNull && dfPurchases("_c11").isNotNull && dfPurchases("_c12").isNotNull &&
      dfPurchases("_c13").isNotNull && dfPurchases("_c14").isNotNull)
    //println(filter1.count())

    //Remove rows with |
    val filter2 = filter1.filter(!filter1("_c0").contains("|") && !filter1("_c1").contains("|") &&
      !filter1("_c2").contains("|") && !filter1("_c3").contains("|") && !filter1("_c4").contains("|")
      && !filter1("_c5").contains("|") && !filter1("_c6").contains("|") && !filter1("_c7").contains("|") &&
      !filter1("_c8").contains("|") &&  !filter1("_c9").contains("|") && !filter1("_c10").contains("|") &&
      !filter1("_c11").contains("|") && !filter1("_c12").contains("|") && !filter1("_c13").contains("|") &&
      !filter1("_c14").contains("|") && !filter1("_c15").contains("|"))
    //println(filter2.count())

    //Remove rows with {
    val filter3 = filter2.filter(!filter2("_c0").contains("{") && !filter2("_c1").contains("{") &&
      !filter2("_c2").contains("{") && !filter2("_c3").contains("{") && !filter2("_c4").contains("{")
      && !filter2("_c5").contains("{") && !filter2("_c6").contains("{") && !filter2("_c7").contains("{") &&
      !filter2("_c8").contains("{") &&  !filter2("_c9").contains("{") && !filter2("_c10").contains("{") &&
      !filter2("_c11").contains("{") && !filter2("_c12").contains("{") && !filter2("_c13").contains("{") &&
      !filter2("_c14").contains("{") && !filter2("_c15").contains("{"))
    //println(filter3.count())

    //Remove rows with }
    val filter4 = filter3.filter(!filter3("_c0").contains("}") && !filter3("_c1").contains("}") &&
      !filter3("_c2").contains("}") && !filter3("_c3").contains("}") && !filter3("_c4").contains("}")
      && !filter3("_c5").contains("}") && !filter3("_c6").contains("}") && !filter3("_c7").contains("}") &&
      !filter3("_c8").contains("}") &&  !filter3("_c9").contains("}") && !filter3("_c10").contains("}") &&
      !filter3("_c11").contains("}") && !filter3("_c12").contains("}") && !filter3("_c13").contains("}") &&
      !filter3("_c14").contains("}") && !filter3("_c15").contains("}"))
    //println(filter4.count())

    //Remove rows with [
    val filter5 = filter4.filter(!filter4("_c0").contains("[") && !filter4("_c1").contains("[") &&
      !filter4("_c2").contains("[") && !filter4("_c3").contains("[") && !filter4("_c4").contains("[")
      && !filter4("_c5").contains("[") && !filter4("_c6").contains("[") && !filter4("_c7").contains("[") &&
      !filter4("_c8").contains("[") &&  !filter4("_c9").contains("[") && !filter4("_c10").contains("[") &&
      !filter4("_c11").contains("[") && !filter4("_c12").contains("[") && !filter4("_c13").contains("[") &&
      !filter4("_c14").contains("[") && !filter4("_c15").contains("["))
    //println(filter5.count())

    //Remove rows with ]
    val filter6 = filter5.filter(!filter5("_c0").contains("]") && !filter5("_c1").contains("]") &&
      !filter5("_c2").contains("]") && !filter5("_c3").contains("]") && !filter5("_c4").contains("]")
      && !filter5("_c5").contains("]") && !filter5("_c6").contains("]") && !filter5("_c7").contains("]") &&
      !filter5("_c8").contains("]") &&  !filter5("_c9").contains("]") && !filter5("_c10").contains("]") &&
      !filter5("_c11").contains("]") && !filter5("_c12").contains("]") && !filter5("_c13").contains("]") &&
      !filter5("_c14").contains("]") && !filter5("_c15").contains("]"))
    //println(filter6.count())

    //Remove rows where the website is wrong
    val filter7 = filter6.filter(filter6("_c12") === "www.amazon.com.br")

    //Remove rows with bad bank info
    val filter8 = filter7.filter(filter7("_c6") === "Bank" || filter7("_c6") === "Card" ||
      filter7("_c6") === "Paypal" || filter7("_c6") === "UPI")

    //Remove rows with <
    val filter9 = filter8.filter(!filter8("_c0").contains("<") && !filter8("_c1").contains("<") &&
      !filter8("_c2").contains("<") && !filter8("_c3").contains("<") && !filter8("_c4").contains("<")
      && !filter8("_c5").contains("<") && !filter8("_c6").contains("<") && !filter8("_c7").contains("<") &&
      !filter8("_c8").contains("<") &&  !filter8("_c9").contains("<") && !filter8("_c10").contains("<") &&
      !filter8("_c11").contains("<") && !filter8("_c12").contains("<") && !filter8("_c13").contains("<") &&
      !filter8("_c14").contains("<") && !filter8("_c15").contains("<"))

    //Remove rows with >
    val filter10 = filter9.filter(!filter9("_c0").contains(">") && !filter9("_c1").contains(">") &&
      !filter9("_c2").contains(">") && !filter9("_c3").contains(">") && !filter9("_c4").contains(">")
      && !filter9("_c5").contains(">") && !filter9("_c6").contains(">") && !filter9("_c7").contains(">") &&
      !filter9("_c8").contains(">") &&  !filter9("_c9").contains(">") && !filter9("_c10").contains(">") &&
      !filter9("_c11").contains(">") && !filter9("_c12").contains(">") && !filter9("_c13").contains(">") &&
      !filter9("_c14").contains(">") && !filter9("_c15").contains(">"))

    //Remove rows with bad product categories
    val filter11 = filter10.filter(filter10("_c5") === "App" || filter10("_c5") === "Drug" ||
      filter10("_c5") === "Groceries" || filter10("_c5") === "Car" || filter10("_c5") === "Plants" ||
      filter10("_c5") === "Movie" )

    //Remove rows with bad countries
    val filter12 = filter11.filter(filter11("_c10") === "United States" || filter11("_c10") === "Russia" ||
      filter11("_c10") === "Venezuela" || filter11("_c10") === "Colombia" || filter11("_c10") === "Argentina" ||
      filter11("_c10") === "India" || filter11("_c10") === "China" || filter11("_c10") === "South Africa" ||
      filter11("_c10") === "Pakistan" || filter11("_c10") === "Mexico" || filter11("_c10") === "South Korea"||
      filter11("_c10") === "United Kingdom" || filter11("_c10") === "Japan" || filter11("_c10") === "Israel" ||
      filter11("_c10") === "Greece" || filter11("_c10") === "Australia" || filter11("_c10") === "Italy" ||
      filter11("_c10") === "Spain" || filter11("_c10") === "Ireland" || filter11("_c10") === "Germany" ||
      filter11("_c10") === "Sweden"|| filter11("_c10") === "Egypt" || filter11("_c10") === "Iran" ||
      filter11("_c10") === "Poland" || filter11("_c10") === "Norway" || filter11("_c10") === "Turkey" ||
      filter11("_c10") === "Ukraine" || filter11("_c10") === "Canada" || filter11("_c10") === "Belgium" ||
      filter11("_c10") === "Netherlands" || filter11("_c10") === "Indonesia" || filter11("_c10") === "France"
      || filter11("_c10") === "Scotland" || filter11("_c10") === "Brazil" || filter11("_c10") === "Portugal")

    //Remove rows with negative numbers
    val filter13 = filter12.filter(!filter12("_c7").contains("-") && !filter12("_c8").contains("-") )

    //Remove rows with bad payment success
    val filter14 = filter13.filter(filter13("_c14") === "Y" || filter13("_c14") === "N")

    //Remove bad failure reasons
    val filter15 = filter14.filter(filter14("_c15").isNotNull || filter14("_c15") === "Fraud"
      || filter14("_c15") === "Server Maintenance"|| filter14("_c15") === "Connection Interrupted"
      || filter14("_c15") === "Invalid Routing Number" || filter14("_c15") === "Bank Account Suspended"
      || filter14("_c15") === "Card Information Incorrect"|| filter14("_c15") === "Card Expired"
      || filter14("_c15") === "Out of Funds"|| filter14("_c15") === "Incorrect Credentials"
      || filter14("_c15") === "Paypal Service Down")

    //Remake the dataframe, cast types and name columns
    val convertedDF = filter15.select(
      filter15("_c0").cast(StringType).as("order_id"),
      filter15("_c1").cast(StringType).as("customer_id"),
      filter15("_c2").cast(StringType).as("customer_name"),
      filter15("_c3").cast(StringType).as("product_id"),
      filter15("_c4").cast(StringType).as("product_name"),
      filter15("_c5").cast(StringType).as("product_category"),
      filter15("_c6").cast(StringType).as("payment_type"),
      filter15("_c7").cast(IntegerType).as("qty"),
      filter15("_c8").cast(StringType).as("price"),
      filter15("_c9").cast(TimestampType).as("datetime"),
      filter15("_c10").cast(StringType).as("country"),
      filter15("_c11").cast(StringType).as("city"),
      filter15("_c12").cast(StringType).as("ecommerce_website_name"),
      filter15("_c13").cast(StringType).as("payment_txn_id"),
      filter15("_c14").cast(StringType).as("payment_txn_success"),
      filter15("_c15").cast(StringType).as("failure_reason"))
    //println(convertedDF.count())

    //Remove bad quantities
    val qtyDF = convertedDF.filter(convertedDF("qty") <= 100 && convertedDF("qty") > 0)

    //Remove bad dates
    val cleanDF = qtyDF.filter(qtyDF("datetime") >= "2000-01-01")

    //Tell us how many rows were bad
    println(dfPurchases.count()-cleanDF.count() + " rows were removed")

    //Send to pattern detector
    PatternDetector.Go(cleanDF)
  }
}
