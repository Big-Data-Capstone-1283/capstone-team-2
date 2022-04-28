package consumer

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
    //no header in other team data, set to False
    val dfPurchases: DataFrame = spark.read.option("header", false)
      //.csv("src/main/data/purchase_examples.csv")
      .csv("consumerOutput/transactions.csv")
    //no header means no column name to refer to

    val filteredPurchases = dfPurchases.filter(dfPurchases("_c0").isNotNull &&
      dfPurchases("_c1").isNotNull && dfPurchases("_c2").isNotNull && dfPurchases("_c3").isNotNull &&
      dfPurchases("_c4").isNotNull && dfPurchases("_c5").isNotNull && dfPurchases("_c6").isNotNull &&
      dfPurchases("_c7").isNotNull && dfPurchases("_c8").isNotNull && dfPurchases("_c9").isNotNull && dfPurchases("_c10").isNotNull &&
      dfPurchases("_c11").isNotNull && dfPurchases("_c12").isNotNull && dfPurchases("_c13").isNotNull && dfPurchases("_c14").isNotNull)


/*    val filteredPurchases = dfPurchases.filter(dfPurchases("order_id").isNotNull &&
      dfPurchases("customer_id").isNotNull && dfPurchases("customer_name").isNotNull && dfPurchases("product_id").isNotNull &&
      dfPurchases("product_name").isNotNull && dfPurchases("product_category").isNotNull && dfPurchases("payment_type").isNotNull &&
      dfPurchases("qty").isNotNull && dfPurchases("price").isNotNull && dfPurchases("datetime").isNotNull && dfPurchases("country").isNotNull &&
      dfPurchases("city").isNotNull && dfPurchases("ecommerce_website_name").isNotNull && dfPurchases("payment_txn_id").isNotNull && dfPurchases("payment_txn_success").isNotNull)*/

    filteredPurchases.show()


  }
}
