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
    val dfPurchases: DataFrame = spark.read.option("header", true)
      .csv("src/main/data/purchase_examples.csv")

    val filteredPurchases = dfPurchases.filter(dfPurchases("order_id").isNotNull &&
      dfPurchases("customer_id").isNotNull && dfPurchases("customer_name").isNotNull && dfPurchases("product_id").isNotNull &&
      dfPurchases("product_name").isNotNull && dfPurchases("product_category").isNotNull && dfPurchases("payment_type").isNotNull &&
      dfPurchases("qty").isNotNull && dfPurchases("price").isNotNull && dfPurchases("datetime").isNotNull && dfPurchases("country").isNotNull &&
      dfPurchases("city").isNotNull && dfPurchases("ecommerce_website_name").isNotNull && dfPurchases("payment_txn_id").isNotNull && dfPurchases("payment_txn_success").isNotNull)

    filteredPurchases.show()


  }
}
