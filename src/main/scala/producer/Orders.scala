package producer

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

object Orders {

  Logger.getLogger("org").setLevel(Level.ERROR)
  System.setProperty("hadoop.home.dir", "C:\\hadoop")
  val spark = SparkSession
    .builder
    .appName("hello hive")
    .config("spark.master", "local[*]")
    .enableHiveSupport()
    .getOrCreate()

  //println("created spark session")
  spark.sparkContext.setLogLevel("ERROR")

def createOrder(numOrders:Int):List[String]={
  var allOrders = new ListBuffer[String]
  var count = 1

  for(i <- 0 to numOrders)
  {
    val person = new personGen
    val products = new Products(person)
    allOrders += count + ","+ products.assignPersontoProduct
    count = count + 1
  }

  val orderoutput:List[String] =allOrders.toList

  return orderoutput
}



  def printOrderList(numOrder:Int): Unit =
  {
    //currently order is a bit bad, but that is based off the data. plans to go back in and add everything to a spark rdd then use the select
    // statments to properly format the data into strings then we can send to team 1's consumer
    var orders:List[String] =createOrder(numOrder)
    for(i <- 0 until orders.length -1 )
      {
        println(orders(i))
      }
  }



  override def  toString():String =
  {
    // formated string should be order_id,customer_id,customer_name,product_id,product_name,
    // product_category,pay_type,qty,price,+
    // datetime,country,city,ecommerce_website_name,payment_txn_id,payment_txn_success
    var output = new String








    // order

    return output
  }





}
