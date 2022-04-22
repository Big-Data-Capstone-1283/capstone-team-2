package producer

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import java.io.{File, FileWriter}
import scala.collection.mutable.ListBuffer
import scala.xml.dtd.Scanner

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


  var f = new File("input/Orders.txt")
  var custString = new String
  for(i <- 0 until allOrders.length)
  {
    custString=s"$custString \n"+allOrders(i)
  }

  val fw = new FileWriter(f)
  fw.write(custString)
  fw.close()

  spark.sql("DROP TABLE IF EXISTS orders")
  spark.sql("CREATE TABLE orders(order_id Int, customer_id String, customer_name String, payment_type String, country String, city String,product_id  String," +
    "product_name String,price String, product_category String,ecommerce_website_name String ," +
    "payment_txn_id String, payment_txn_success String,failure_reason String) row format delimited fields terminated by ',' stored as textfile")

  spark.sql("LOAD DATA LOCAL INPATH 'input/orders.txt' OVERWRITE INTO TABLE orders")
  var orderTransfer = new ListBuffer[String]
  var ordercount = 1

  var orderTransferString:String = spark.sql(s"Select order_id,customer_id,customer_name,product_id,product_name,product_category,payment_type,price,country,city," +
    s"ecommerce_website_name,payment_txn_id,payment_txn_success, failure_reason FROM orders WHERE order_id == $ordercount").show(100000000,false).toString()

  val f2 = new File("input/OrdersOrg.txt")
  val fw2 = new FileWriter(f2)
  fw2.write(orderTransferString)

  val s2 = new Scanner()




  for(i <- 0 until allOrders.length)
    {
      //qty,datetime,


      ordercount = ordercount + 1
    }







  val orderoutput:List[String]= orderTransfer.toList



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
