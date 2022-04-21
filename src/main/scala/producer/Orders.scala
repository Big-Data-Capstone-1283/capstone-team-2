package producer

import scala.collection.mutable.ListBuffer

object Orders {


def createOrder(numOrders:Int):ListBuffer[String]={
  var allOrders = new ListBuffer[String]
  var count = 1

  for(i <- 0 to numOrders)
    {
      val person = new personGen
      val products = new Products(person)
      allOrders += count + ","+ products.assignPersontoProduct
      count = count + 1
    }
  return allOrders
}

  def printOrderList(numOrder:Int): Unit =
  {
    //currently order is a bit bad, but that is based off the data. plans to go back in and add everything to a spark rdd then use the select
    // statments to properly format the data into strings then we can send to team 1's consumer
    var orders:ListBuffer[String] =createOrder(numOrder)
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
