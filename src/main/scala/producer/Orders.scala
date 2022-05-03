package producer

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

object Orders {
  val badData = Map("G" -> 97, "B" -> 3 ) // Map for the percentage of bad data B indicates the ~percentage of bad data


  /**
   * Takes in a number of orders and the start of index
   *
   * @param numOrders
   * @param startAt
   * @return
   */
  def createOrder(numOrders:Int,startAt:Int):List[String]={
  val allOrders = new ListBuffer[String]
  var count = startAt
  val numOfPeople = numOrders /2
  val peopleList = new ListBuffer[personGen]


  for(i <-0 to numOfPeople)
    {
      val person = new personGen
      peopleList+= person
    }




  for(i <- 0 to numOrders)
  {
    val badcheck = WeightedRandomizer(badData)

    val rng = scala.util.Random
    val person = peopleList(rng.nextInt(numOfPeople))

    val products = new Products(person)

    if(badcheck == "G") {
      allOrders += count + "," + products.assignPersontoProduct
    }
    else if(badcheck == "B") {
        allOrders += count + " " + products.falseProductGen
      }
    count = count + 1
  }

  val orderoutput:List[String] =allOrders.toList


  orderoutput
}


  /**
   * Used to print the orders that are created above
   * @param numOrder number of orders to generate
   * @param order number at which the index of orders begin at same as the create orders
   */
  def printOrderList(numOrder:Int,order:Int): Unit =
  {

    var orders:List[String] =createOrder(numOrder,order:Int)
    for(i <- 0 until orders.length -1 )
      {
        println(orders(i))
      }
  }









}
