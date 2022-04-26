package producer

object main {


  def main(args: Array[String]): Unit = {


   //val customer = new CSVBuilder
    //customer.createCustomersCSV(10000)

    Orders.printOrderList(100,1)


    /**
    val prod = new Products


    val testList =prod.productsLists.foods_list("input/products_food.csv")

    for(e <- 0 until testList.length)
      {
        println(testList)
      }
     */
  }

}
