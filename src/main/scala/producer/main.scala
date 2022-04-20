package producer

object main {


  def main(args: Array[String]): Unit = {


    val customer = new CSVBuilder
    customer.createCustomersCSV(10000)




  }

}
