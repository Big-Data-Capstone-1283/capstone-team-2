package producer

import java.io.{File, FileWriter}
import scala.collection.mutable.ListBuffer

class CSVBuilder {


// creates a customers list using the personGen's toString(String) method
// can reformat to fit schema
  def customerList(arr:Int): ListBuffer[String] =
  {

    var output = new ListBuffer[String]
    for(i <- 0 until arr)
    {
      var p = new personGen
      output += p.toString()

    }

    output
  }

  //creates a Customer CSV and adds it to the input folder
  def createCustomersCSV(arr:Int):Unit = {

    val customers = customerList(arr)
    var f = new File("input/Customers.csv")
    var custString = new String
    for(i <- 0 until customers.length)
    {
      custString=s"$custString \n"+customers(i)
    }

    val fw = new FileWriter(f)
    fw.write(custString)
    fw.close()
  }
}
