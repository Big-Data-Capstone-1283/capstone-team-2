package producer

import java.io.File

class StringBuilder {
  private val order_id =""
  private val customer_id =""
  private val customer_name =""
  private val product_name =""
  private val product_category =""
  private val payment_type =""
  private val qty =""
  private val datetime =""
  private val country =""
  private val city =""
  private val ecommerce_website_name = ""
  private val payment_txn_id =""
  private val payment_txn_success = ""
  private val failure_reason = ""

  object StringBuilder
  {


    //takes in a csv file and parses out the parts of the string
    def customer(f:File): Unit ={




    }

    def stringConcat: String =
    {
      val finalString =s"$order_id,$customer_id,$customer_name,$product_name,$product_category,$payment_type,$qty" +
        s",$datetime,$country,$city,$ecommerce_website_name,$payment_txn_id,$payment_txn_success,$failure_reason"
      return finalString
    }






  }





}
