package producer

class Products {

  import java.io.{File, FileWriter}
  import java.util.Scanner
  import scala.collection.mutable.ListBuffer

  object productsLists{

    val pr = new Products

    def foods_list(fileName:String):ListBuffer[String]= {
        val foodList = new ListBuffer[String]
        val f = new File(fileName)
        val s = new Scanner(f)


         while(s.hasNext())
           {
             foodList+=s.nextLine()
           }
        return foodList
      }


  }


  object tool extends Product {



    override var product_id: String = _
    override var product_name: String = _
    override var product_category: String = _
    override var price: String = _
    override var ecommerce_website_name: String = _

  }

  object book extends Product {
    private val filename: String = "product_books.csv"
    val f = new File(filename)
    val s = new Scanner(f)

    override var product_id: String = _
    override var product_name: String = _
    override var product_category: String = _
    override var price: String = _
    override var ecommerce_website_name: String = _
  }

    object clothing extends Product {
      private val filename: String = "product_clothes.csv"
      val f = new File(filename)
      val s = new Scanner(f)

      override var product_id: String = _
      override var product_name: String = _
      override var product_category: String = _
      override var price: String = _
      override var ecommerce_website_name: String = _

    }


    object electronic extends Product {
      private val filename: String = "product_electronics.csv"
      val f = new File(filename)
      val s = new Scanner(f)

      override var product_id: String = _
      override var product_name: String = _
      override var product_category: String = _
      override var price: String = _
      override var ecommerce_website_name: String = _

    }

    object food extends Product {
      private val filename:String ="product_food"
      val f = new File(filename)
      val s = new Scanner(f)

      override var product_id: String = _
      override var product_name: String = _
      override var product_category: String = _
      override var price: String = _
      override var ecommerce_website_name: String = _
    }

}
