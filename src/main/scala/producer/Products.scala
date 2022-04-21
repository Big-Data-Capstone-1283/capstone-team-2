package producer

class Products(person:personGen) {

  import java.io.{File, FileWriter}
  import java.util.Scanner
  import scala.collection.mutable.ListBuffer

  val eleList:ListBuffer[String]= productsLists.electronicsList("input/products_electronics.csv")
  val bookList:ListBuffer[String] = productsLists.booksList("input/products_books.csv")
  val clothList:ListBuffer[String]= productsLists.clothesList("input/products_clothes.csv")
  val foodList:ListBuffer[String] = productsLists.foodsList("input/products_food.csv")
  val toolList:ListBuffer[String] = productsLists.toolsList("input/products_tools.csv")

  object productsLists {


    def foodsList(fileName: String): ListBuffer[String] = {
      val foods = new ListBuffer[String]
      val f = new File(fileName)
      val s = new Scanner(f)


      while (s.hasNext()) {
        foods += s.nextLine()
      }
      return foods
    }


    def toolsList(fileName: String): ListBuffer[String] = {
      val tools = new ListBuffer[String]
      val f = new File(fileName)
      val s = new Scanner(f)


      while (s.hasNext()) {
        tools += s.nextLine()
      }
      return tools
    }

    def clothesList(fileName: String): ListBuffer[String] = {
      val clothes = new ListBuffer[String]
      val f = new File(fileName)
      val s = new Scanner(f)


      while (s.hasNext()) {
        clothes += s.nextLine()
      }
      return clothes
    }

    def booksList(fileName: String): ListBuffer[String] = {
      val books = new ListBuffer[String]
      val f = new File(fileName)
      val s = new Scanner(f)


      while (s.hasNext()) {
        books += s.nextLine()
      }
      return books
    }

    def electronicsList(fileName: String): ListBuffer[String] = {
      val electronics = new ListBuffer[String]
      val f = new File(fileName)
      val s = new Scanner(f)


      while (s.hasNext()) {
        electronics += s.nextLine()
      }
      return electronics
    }

  }



  def assignPersontoProduct:String={
    var personProductString = new String
    var ordercat = new String

    var rng = scala.util.Random


    person.country match{
      case "Australia" => {val categoryRate = Map("books" -> 19, "clothes"->19, "electronics" -> 19, "food" -> 19, "tools" -> 24)
        ordercat =  WeightedRandomizer(categoryRate)}

      case "Canada" => {val categoryRate = Map("books" ->24, "clothes"-> 19, "electronics" -> 19, "food" -> 19, "tools" -> 19)
        ordercat =  WeightedRandomizer(categoryRate)}

      case "New_Zealand" => {val categoryRate = Map("books" -> 19, "clothes"-> 19, "electronics" -> 19, "food" -> 24, "tools" -> 19)
        ordercat =  WeightedRandomizer(categoryRate)}

      case "United_States_of_America" => {val categoryRate = Map("books" -> 19, "clothes"-> 19, "electronics" -> 24, "food" -> 19, "tools" -> 19)
      ordercat =  WeightedRandomizer(categoryRate)}

      case "United_Kingdom" => {val categoryRate = Map("books" -> 19, "clothes"-> 24, "electronics" -> 19, "food" -> 19, "tools" -> 19)
        ordercat =  WeightedRandomizer(categoryRate)}
      case default  => {val categoryRate = Map("books" -> 20, "clothes" -> 20, "electronics" -> 20, "food" -> 20, "tools" -> 20)
        ordercat = WeightedRandomizer(categoryRate)}
    }

    var productString = new String
    var txabrv = new String
    ordercat match{
      case "books" => {val rngnum = rng.nextInt(bookList.length -1)
        productString = bookList(rngnum) + ",books"
        txabrv = "bk"}
      case "clothes" => {val rngnum = rng.nextInt(clothList.length -1)
      productString = clothList(rngnum) + ",clothes"
      txabrv = "cs"}
      case "electronics" =>{val rngnum = rng.nextInt(eleList.length -1)
      productString = eleList(rngnum) + ",electronics"
      txabrv ="es"}
      case "food" => {val rngnum =rng.nextInt(foodList.length -1)
      productString = foodList(rngnum)+ ",food"
      txabrv ="fd"}
      case "tools" => {val rngnum = rng.nextInt(toolList.length -1)
      productString = toolList(rngnum)+ ",tools"
      txabrv ="ts"}
      case default => {val rngnum = rng.nextInt(eleList.length -1)
      productString = eleList(rngnum)+ ",electronics"
      txabrv ="es"}
    }


    val txn_IdRNG = scala.util.Random.between(100000,999999)
    val txn_id = txabrv+txn_IdRNG
    val txn_success = "Y"
    val fail = "no failure"
    personProductString = person.toString()+s",$productString,+$txn_id,$txn_success,$fail"

    //println(personProductString)
    personProductString
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
