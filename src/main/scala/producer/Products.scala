package producer

class Products(person:personGen) {

  import java.io.{File, FileWriter}
  import java.util.Scanner
  import scala.collection.mutable.ListBuffer

  val eleList:ListBuffer[Product]= productsLists.electronicsList("input/products_electronics.csv")
  val bookList:ListBuffer[Product] = productsLists.booksList("input/products_books.csv")
  val clothList:ListBuffer[Product]= productsLists.clothesList("input/products_clothes.csv")
  val foodList:ListBuffer[Product] = productsLists.foodsList("input/products_food.csv")
  val toolList:ListBuffer[Product] = productsLists.toolsList("input/products_tools.csv")

  object productsLists {




    def foodsList(fileName: String): ListBuffer[Product] = {
      val foods = new ListBuffer[Product]
      val f = new File(fileName)
      val s = new Scanner(f)
      var price = new String
      var name = new String
      s.useDelimiter(",")

      while (s.hasNextLine) {




        var line = new String
        line = s.nextLine()

        val lines = line.split(",")

        name = lines(0)
        price = lines(1)

        //println(s" the name $name")
        //println(s"the price $price")

       val pro = new Product(name,price)
        foods += pro
      }
      return foods
    }


    def toolsList(fileName: String): ListBuffer[Product] = {
      val tools = new ListBuffer[Product]
      val f = new File(fileName)
      val s = new Scanner(f)
      var price = new String
      var name = new String
      s.useDelimiter(",")

      while (s.hasNextLine()) {
        var line = new String
        line = s.nextLine()
        val lines = line.split(",")
        name = lines(0)
        price = lines(1)

        //println(s" the name $name")
        //println(s"the price $price")

        val pro = new Product(name,price)

        tools += pro
      }
      return tools
    }

    def clothesList(fileName: String): ListBuffer[Product] = {
      val clothes = new ListBuffer[Product]
      val f = new File(fileName)
      val s = new Scanner(f)
      var price = new String
      var name = new String
      s.useDelimiter(",")

      while (s.hasNextLine()) {

        var line = new String
        line = s.nextLine()
        val lines = line.split(",")
        name = lines(0)
        price = lines(1)
        //println(s" the name $name")
        //println(s"the price $price")
       val pro = new Product(name,price)
        clothes += pro
      }
      return clothes
    }

    def booksList(fileName: String): ListBuffer[Product] = {
      val books = new ListBuffer[Product]
      val f = new File(fileName)
      val s = new Scanner(f)
      var price = new String
      var name = new String
      s.useDelimiter(",")


      while (s.hasNextLine()) {

        var line = new String
        line = s.nextLine()
        val lines = line.split(",")
        name = lines(0)
        price = lines(1)

        //println(s" the name $name")
        //println(s"the price $price")

        val pro = new Product(name,price)

        books += pro
      }
      return books
    }

    def electronicsList(fileName: String): ListBuffer[Product] = {
      val electronics = new ListBuffer[Product]
      val f = new File(fileName)
      val s = new Scanner(f)
      var price = new String
      var name = new String

      s.useDelimiter(",")

      while (s.hasNext()) {

        var line = new String
        line = s.nextLine()
        val lines = line.split(",")
        name = lines(0)
        price = lines(1)

        //println(s" the name $name")
        //println(s"the price $price")

        val pro = new Product(name,price)
        electronics += pro
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

    var rngnum = 0
    var price= new String
    ordercat match{
      case "books" => {rngnum = rng.nextInt(bookList.length -1)
        productString = bookList(rngnum).getName + ",books"
        txabrv = "bk"
      price = bookList(rngnum).getPrice}
      case "clothes" => {rngnum = rng.nextInt(clothList.length -1)
      productString = clothList(rngnum).getName + ",clothes"
      txabrv = "cs"
      price = clothList(rngnum).getPrice}
      case "electronics" =>{ rngnum = rng.nextInt(eleList.length -1)
      productString = eleList(rngnum).getName + ",electronics"
      txabrv ="es"
      price =eleList(rngnum).getPrice}
      case "food" => {rngnum =rng.nextInt(foodList.length -1)
      productString = foodList(rngnum).getName+ ",food"
      txabrv ="fd"
      price = foodList(rngnum).getPrice}
      case "tools" => { rngnum = rng.nextInt(toolList.length -1)
      productString = toolList(rngnum).getName+ ",tools"
      txabrv ="ts"
      price =foodList(rngnum).getPrice}
      case default => { rngnum = rng.nextInt(eleList.length -1)
      productString = eleList(rngnum).getName +",electronics"
      txabrv ="es"
      price=eleList(rngnum).getPrice}
    }


    val txn_IdRNG = scala.util.Random.between(100000,999999)
    val product_id = scala.util.Random.between(10000000,99999999)
    val txn_id = txabrv+txn_IdRNG
    val txn_success = "Y"
    val fail = "no failure"
    val ecoom_website = "Amazon"
    personProductString = s"${person.customer_id},${person.customer_name},$product_id,$productString,$price,${person.country},${person.city},$txn_id,$txn_success,$fail"

    //println(personProductString)
    personProductString
  }

}

class Product(name:String, price:String)
{
  val Name =name
  val Price =price
  def getName:String ={
    Name
  }
  def getPrice:String ={
    Price
  }


}