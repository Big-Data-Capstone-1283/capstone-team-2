package producer

class Products {




  object tool extends  Product{
    override var product_id: String = ""
    override var product_name: String = ""
    override var product_category: String =""
    override var price: String = ""
    override var ecommerce_website_name: String = ""

    override def setproduct_id(name: String): Unit = ???

    override def setproduct_name(name: String): Unit = ???

    override def setproduct_category(name: String): Unit = ???

    override def setprice(name: String): Unit = ???

    override def setecommerce_website_name(name: String): Unit = ???

    override def getproduct_id: String = ???

    override def getproduct_name: String = ???

    override def getproduct_category: String = ???

    override def getprice: String = ???

    override def getecommerce_website_name: String = ???
  }

  object book extends Product{
    override var product_id: String = ""
    override var product_name: String = ""
    override var product_category: String = ""
    override var price: String = ""
    override var ecommerce_website_name: String = ""

    override def setproduct_id(name: String): Unit = ???

    override def setproduct_name(name: String): Unit = ???

    override def setproduct_category(name: String): Unit = ???

    override def setprice(name: String): Unit = ???

    override def setecommerce_website_name(name: String): Unit = ???

    override def getproduct_id: String = ???

    override def getproduct_name: String = ???

    override def getproduct_category: String = ???

    override def getprice: String = ???

    override def getecommerce_website_name: String = ???
  }

  object clothing extends Product{
    override var product_id: String = ""
    override var product_name: String = ""
    override var product_category: String = ""
    override var price: String = ""
    override var ecommerce_website_name: String = ""

    override def setproduct_id(name: String): Unit = ???

    override def setproduct_name(name: String): Unit = ???

    override def setproduct_category(name: String): Unit = ???

    override def setprice(name: String): Unit = ???

    override def setecommerce_website_name(name: String): Unit = ???

    override def getproduct_id: String = ???

    override def getproduct_name: String = ???

    override def getproduct_category: String = ???

    override def getprice: String = ???

    override def getecommerce_website_name: String = ???
  }


  object electronic extends Product{
    override var product_id: String =""
    override val product_name: String =""
    override val product_category: String = ""
    override val price: String = ""
    override val ecommerce_website_name: String = ""

    override def setproduct_id(name: String): Unit = {
      this.product_id = name
    }

    override def setproduct_name(name: String): Unit = {


    }

    override def setproduct_category(name: String): Unit = {


    }

    override def setprice(name: String): Unit = ???

    override def setecommerce_website_name(name: String): Unit = ???

    override def getproduct_id: String = ???

    override def getproduct_name: String = ???

    override def getproduct_category: String = ???

    override def getprice: String = ???

    override def getecommerce_website_name: String = ???
  }

  object  food extends Product{
    override var product_id: String = ""
    override var product_name: String = ""
    override var product_category: String = ""
    override var price: String = ""
    override var ecommerce_website_name: String = ""

    override def setproduct_id(name: String): Unit = ???

    override def setproduct_name(name: String): Unit = ???

    override def setproduct_category(name: String): Unit = ???

    override def setprice(name: String): Unit = ???

    override def setecommerce_website_name(name: String): Unit = ???

    override def getproduct_id: String = ???

    override def getproduct_name: String = ???

    override def getproduct_category: String = ???

    override def getprice: String = ???

    override def getecommerce_website_name: String = ???
  }
}
