package producer

trait Product {

  //variables used in each class

  var product_id:String
  var product_name:String
  var product_category:String
  var price:String
  var ecommerce_website_name:String

  //setter methods for each class

  def setproduct_id(name:String)
  def setproduct_name(name:String)
  def setproduct_category(name:String)
  def setprice(name:String)
  def setecommerce_website_name(name:String)

  //getter methods for each class

  def getproduct_id:String
  def getproduct_name:String
  def getproduct_category:String
  def getprice:String
  def getecommerce_website_name:String

}
