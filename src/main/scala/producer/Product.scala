package producer

trait Product {

  //variables used in each class

  var product_id:String
  var product_name:String
  var product_category:String
  var price:String
  var ecommerce_website_name:String

  //setter methods for each class

  def setproduct_id(name: String): Unit = {product_id = name}
  def setproduct_name(name: String): Unit = {product_name = name}
  def setproduct_category(name: String): Unit = {product_category = name}
  def setprice(name: String): Unit = {price = name}
  def setecommerce_website_name(name: String): Unit = {ecommerce_website_name = name}
  def getproduct_id: String = {product_id}
  def getproduct_name: String = {product_name}
  def getproduct_category: String = {product_category}
  def getprice: String = {price}
  def getecommerce_website_name: String = {ecommerce_website_name}
}
