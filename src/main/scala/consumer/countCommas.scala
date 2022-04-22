package consumer

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap}

object countCommas extends App{
  /*
  * Sixteen Columns, should have 15 commas
  *
  * */

  val rowArray = ArrayBuffer("1,101,John  Smith,201,Pen,Stationery,Card,24,10,2021-01-10  10:12,India,Mumbai,www.amazon.com,36766,Y",
    "2,102,Mary  Jane,202,Pencil,Stationery,Internet  Banking,36,5,2021-10-31 13:45,USA,Boston,www.flipkart.com,37167,Y",
    "3,103,Joe Smith,203,Some mobile,Electronics,UPI,1,4999,2021-04-23 11:32,UK,Oxford,www.tatacliq.com,90383,Y",
    "4,104,Neo,204,Some laptop,Electronics,Wallet,1,59999,2021-06-13 15:20,India,Indore,www.amazon.in,12224,N,Invalid  CVV.",
    "5,104,Neo,204,Some laptop,Electronics,Wallet,1,59999,2021-06-13 15:20,India,Indore,www.amazon.in,12224,N,Invalid  CVV.")


  //val rowMap = scala.collection.mutable.HashMap.empty[String,Int]
  //val rowTest = "1,101,John  Smith,201,Pen,Stationery,Card,24,10,2021-01-10  10:12,India,Mumbai,www.amazon.com,36766,Y"

  //key,value = Char.toString, Int
  //counts number of Chars


  def commaCounter(rowString:String) = {
    val rowMap = scala.collection.mutable.HashMap.empty[String,Int]
    for (symbol <- rowString) {
      if (rowMap.contains(symbol.toString)) {
        rowMap(symbol.toString) = rowMap(symbol.toString) + 1
      } else {
        rowMap.+=((symbol.toString, 1))
      }
    }
    rowMap(",")


  }
  //for (i <- rowArray) {commaCounter(i)}
  var correctColNum:Array[String] = Array()
  for (i<-rowArray) {
    //Number of commas should be fifteen
    if (commaCounter(i) == 15)
      correctColNum = correctColNum :+ i
    //println(commaCounter(i))
  }
  //println(correctColNum.mkString)
  for (i <- correctColNum) {println(i)}


}
