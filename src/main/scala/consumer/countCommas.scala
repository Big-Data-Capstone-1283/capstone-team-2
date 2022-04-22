package consumer

object countCommas extends App{

  val rowMap = scala.collection.mutable.HashMap.empty[String,Int]

  val rowTest = "1,101,John  Smith,201,Pen,Stationery,Card,24,10,2021-01-10  10:12,India,Mumbai,www.amazon.com,36766,Y"

  for (symbol <- rowTest) {if (rowMap.contains(symbol.toString)) {rowMap(symbol.toString) = rowMap(symbol.toString) + 1} else {rowMap.+=((symbol.toString,1))}}


  val commaCount = rowMap(",")
}
