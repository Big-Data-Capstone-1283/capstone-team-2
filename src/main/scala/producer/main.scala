package producer

object main {


  def main(args: Array[String]): Unit = {


    val count = 10000
    for (i <- 0 until count) {
      var per = new personGen
      println(per.toString("d"))
    }


  }

}
