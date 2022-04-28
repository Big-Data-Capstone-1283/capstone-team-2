package producer

import org.joda.time.DateTime
import java.time.{DayOfWeek, LocalDate, Month, YearMonth}
import java.util.stream.IntStream
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.Random

object dateTimeGenerator {
  var cityString=""
  var MM: Int = 0
  val yyyy=2022
  var dd:Int= 0
  var hh:Int= 0
  var mm:Int= 0
  var ss:Int= 0
  var thisMonthLength:Int=0
  var dayOfEntireRange_Modifier:Int=0

  private def getHour(city:String): Int = {
    val filepath = "input/TIMEZONES_cities.csv"
    val timezones = mutable.Map[String, Int]()
    Source.fromFile(filepath)
      .getLines()
      .drop(1)
      .map(_.split(","))
      .foreach { case x => timezones.put(x(0), x(1).toInt)
      }
    //println(timezones)
    var weightsMap=scala.collection.mutable.Map[Int, Int]()
    //fill weight map with 100 weight for every day of month
    for( i<- 0 to  23){
      weightsMap+= i -> 100
    }
    val busyHours=Array(13,20,21)
    for( i<- busyHours.indices){
      weightsMap(busyHours(i))=125
    }
    hh=WeightedRandomizer(weightsMap.toMap)
    val timezoneMod:Int=timezones.getOrElse(city,0)
    //println(timezoneMod)
    hh=hh+timezoneMod
    if (hh>24) {
      hh=hh-24
    }
    else if(hh<0){
      hh=hh+24
    }
    hh
  }

  private def genRandomInt(min:Int=0,max:Int=59):Int={
    Random.between(min, max)
  }

  def main(args:Array[String]): Unit = {
    //apply("London")
    badDateTime("London")
  }

  def apply(city:String): String = {
    MM = getMonth
    dd =getDay
    hh =getHour(cityString)
    mm=genRandomInt()
    ss=genRandomInt()

    cityString=city
    getString
  }
  def badDateTime(city:String):String={
    MM = getMonth
    dd =getDay
    hh =getHour(cityString)
    mm=genRandomInt()
    ss=genRandomInt()
    cityString=city
    var incorrectString=getString
    incorrectString= incorrectString.substring(0, 5) + "13" + incorrectString.substring(7)
    incorrectString
  }

  private def getMonth:Int={
    var month= WeightedRandomizer(Map("January" -> 26, "February" -> 43, "March" -> 51))
    val returnMonth= month match{
      case "January" => 1
      case "February" => 2
      case "March" => 3
      case default => 3
    }
    returnMonth//return
  }

  private def getDay:Int={
    val year = 2022
      var month:Month=null
    MM match{
      case 1 => month=Month.JANUARY
      case 2=> {
        month=Month.FEBRUARY
        dayOfEntireRange_Modifier= {
          dayOfEntireRange_Modifier + YearMonth.of(year, Month.JANUARY).lengthOfMonth
        }
      }
      case 3 => {
        month=Month.MARCH
        dayOfEntireRange_Modifier= {
          dayOfEntireRange_Modifier + YearMonth.of(year, Month.JANUARY).lengthOfMonth
          + YearMonth.of(year, Month.FEBRUARY).lengthOfMonth
        }
      }
      case default => Month.MARCH
    }
    thisMonthLength=YearMonth.of(year, month).lengthOfMonth
    var weekendDays:ListBuffer[Int]=new ListBuffer[Int]
    IntStream.rangeClosed(1, YearMonth.of(year, month).lengthOfMonth).mapToObj((day: Int) =>
        LocalDate.of(year, month, day)).filter((date: LocalDate) =>
      (date.getDayOfWeek eq DayOfWeek.SATURDAY) || (date.getDayOfWeek eq DayOfWeek.SUNDAY)).forEach((date: LocalDate) =>
        //System.out.print(date.getDayOfMonth + " "
        weekendDays.append(date.getDayOfMonth)
    )
    var weightsMap=scala.collection.mutable.Map[Int, Int]()
    //fill weight map with 100 weight for every day of month
    for( i<- 1 to thisMonthLength ){
      weightsMap+= i -> 100
    }
    //update weekend days weight to %25 more than non weekend days
    for( i<- weekendDays.indices){
      weightsMap(weekendDays(i))=125
    }
    dd=WeightedRandomizer(weightsMap.toMap)
    dd //return
  }

  private def getString:String= {
    var dateStr = "UNCOMPUTED-YY-MM-dd HH:mm:ss"
    var tempDate = (new DateTime)
      .withYear(2022)
      .withMonthOfYear(MM)
      .withDayOfMonth(dd)
      .withHourOfDay(hh)
      .withMinuteOfHour(mm)
      .withSecondOfMinute(ss)
    var dayOfWeek = tempDate.dayOfWeek().get() // Returns an integer 1 - 7 representing Mon - Sun
    dateStr = tempDate.toString("YYYY-MM-dd HH:mm:ss")
//    println(dayOfWeek)
//    println(dateStr)
    dateStr
  }
}
