package producer

import java.io.File
import java.util.Scanner
import scala.collection.mutable.ListBuffer
import scala.util.Random

class personGen {

  private val firstName:String = genRandomFname
  private val lastName:String = genRandomLname
  private val customerID:String = genID(firstName,lastName)
  private val cus_country:String = genRandomCountry
  private val cus_city:String =  genRandomCity

def createFnameList(fnameFile:String):ListBuffer[String]= {

  var fName = new ListBuffer[String]
  val f = new File(fnameFile)
  var s = new Scanner(f)
  while(s.hasNext)
    {
      fName += s.next()
    }

return fName
}

def createLnameList(lnameFile:String):ListBuffer[String] ={

  var lName = new ListBuffer[String]
  val f = new File(lnameFile)
  var s = new Scanner(f)

  while(s.hasNext)
    {
      lName += s.next()
    }



  return lName
}

def createCountryList(countryf:String):ListBuffer[String]={
  var country = new ListBuffer[String]
  val f = new File(countryf)
  var s = new Scanner(f)

  while(s.hasNext)
    {
      country+=s.next()
    }

  return  country
}

def createCityList(countryName:String):ListBuffer[String] = {
  var cities = new ListBuffer[String]
  var cityfileName = new String
  countryName match{
    case "Australia" => cityfileName = "AU10Cities.csv"
    case "Canada"=> cityfileName = "CA10Cities.csv"
    case "New_Zealand" => cityfileName = "NZ10Cities.csv"
    case "United_States_of_America" => cityfileName ="US10Cities.csv"
    case "United_Kingdom" => cityfileName = "UK10Cities.csv"
  }
  val f = new File(cityfileName)
  var s = new Scanner(f)

  while(s.hasNext)
    {
      cities += s.next()
    }
  return cities
}

def genRandomCountry:String={
  var countryName = createCountryList("Country.csv")
  val rng = new Random()
  val output:String = countryName(rng.nextInt(countryName.length - 1))
  output
}



def genRandomCity:String={
  var cityName = createCityList(country)
  val rng = new Random()
  val output:String = cityName(rng.nextInt(cityName.length -1))
  output
}




  def genRandomFname: String = {
    val fname = createFnameList("FirstName.csv")
    var output = new String
    val rng = new Random()
    output = fname(rng.nextInt(fname.length))
    output
  }


  def genRandomLname: String = {
    val Lname = createLnameList("LastName.csv")
    var output = new String
    val rng = new Random()
    output = Lname(rng.nextInt(Lname.length))
    output
  }


def fullNameGen(fname:String,lname:String):String={
  val fullname = s"$fname $lname"
  fullname
}


def genID(fname:String,lname:String):String = {
  val fChar = fname.charAt(0)
  val lChar = lname.charAt(0)
  val namerng = Random.between(1000000,9999999)
  val ID:String = s"$fChar$lChar$namerng"
    return ID
}


  def customer_name:String={
    val fullname = s"$firstName $lastName"
    fullname
  }
  def customer_id:String={
    val id = s"$customerID"
    id
  }
  def country:String={
    val country =s"$cus_country"
    country
  }
  def city:String={
    val city = s"$cus_city"
    city
  }

def toString(d:String): String =
  {
    val output = s"$customer_id,$customer_name,$country,$city"
    output
  }


}
