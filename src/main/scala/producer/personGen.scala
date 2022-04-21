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
  private val paymentType:String = genPayment
  private val datetime:String = ""
  private val startDate:String=""
  private val endDate:String =""

// all CSV are in the input directory



// creates a list of first names to be used in creating the full name// needs a string as a CSV file as input
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
// creates a list of last names to be used in creating the full name // needs CSV string as input
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


//creates a list of countries that will be used in creating the String for the csv // needs a CSV name as text
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
//creates a list of cities that will be used in creating the String for csv needs a country name as input
def createCityList(countryName:String):ListBuffer[String] = {
  var cities = new ListBuffer[String]
  var cityfileName = new String
  countryName match{
    case "Australia" => cityfileName = "input/AU10Cities.csv"
    case "Canada"=> cityfileName = "input/CA10Cities.csv"
    case "New_Zealand" => cityfileName = "input/NZ10Cities.csv"
    case "United_States_of_America" => cityfileName ="input/US10Cities.csv"
    case "United_Kingdom" => cityfileName = "input/UK10Cities.csv"
  }
  val f = new File(cityfileName)
  var s = new Scanner(f)

  while(s.hasNext)
    {
      cities += s.next()
    }
  return cities
}

//generates a random country by using the Country list
def genRandomCountry:String={
  var countryName = createCountryList("input/Country.csv")
  val rng = new Random()
  val output:String = countryName(rng.nextInt(countryName.length - 1))

  output
}


//generates a random city by using the City List
def genRandomCity:String={
  var cityName = createCityList(country)
  val rng = new Random()
  val output:String = cityName(rng.nextInt(cityName.length -1))
  output
}

def genPayment:String={
  var output = new String
// payment rates to be changed based off of Patterns USes the weighted Randomizer class
  cus_country match{


    case "Australia" => output ={val paymentRates = Map("Card" -> 60, "Internet Banking" -> 10, "UPI" -> 5, "Wallet" -> 25)
      WeightedRandomizer(paymentRates)}

    case "Canada" => output ={val paymentRates = Map("Card" -> 60, "Internet Banking" -> 10, "UPI" -> 5, "Wallet" -> 25)
      WeightedRandomizer(paymentRates)}

    case "New_Zealand" =>output ={val paymentRates = Map("Card" -> 60, "Internet Banking" -> 10, "UPI" -> 5, "Wallet" -> 25)
      WeightedRandomizer(paymentRates)}

    case "United_Kingdom" => output ={val paymentRates = Map("Card" -> 60, "Internet Banking" -> 10, "UPI" -> 5, "Wallet" -> 25)
      WeightedRandomizer(paymentRates)}

    case "United_States_of_America" => output = {{val paymentRates = Map("Card" -> 60, "Internet Banking" -> 10, "UPI" -> 5, "Wallet" -> 25)
      WeightedRandomizer(paymentRates)}}

    case default => output = "Card"
  }
  output
}


def gendateTime:String ={
  var output = new String
  val



  var timezone = 0


  output
}

//generate a random first name
  def genRandomFname: String = {
    val fname = createFnameList("input/FirstName.csv")
    var output = new String
    val rng = new Random()
    output = fname(rng.nextInt(fname.length))
    output
  }

//generate a random last night
  def genRandomLname: String = {
    val Lname = createLnameList("input/LastName.csv")
    var output = new String
    val rng = new Random()
    output = Lname(rng.nextInt(Lname.length))
    output
  }

//generates a full name string with first and last name separated by a space
def fullNameGen(fname:String,lname:String):String={
  val fullname = s"$fname $lname"
  fullname
}

//generates a customer_id based on the first and last name and 7 numerical characters Ex: Alex White would be AW1234567
def genID(fname:String,lname:String):String = {
  val fChar = fname.charAt(0)
  val lChar = lname.charAt(0)
  val lower =1000000
  val upper =9999999


  val namerng = (new Random(upper).nextInt() + lower)

  val ID:String = s"$fChar$lChar$namerng"
  return ID
}

//-----------------------------------------
  // Getters for Customer_name, customer_id, Country, and city
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
  def payment_type:String={
    val payment = s"$paymentType"
    payment
  }

//basic toString that creates the string for the CSV input

  override def toString(): String = {

    val output = s"$customer_id,$customer_name,$payment_type,$country,$city"
    println(output)
    output
  }





}
