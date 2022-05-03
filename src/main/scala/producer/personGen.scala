package producer

import java.io.File
import java.util.Scanner
import scala.collection.mutable.ListBuffer
import scala.util.Random

class personGen {

  var firstName:String = genRandomFname //First name
  var lastName:String = genRandomLname //Last Name
  var fullName:String = s"$firstName $lastName" // Full name ex "Adam Gore"
  var customerID:String = genID(firstName,lastName) //ID AG1234567
  var cus_country:String = genRandomCountry //Country Name
  var cus_city:String =  genRandomCity // City Name within Country
  var paymentType:String = genPayment // Payment type


// all CSV are in the input directory


  /**
   * takes in the name of CSV
   * @param fnameFile
   * @return fname List
   */
  // creates a list of first names to be used in creating the full name// needs a string as a CSV file as input
def createFnameList(fnameFile:String):ListBuffer[String]= {

  val fName = new ListBuffer[String]
  val f = new File(fnameFile)
  val s = new Scanner(f)
  while(s.hasNext)
    {
      fName += s.next()
    }

fName
}

  /**
   * takes in the name of a csv
   * @param lnameFile
   * @return Lname list
   */
  // creates a list of last names to be used in creating the full name // needs CSV string as input
def createLnameList(lnameFile:String):ListBuffer[String] ={

  val lName = new ListBuffer[String]
  val f = new File(lnameFile)
  val s = new Scanner(f)

  while(s.hasNext)
    {
      lName += s.next()
    }



 lName
}


  /**
   * takes in the name of csv
   * @param countryf
   * @return country list
   */
  //creates a list of countries that will be used in creating the String for the csv // needs a CSV name as text
def createCountryList(countryf:String):ListBuffer[String]={
  val country = new ListBuffer[String]
  val f = new File(countryf)
  val s = new Scanner(f)

  while(s.hasNext)
    {
      country+=s.next()
    }

  country
}

  /**
   * Takes in the name of a country
   * @param countryName
   * @return list of countries
   */
  //creates a list of cities that will be used in creating the String for csv needs a country name as input
def createCityList(countryName:String):ListBuffer[String] = {
  val cities = new ListBuffer[String]
  var cityfileName = new String
  countryName match{
    case "Australia" => cityfileName = "input/AU10Cities.csv"
    case "Canada"=> cityfileName = "input/CA10Cities.csv"
    case "New_Zealand" => cityfileName = "input/NZ10Cities.csv"
    case "United_States_of_America" => cityfileName ="input/US10Cities.csv"
    case "United_Kingdom" => cityfileName = "input/UK10Cities.csv"
  }
  val f = new File(cityfileName)
  val s = new Scanner(f)

  while(s.hasNext)
    {
      cities += s.next()
    }
 cities
}

  /**Generates a random country from CSV
   * @return countryName
   */
  //generates a random country by using the Country list
def genRandomCountry:String={
  val countryName = createCountryList("input/Country.csv")
  val rng = new Random()
  val output:String = countryName(rng.nextInt(countryName.length - 1))

  output
}


//generates a random city by using the City List

  /**
   * Genereates a random city based off of the country using createCityList
   * @return cityName
   */
  def genRandomCity:String={
  val cityName = createCityList(country)
  val rng = new Random()
  val output:String = cityName(rng.nextInt(cityName.length -1))
  output
}

  /**
   * Generates a random payment_type
   * @return output payment_type
   */
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

  /**
   * Generates a random first name
   * @return fname
   */
  //generate a random first name
  def genRandomFname: String = {
    val fname = createFnameList("input/FirstName.csv")
    var output = new String
    val rng = new Random()
    output = fname(rng.nextInt(fname.length))
    output
  }

  /**
   * Generates a rangdom last name csv
   * @return lname
   */
  //generate a random last night
  def genRandomLname: String = {
    val Lname = createLnameList("input/LastName.csv")
    var output = new String
    val rng = new Random()
    output = Lname(rng.nextInt(Lname.length))
    output
  }

  /**
   * @param fname First name from the firstname generator
   * @param lname Last name from the lastname generator
   * @return "First Last"
   */
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


  val namerng = Random.between(lower,upper)

  val ID:String = s"$fChar$lChar$namerng"
  ID
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

  /**
   * overloaded string method. Prints out all of the aspects of a person.
   * @return output String.
   */
  override def toString(): String = {

    var output = new String
    val falseData = Map("Y" -> 97, "N" -> 3)
    val checking = WeightedRandomizer(falseData)

    if (checking == "Y"){
      output = s"$customerID,$fullName,$paymentType,$cus_country,$cus_city"

    }
    else
      {
        falsePersonGen
        output = s"$customerID,$fullName,$paymentType,$cus_country,$cus_city"
      }

    output
  }


  /**
   * Changes pieces of the  person data to make them false.
   */
  def falsePersonGen:Unit = {

   val rng = scala.util.Random

   val check = rng.nextInt(5)

   check match {
     case 1 => customerID = customerID + rng.nextInt(10000)
     case 2 => fullName = firstName+lastName
     case 3 => paymentType = "Cold Hard Cash"
     case 4 => cus_country = "Mozambique"
     case 5 => cus_city = " "
   }
 }



}
