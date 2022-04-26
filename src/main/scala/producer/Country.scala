package producer


case class City(
       name:String
)

case class Country (
  name:String,
  city:City,
  timezoneHrModifier:Int
)