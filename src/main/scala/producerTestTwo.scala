import java.util.Properties
//import collection.mutable.ListBuffer
//import java.io._
//import scala.io._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{IntegerSerializer, StringSerializer}

object producerTestTwo extends App{
  //in intellij, create output directory
  //start WSL/Ubuntu
  //cd into kafka file
  //cd config
  //nano server properties
  //configure advertised.listeners = PLAINTEXT://192.168.204.194:9092
  //replace localhost:9092 with MY_IPADDRESS:9092
  //My IP_ADDRESS changes as it is determined by router
  //user hostname-I to check IP address
  //172.18.217.222:9092
  //run kafka zookeeper
  //run kafka broker server

  //run intellij producer
  //to check running topics
  //use bin/./kafka-topics.sh --list --bootstrap-server 172.18.217.222:9092
  //you should see sql_dolphins2
  //or cd to bin, then run ./kafka-topics.sh --list --bootstrap-server localhost:9092

  //run consumer


  //this will need to change to whatever the other team names their topic
  val topicName = "doug_test2"

  //ip address is for local computer, and may change depending on dhcp of home router, will have to change if it suddenly doesn't work
  val producerProperties = new Properties()
  //producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  //-----AMAZON EC2 IP ADDRESS = 3.93.174.172 -----
  producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "ec2-3-93-174-172.compute-1.amazonaws.com:9092")
  producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[IntegerSerializer].getName)
  producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

  val producer = new KafkaProducer[Int, String](producerProperties)



  //SENDS A MESSAGE TO THE TOPIC
  val transaction="12345"
  //producer.send(new ProducerRecord[Int, String](topicName, 1,transaction))

  producer.send(new ProducerRecord[Int, String](topicName, 10, "1,101,John  Smith,201,Pen,Stationery,Card,24,10,2021-01-10  10:12,India,Mumbai,www.amazon.com,36766,Y"))
  producer.send(new ProducerRecord[Int, String](topicName, 20, "2,102,Mary  Jane,202,Pencil,Stationery,Internet  Banking,36,5,2021-10-31 13:45,USA,Boston,www.flipkart.com,37167,Y"))
  producer.send(new ProducerRecord[Int, String](topicName, 30, "3,103,Joe Smith,203,Some mobile,Electronics,UPI,1,4999,2021-04-23 11:32,UK,Oxford,www.tatacliq.com,90383,Y"))
  producer.send(new ProducerRecord[Int, String](topicName, 40, "4,104,Neo,204,Some laptop,Electronics,Wallet,1,59999,2021-06-13 15:20,India,Indore,www.amazon.in,12224,N,Invalid  CVV."))
  producer.send(new ProducerRecord[Int, String](topicName, 50, "5,105,Trinity,205,Some book,Books,Card,1,259,2021-08-26  19:54,India,Bengaluru,www.ebay.in,99958,Y"))

  producer.flush()
}
