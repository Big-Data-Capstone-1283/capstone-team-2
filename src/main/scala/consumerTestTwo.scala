import java.time.Duration
import java.util.Properties
import org.apache.kafka.clients.consumer.ConsumerConfig._
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.{IntegerDeserializer, StringDeserializer}
import java.io._
import scala.collection.JavaConverters._

object consumerTestTwo extends App{
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
  //to check running topics
  //use bin/./kafka-topics.sh --list --bootstrap-server 172.18.217.222:9092
  //you should see sql_dolphins2
  //or cd to bin, then run bin/./kafka-topics.sh --list --bootstrap-server localhost:9092
  //ssh -i William-Big_Data-1283.pem ec2-user@ec2-3-93-174-172.compute-1.amazonaws.com

  //DELETE topic from EC2
  //  bin/kafka-topics.sh --delete --topic doug_test --bootstrap-server localhost:9092

  //List topics in EC2
  //  bin/kafka-topics.sh --list --bootstrap-server localhost:9092


  val topicName = "doug_test2"
  val consumerProperties = new Properties()

  //consumerProperties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  //----------MY IP ADDRESS WILL CHANGE ON COMPUTER RESTART/ TURN OFF TURN ON; CHECK IN WSL/UBUNTU USING: hostname -I          ----------
  //-----AMAZON EC2 IP ADDRESS = 3.93.174.172 -----
  consumerProperties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "ec2-3-93-174-172.compute-1.amazonaws.com:9092")
  //consumerProperties.setProperty(GROUP_ID_CONFIG, "group-id-2")
  consumerProperties.setProperty(GROUP_ID_CONFIG, "test")
  //  consumerProperties.setProperty(AUTO_OFFSET_RESET_CONFIG, "latest")

  //must match producer TYPES
  consumerProperties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  //must match producer TYPES;
  consumerProperties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  //  consumerProperties.setProperty(ENABLE_AUTO_COMMIT_CONFIG, "false")
  val consumer = new KafkaConsumer[String, String](consumerProperties)
  consumer.subscribe(List(topicName).asJava)
  //CREATE AND OPEN FILE WRITER
  val fileObject = new File("consumerOutput/transactions.csv")
  val printWriter = new PrintWriter(new FileOutputStream(fileObject))
  val polledRecords: ConsumerRecords[String, String] = consumer.poll(Duration.ofSeconds(50))



  if (!polledRecords.isEmpty) {
    println(s"Polled ${polledRecords.count()} records")
  }
  val recordIterator = polledRecords.iterator()
  //

  while (recordIterator.hasNext) {
    val record: ConsumerRecord[String, String] = recordIterator.next()
    println(s"| ${record.key()} | ${record.value()} ") //| ${record.partition()} | ${record.offset()} |")
    //WRITE THE KAFKA STREAM TO THE FILE
    printWriter.write(record.value() + "\n")

    printWriter.flush()
  }



}
