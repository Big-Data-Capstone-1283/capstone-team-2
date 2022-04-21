package producer

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

class Producer extends App{
  val props:Properties = new Properties()
  //Replace ip with your ip from server.config
  props.put("bootstrap.servers","ec2-3-93-174-172.compute-1.amazonaws.com:9092")
  props.put("key.serializer",
    "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer",
    "org.apache.kafka.common.serialization.StringSerializer")
  props.put("acks","all")
  val producer = new KafkaProducer[String, String](props)
  val topic = "team2"


}
