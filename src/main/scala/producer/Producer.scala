package producer

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import producer.Orders._

object Producer extends App {
  val props:Properties = new Properties()
  // Connecting to EC2
  props.put("bootstrap.servers", "ec2-3-93-174-172.compute-1.amazonaws.com:9092")
  props.put("key.serializer",
    "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer",
    "org.apache.kafka.common.serialization.StringSerializer")
  props.put("acks","all")
  val producer = new KafkaProducer[String, String](props)
  val topic = "team2DataTest"

  try {
    // for loop determines number of batches
    for (i <- 1 to 100) {
      val batch = createOrder(1000-1, i * 1000) // Number of orders (rows)
      println(s"Sending batch #$i ")
      Thread.sleep(1000) // for testing purposes, to give user time to read
      batch.foreach(x => {
        val split = x.split(",")
        val key = split(0).toString
        val record = new ProducerRecord[String, String](topic, key, x)
        val metadata = producer.send(record)
        // Display to console what is being sent to Kafka broker
        printf(s"Sent record(key=%s value=%s) " +
          "meta(partition=%d, offset=%d)\n",
          record.key(), record.value(),
          metadata.get().partition(),
          metadata.get().offset())
      })
      producer.flush()
    }
  } catch {
    case e: Exception =>  e.printStackTrace()
  } finally {
    producer.close()
  }
}
