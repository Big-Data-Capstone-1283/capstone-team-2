package producer

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import producer.Orders._

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

  try {
    // for loop determines number of batches
<<<<<<< Updated upstream
    for (i <- 1 to 2) {
      val batch = createOrders() // placeholder for data generation method
      println(s"Sending batch #$i ")
      Thread.sleep(2000) // for testing purposes, to give user time to read
      batch.foreach(x => {
        // REMINDER: change substring so that it pulls OrderID (I don't know how long it is currently)
        val record = new ProducerRecord[String, String](topic, x.substring(0, 10).toString, x)
=======
    for (i <- 1 to 100) {
      val batch = createOrder(1000-1, i*1000+1) // Number of orders (rows)
      // numOrders and startAt must be the same multiple of 10 (i.e. 1000 or 100) so there are no duplicate OrderIds
      println(s"Sending batch #$i ")
      Thread.sleep(500) // for testing purposes, to give user time to read
      batch.foreach(x => {
        val split = x.split(",")
        val key = split(0)
        val record = new ProducerRecord[String, String](topic, key, x)
>>>>>>> Stashed changes
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
