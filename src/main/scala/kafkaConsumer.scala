import java.util.{Collections, Properties}
import KafkaProducer.bootstrapServers
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import scala.collection.convert.ImplicitConversions.`iterable AsScalaIterable`

object kafkaConsumer {
  val topicName = ""
  def createConsumer() = {
    val props = new Properties()
    props.put("group.id", "insurance-topic")
    props.put("bootstrap.servers", bootstrapServers)
    props.put("key.deserializer", classOf[StringDeserializer].getCanonicalName)
    props.put("value.deserializer", classOf[StringDeserializer].getCanonicalName)
    new KafkaConsumer[String, String](props)
  }
  //subscribe the consumer
  def subscribeConsumer(consumer:  KafkaConsumer[String, String]): Unit ={
    consumer.subscribe(Collections.singletonList(topicName))
  }

  //get records
  def getRecords(consumer: KafkaConsumer[String, String]): Unit ={
    (1 to 100).foreach { i =>
      val records = consumer.poll(1000)
      println(s"got ${records.count()} records ...")
      records.map { record =>
        println(record.value())
      }
    }
  }
  //close Consumer
  def closeRecord(consumer: KafkaConsumer[String, String]): Unit ={
    consumer.close()
  }

  def main(args: Array[String]): Unit = {
    val consumer = createConsumer()
//    consumer.subscribe(Collections.singletonList(topicName))
//    println("consumer has been subscribed ...")
//
//    (1 to 100).foreach { i =>
//      val records = consumer.poll(1000)
//      println(s"got ${records.count()} records ...")
//      records.map { record =>
//        println(record.value())
//
//      }
//
//    }
//
//
//    println("done")
//    consumer.close()
  }

}
