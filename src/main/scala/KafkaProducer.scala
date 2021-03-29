import java.util.Properties
import java.util.concurrent.TimeUnit
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer
import scala.io.Source

object KafkaProducer {
  val bootstrapServers = "localhost:9092"
  val zookeeperServers = "localhost:2181"
  //val topicName = "insurance-topic"
  def createProducer(topicName: String) = {
    val props = new Properties()
    props.put("client.id", topicName)
    props.put("linger.ms", "0")
    props.put("request.timeout.ms", "10000")
    props.put("bootstrap.servers", bootstrapServers)
    props.put("key.serializer", classOf[StringSerializer].getCanonicalName)
    props.put("value.serializer", classOf[StringSerializer].getCanonicalName)
    new KafkaProducer[String, String](props)
  }
  //Source Files
  def readFiles(path: String): Iterator[String] ={
    Source.fromFile(path).getLines().drop(1) //drop the column names
  }
  //Send Data
  def sendData(topicName: String,entries: Iterator[String], producer: KafkaProducer[String, String]  ): Unit ={
    entries.foreach{ line =>
      val data = new ProducerRecord[String, String](topicName, line)
      producer.send(data, new Callback {
        override def onCompletion(metadata: RecordMetadata, exception: Exception) {
          Option(exception).foreach(_.printStackTrace())
          Option(metadata).foreach(m => println(s"sent[${m.offset()}]= $line"))
        }
      })
    }
  }
  //Close Producer
  def closeProducer(producer: KafkaProducer[String, String]): Unit ={
    producer.close(10000, TimeUnit.SECONDS)
  }
}
