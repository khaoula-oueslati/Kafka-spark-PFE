import KafkaProducer.{closeProducer, createProducer, readFiles, sendData}
import SparkHbaseWriter.{hbaseWriter, saveHbaseWriter, streamingDataValue}

object Main {
  def main(args: Array[String]): Unit = {
    // kafka
    /////////////////////////////////////////////////////
    val entries = readFiles("C:\\Users\\lenovo\\Desktop\\insurance&BigData\\automobile-insurance-company.csv")
    val producer = createProducer("insurances-names-topic")
    println("Kafka Producer is RUNNING")
    sendData("insurances-names-topic",entries, producer)
    println("sent")
    closeProducer(producer)
    println("Kafka Producer CLOSED")
    /////////////////////////////////////////////////////
    // Spark Streaming + Hbase
    val writer= hbaseWriter()
    saveHbaseWriter(streamingDataValue, writer)
    /////////////////////////////////////////////////////
  }
}
