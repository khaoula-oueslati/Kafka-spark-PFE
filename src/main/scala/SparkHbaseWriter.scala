import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{Dataset, ForeachWriter, Row, SparkSession}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.util.Bytes

object SparkHbaseWriter {
  //Initialize SparkSession
  val spark: SparkSession = SparkSession.builder().appName("Application").master("local[*]").getOrCreate()
  var i = 9
  //Spark structured Streaming
  val streaminData = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "insurance-topic") //"insurance-topic"
    //Reading data From starting
    .option("startingOffsets", "latest")  //For reading only the new data, use "latest"
    //earliest
    .load()

  //Select Stream Value
  val streamingDataValue : Dataset[Row] = streaminData.selectExpr("CAST(value AS STRING)")

  //Hbase Writer
  def hbaseWriter():  ForeachWriter[Row] ={
    val writer = new ForeachWriter[Row] {
      var hbaseConfig: Configuration = _
      var connection: Connection = _
      override def open(partitionId: Long, epochId: Long): Boolean = {
        hbaseConfig = HBaseConfiguration.create()
        hbaseConfig.set("hbase.zookeeper.quorum", "localhost")
        hbaseConfig.set("hbase.zookeeper.quorum.clientPort", "2181")
        connection = ConnectionFactory.createConnection(hbaseConfig)
        true
      }

      override def process(value: Row): Unit = {
        val textValue = value.toString().split(";")
        val valeur : String = textValue(0).toString
        //val key : String = textValue(1).toString
        //register table (Hbase connection for the table)
        val table = connection.getTable(TableName.valueOf("insurance-names-table")) //""
        val row = new Put(Bytes.toBytes("row1"))
        //add columns
        row.addColumn(
          Bytes.toBytes("groupe1"), Bytes.toBytes("col"+ i), Bytes.toBytes(valeur)
        )
        table.put(row)
        i = i + 1
      }
      override def close(errorOrNull: Throwable): Unit = {
        connection.close()
      }
    }
    writer
  }

  // Save HbaseWriter
  def saveHbaseWriter(streamingDataValue: Dataset[Row], writer: ForeachWriter[Row]): Unit ={
    streamingDataValue
      .writeStream
      .foreach(writer)
      .outputMode("update")
      //.trigger(Trigger.ProcessingTime("20 seconds"))
      .start
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    //import spark.implicits._
//      val writer = hbaseWriter()
//        saveHbaseWriter(streamingDataValue, writer)



    //
//          streamingDataValue
//          .writeStream
//          .foreach(writer)
//          .outputMode("update")
//          //.trigger(Trigger.ProcessingTime("20 seconds"))
//          .start
//          .awaitTermination()

//To Print the stream "console"  Format
//    streaminData.writeStream
//          .format("console")
//          .option("truncate","false")
//          .start()
//          .awaitTermination()


  }
}
