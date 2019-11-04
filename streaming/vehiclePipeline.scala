import kafka.serializer.StringDecoder
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{kafka, _}
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils


object vehiclePipeline {
  val localLogger = Logger.getLogger("vehiclePipelineLogger")

  def main(args: Array[String]) {

    // update
    // val checkpointDir = "./tmp"

    val sparkConf = new SparkConf().setAppName("vehiclePipeline")
    sparkConf.setIfMissing("spark.master", "local[*]")
    //    sparkConf.setIfMissing("spark.checkpoint.dir", checkpointDir)
    //sparkConf.setIfMissing("spark.cassandra.connection.host", "127.0.0.1")

    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val kafkaTopics = "vehicleScraper"
    val kafkaBroker = "127.0.01:9092"

    //val cassandraKeyspace = "vehicle_data"
    //val cassandraTableRaw = "raw_vehicle_data"

    val topics: Set[String] = kafkaTopics.split(",").map(_.trim).toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> kafkaBroker)

    localLogger.info(s"connecting to brokers: $kafkaBroker")
    localLogger.info(s"kafkaParams: $kafkaParams")
    localLogger.info(s"topics: $topics")

    val vehicleStream : InputDStream[(String, String)] =
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    val parsedVehicleStream = vehicleStream.map(_._2.split(",")).map(print(_))
    parsedVehicleStream.print()


    vehicleStream
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}

