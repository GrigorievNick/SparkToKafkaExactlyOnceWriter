package mhr.spark.kafka

import com.typesafe.config.Config
import mhr.spark.kafka.IngestionToKafka.ApplicationConfig
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader._
import net.ceedubs.ficus.readers.namemappers.implicits.hyphenCase
import org.apache.spark.SparkConf

trait RunLocally extends CommonTestsUtils {

  override def runUploadToKafkaApp(config: Config): Unit = {
    val appConfig = config.as[ApplicationConfig]("stream-uploading")
    val sparkConf = new SparkConf().setAppName("it-files-to-kafka").setMaster("local[*]")
    IngestionToKafka.runApp(appConfig, sparkConf)
  }

}
