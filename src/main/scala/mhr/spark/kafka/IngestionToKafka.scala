package mhr.spark.kafka

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import mhr.spark.kafka.connector.KafkaSinks._
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader._
import net.ceedubs.ficus.readers.namemappers.implicits.hyphenCase
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.streaming.DataStreamReader
import org.apache.spark.sql.DataFrameReader
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

object IngestionToKafka extends Logging {

  val PartitionToProduce = "partition"

  case class SourceConfig(format: String, explicitInferSchema: Boolean, options: Config)

  case class ApplicationConfig(
      sourceConfig: SourceConfig,
      idColumn: String,
      bootstrapServers: String,
      targetTopic: String,
      checkpointDir: String)

  def main(args: Array[String]): Unit = {
    val appConfig = ConfigFactory.load().as[ApplicationConfig]("stream-uploading")
    val sparkConf = new SparkConf()
    sparkConf.registerKryoClasses(Array(ApplicationConfig.getClass))
    logInfo("Start Stream %s\n%s".format(appConfig, sparkConf))

    runApp(appConfig, sparkConf)
  }

  val MsgToProduce = "value"

  def runApp(appConfig: ApplicationConfig, sparkConf: SparkConf): Unit = {
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val streamReader = {
      val builder = createSparkSourceFrom(appConfig.sourceConfig, sparkSession.readStream)
      if (appConfig.sourceConfig.explicitInferSchema) {
        val schema = createSparkSourceFrom(appConfig.sourceConfig, sparkSession.read).load().schema
        builder.schema(schema)
      } else builder
    }

    val stream = streamReader
      .load()
      .writeStream
      .option("checkpointLocation", appConfig.checkpointDir)
      .exactlyOnceToKafka(
        appConfig.targetTopic,
        appConfig.bootstrapServers,
        appConfig.checkpointDir,
        appConfig.idColumn
      )
      .start()
    stream.processAllAvailable()
    logInfo(s"All data processed!")
    stream.stop()
    stream.awaitTermination()
    logInfo(s"Shutdown Stream $appConfig!")
  }

  private def createSparkSourceFrom[T](sourceConfig: SourceConfig, sparkSource: DataFrameReader) =
    sourceConfig.options
      .entrySet()
      .asScala
      .foldLeft(sparkSource.format(sourceConfig.format)) {
        case (streamBuilder, entry) =>
          streamBuilder.option(entry.getKey, sourceConfig.options.getString(entry.getKey))
      }

  private def createSparkSourceFrom[T](sourceConfig: SourceConfig, sparkSource: DataStreamReader) =
    sourceConfig.options
      .entrySet()
      .asScala
      .foldLeft(sparkSource.format(sourceConfig.format)) {
        case (streamBuilder, entry) =>
          streamBuilder.option(entry.getKey, sourceConfig.options.getString(entry.getKey))
      }

}
