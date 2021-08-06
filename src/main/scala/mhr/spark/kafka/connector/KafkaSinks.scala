package mhr.spark.kafka.connector

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.execution.streaming.StreamExecution
import org.apache.spark.sql.functions._
import org.apache.spark.sql.kafka010.OffsetUtils
import org.apache.spark.sql.streaming.DataStreamWriter

import scala.io.Source

object KafkaSinks extends Logging {

  implicit class DataStreamExactlyOnceSinkToKafka[T](dataStreamWriter: DataStreamWriter[T]) {

    def exactlyOnceToKafka(
        topic: String,
        bootstrapServers: String,
        checkpointDir: String,
        idColumnName: String): DataStreamWriter[T] =
      dataStreamWriter.foreachBatchNS { (dataset, batchId) =>
        logInfo(
          "Sink to Kafka: QueryId %s BatchId %s"
            .format(dataset.sqlContext.sparkContext.getLocalProperty(StreamExecution.QUERY_ID_KEY), batchId)
        )
        val initialOffsetsByPartition = initializeOffsetForBatch(
          topic,
          bootstrapServers,
          checkpointDir,
          batchId,
          dataset.sparkSession.sparkContext.hadoopConfiguration
        )
        dataset
          .repartition(initialOffsetsByPartition.size, col(idColumnName))
          .sortWithinPartitions(col(idColumnName))
          .sinkToKafka(topic, bootstrapServers, initialOffsetsByPartition)
      }

  }

  implicit class IdempotentSinkToKafka[T](dataset: Dataset[T]) {

    def sinkToKafka(
        topic: String,
        bootstrapServers: String,
        initialOffsetsByPartition: Map[TopicPartition, Long]): Unit = {
      dataset
        .toJSON
        .foreachPartition { it: Iterator[String] =>
          if (it.nonEmpty) {
            val partition = TaskContext.getPartitionId()
            val initialOffset = initialOffsetsByPartition(new TopicPartition(topic, partition))
            val currentOffset = KafkaSinkUtils.getLatestOffset(topic, partition, bootstrapServers)
            logInfo(
              s"Start upload data to %s-%s. Initial batch offset %s, current offset %s."
                .format(topic, partition, initialOffset, currentOffset)
            )
            val producer = KafkaSinkUtils.createKafkaProducerClient(bootstrapServers)
            try it
              .zip(KafkaSinkUtils.from(initialOffset))
              .drop((currentOffset - initialOffset).toInt)
              .map {
                case (jsonMsgToSend, offset) =>
                  logDebug(s"KPartition $partition offset $offset -> $jsonMsgToSend")
                  val msg = new ProducerRecord[String, String](topic, partition, null, jsonMsgToSend)
                  producer.send(msg) -> offset
                // TODO calculate batch size base on Size of Serialized Message and producer -> batch.size
              }.grouped(100)
              .foreach { kafkaResultsWIthExpectedOffset =>
                kafkaResultsWIthExpectedOffset.reverse.foreach {
                  case (kafkaResult, expectedOffset) =>
                    assert(
                      kafkaResult.get().offset() == expectedOffset,
                      "Offset from kafka %s Expected offset %s".format(kafkaResult.get().offset(), expectedOffset)
                    )
                }
              } finally {
              producer.flush()
              producer.close()
            }
          }
        }
    }

  }

  private[connector] def initializeOffsetForBatch(
      topic: String,
      bootstrapServers: String,
      checkpointDir: String,
      batchId: Long,
      configuration: Configuration) = {
    //  TODO make file consistent with Structure Streaming metadata-log.
    val kafkaMetadataPath = new Path(s"$checkpointDir/kafka-target-topic-$topic-offsets//$batchId")
    val fs = FileSystem.get(kafkaMetadataPath.toUri, configuration)
    if (fs.exists(kafkaMetadataPath)) {
      val offset = OffsetUtils.fromJson(Source.fromInputStream(fs.open(kafkaMetadataPath)).mkString)
      logInfo(
        "One more attempt upload of batch %s to %s on server %s\nstored to %s\ninitial offsets %s"
          .format(batchId, topic, bootstrapServers, kafkaMetadataPath, offset)
      )
      offset
    } else {
      val offset = KafkaSinkUtils.getLatestOffset(topic, bootstrapServers)
      logInfo(
        "First attempt upload of batch %s to %s on server %s\nstored to %s\ninitial offsets %s"
          .format(batchId, topic, bootstrapServers, kafkaMetadataPath, offset)
      )
      KafkaSinkUtils.writeTo(fs.create(kafkaMetadataPath), OffsetUtils.toJson(offset))
      offset
    }
  }

}
