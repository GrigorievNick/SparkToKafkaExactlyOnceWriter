package mhr.spark.kafka.connector

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.StringSerializer

import java.io.OutputStream
import java.io.PrintWriter
import java.util.concurrent.TimeUnit
import java.util.Properties
import java.util.UUID
import scala.collection.JavaConverters._
import scala.collection.AbstractIterator
import scala.collection.Iterator

object KafkaSinkUtils {

  def writeTo(stream: OutputStream, body: String): Unit = {
    val pw = new PrintWriter(stream)
    try pw.write(body)
    finally pw.close()
  }

  def getLatestOffset(topic: String, partition: Int, bootstrapServers: String): Long = {
    val consumerClient = createKafkaConsumerClient(bootstrapServers)
    try {
      val topicPartition = new TopicPartition(topic, partition)
      consumerClient.endOffsets(List(topicPartition).asJavaCollection).get(topicPartition)
    } finally consumerClient.close()
  }

  def getLatestOffset(topic: String, bootstrapServers: String): Map[TopicPartition, Long] = {
    val consumerClient = createKafkaConsumerClient(bootstrapServers)
    val adminClient = createAdminClient(bootstrapServers)
    val topicPartitions = adminClient.describeTopics(List(topic).asJavaCollection)
      .all()
      .get(10, TimeUnit.MINUTES)
      .get(topic)
      .partitions()
    try {
      val partitions = topicPartitions.asScala
        .map(partInf => new TopicPartition(topic, partInf.partition()))
        .asJavaCollection
      consumerClient.endOffsets(partitions).asScala.mapValues(_.toLong).toMap
    } finally {
      adminClient.close()
      consumerClient.close()
    }
  }

  private def createAdminClient(bootstrapServers: String) = {
    val config = new Properties()
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    AdminClient.create(config)
  }

  private def createKafkaConsumerClient(bootstrapServers: String) = {
    val config = new Properties()
    val deserializer = (new ByteArrayDeserializer).getClass.getName
    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    config.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString)
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserializer)
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializer)
    config.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString)
    new KafkaConsumer[Byte, Byte](config)
  }

  def createKafkaProducerClient(bootstrapServers: String): KafkaProducer[String, String] = {
    val config = new Properties()
    val serializer = (new StringSerializer).getClass.getName
    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    config.put(ProducerConfig.RETRIES_CONFIG, "5")
    config.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1") // required for exactly once semantic
    config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true") // required for exactly once semantic
    config.put(ProducerConfig.ACKS_CONFIG, "all") // required for exactly once semantic
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, serializer)
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, serializer)
    new KafkaProducer[String, String](config)
  }

  /** Creates an infinite-length iterator returning values equally spaced apart.
    *
    * @param start the start value of the iterator
    * @return the iterator producing the infinite sequence of values `start, start + 1 , start + 2, ...`
    */
  def from(start: Long): Iterator[Long] =
    new AbstractIterator[Long] {
      private var i = start

      def hasNext: Boolean = true

      def next(): Long = {
        val result = i; i += 1; result
      }

    }

}
