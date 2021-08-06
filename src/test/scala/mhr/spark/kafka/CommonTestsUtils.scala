package mhr.spark.kafka

import com.typesafe.config.Config
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.spark.sql.SparkSession

import java.lang.System.getenv
import java.util.Properties
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._

trait CommonTestsUtils {

  protected val owner: String = Option(getenv("CI_JOB_ID"))
    .map(List("Gitlab", getenv("CI_PROJECT_PATH"), getenv("CI_COMMIT_REF_NAME"), _).mkString("_"))
    .getOrElse(getenv("USER"))

  protected val testKafkaUrl = "kafka:9092,kafka:9092,kafka:9092"

  protected def cleanseKafkaName(name: String): String = name.replaceAll("_", "-").replaceAll("/", "-")

  protected def createTopic(topic: String, bootstrapServers: String): Unit = {
    val admin: AdminClient = adminClient(bootstrapServers)
    try {
      def topics = admin.listTopics().names().get(1, TimeUnit.MINUTES)
      if (topics.contains(topic)) {
        admin.deleteTopics(List(topic).asJavaCollection).all().get(1, TimeUnit.MINUTES)
        while (topics.contains(topic)) Thread.sleep(500)
      }
      admin.createTopics(List(new NewTopic(topic, 2, 3.toShort)).asJavaCollection)
        .all()
        .get(1, TimeUnit.MINUTES)
    } finally admin.close()
  }

  protected def deleteTopic(topic: String, bootstrapServers: String): Unit = {
    val admin: AdminClient = adminClient(bootstrapServers)
    try {
      val topics = admin.listTopics().names().get(1, TimeUnit.MINUTES)
      if (topics.contains(topic))
        admin.deleteTopics(List(topic).asJavaCollection).all().get(1, TimeUnit.MINUTES)
    } finally admin.close()
  }

  protected def adminClient(bootstrapServers: String): AdminClient = {
    val config = new Properties()
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    AdminClient.create(config)
  }

  protected def deleteIfExists(spark: SparkSession, path: String): Unit = {
    val location: Path = new Path(path)
    val fs = FileSystem.newInstance(location.toUri, spark.sparkContext.hadoopConfiguration)
    if (fs.exists(location)) fs.delete(location, true)
    fs.close()
  }

  protected def runUploadToKafkaApp(config: Config): Unit

}
