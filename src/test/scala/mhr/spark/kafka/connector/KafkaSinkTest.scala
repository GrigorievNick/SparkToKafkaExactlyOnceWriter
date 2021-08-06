package mhr.spark.kafka.connector

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import mhr.spark.kafka.CommonTestsUtils
import mhr.spark.kafka.connector.KafkaSinks.IdempotentSinkToKafka
import org.apache.spark.TaskContext
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.CancelAfterFailure
import org.scalatest.concurrent.Eventually
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.util.Try

case class TestRow(id: Long, mockedValue: String)

trait KafkaSinkTest
    extends AnyFunSuite
    with CommonTestsUtils
    with BeforeAndAfterAll
    with Eventually
    with Matchers
    with CancelAfterFailure {

  private val basePath: String = s"/tmp/spark-to-kafka-test/"
  private val checkpointDir: String = s"$basePath/checkpoint"
  private val sourcePath = s"$basePath/input-data"
  private val targetTopic = s"files-to-kafka-test-by-${cleanseKafkaName(owner)}"
  private val localSparkSession = SparkSession.builder().master("local[*]").getOrCreate()
  private val idColumnName = "id"
  import localSparkSession.implicits._

  override def beforeAll(): Unit = {
    deleteIfExists(localSparkSession, basePath)
    createTopic(targetTopic, testKafkaUrl)
  }

  override def afterAll(): Unit = {
    deleteTopic(targetTopic, testKafkaUrl)
  }

  private val firstBatchInput = (0 to 10).map(index => TestRow(index, s"dummy$index")).toList
  private val secondBatchInput = (11 to 20).map(index => TestRow(index, s"dummy$index")).toList
  private val thirdBatchInput = (21 to 30).map(index => TestRow(index, s"dummy$index")).toList
  private val fourthBatchInput = (31 to 40).map(index => TestRow(index, s"dummy$index")).toList
  private val fifthBatchInput = (41 to 50).map(index => TestRow(index, s"dummy$index")).toList

  test("First batch write all data with exactly-once semantic") {
    // Prepare records
    prepareData(firstBatchInput)
    // run app
    runUploadToKafkaApp(config(targetTopic, checkpointDir, testKafkaUrl, sourcePath))
    // assert
    eventually {
      readFromKafka should contain theSameElementsAs firstBatchInput
    }
  }

  test("Second batch keep state and write all data with exactly-once semantic") {
    // Prepare records
    prepareData(secondBatchInput)
    // run app
    runUploadToKafkaApp(config(targetTopic, checkpointDir, testKafkaUrl, sourcePath))
    // assert
    val expectedResult = firstBatchInput ::: secondBatchInput
    eventually {
      readFromKafka should contain theSameElementsAs expectedResult
    }
  }

  test("retry, batch fail before write all data to kafka, data partially produced to kafka") {
    // Prepare records
    prepareData(thirdBatchInput)
    // emulate batch fail metadata
    Try {
      implicit val encoder: Encoder[TestRow] = Encoders.product[TestRow]
      localSparkSession
        .readStream
        .schema(encoder.schema)
        .parquet(sourcePath)
        .writeStream
        .option("checkpointLocation", checkpointDir)
        .foreachBatchNS { (df, batchId) =>
          val initialOffsetsByPartition = KafkaSinks.initializeOffsetForBatch(
            targetTopic,
            testKafkaUrl,
            checkpointDir,
            batchId,
            df.rdd.sparkContext.hadoopConfiguration
          )
          df.as[TestRow]
            .repartition(initialOffsetsByPartition.size, col(idColumnName))
            .sortWithinPartitions(col(idColumnName))
            .mapPartitions(_.take(3 + TaskContext.getPartitionId()))
            .sinkToKafka(targetTopic, testKafkaUrl, initialOffsetsByPartition)
          throw new Exception("Expected exception!")
        }
        .start()
        .processAllAvailable()
    }
    // run retry
    runUploadToKafkaApp(config(targetTopic, checkpointDir, testKafkaUrl, sourcePath))
    // assert
    val expectedResult = firstBatchInput ::: secondBatchInput ::: thirdBatchInput
    eventually {
      readFromKafka should contain theSameElementsAs expectedResult
    }
  }

  test("retry, batch fail after write all data to kafka, but before commit batch") {
    // Prepare records
    prepareData(fourthBatchInput)
    // emulate batch fail metadata
    Try {
      localSparkSession
        .readStream
        .schema(Encoders.product[TestRow].schema)
        .format("parquet")
        .option("path", sourcePath)
        .load()
        .writeStream
        .option("checkpointLocation", checkpointDir)
        .foreachBatchNS { (df, batchId) =>
          val initialOffsetsByPartition = KafkaSinks.initializeOffsetForBatch(
            targetTopic,
            testKafkaUrl,
            checkpointDir,
            batchId,
            df.rdd.sparkContext.hadoopConfiguration
          )
          df
            .repartition(initialOffsetsByPartition.size, col(idColumnName))
            .sortWithinPartitions(col(idColumnName))
            .sinkToKafka(targetTopic, testKafkaUrl, initialOffsetsByPartition)
          throw new Exception("Expected exception!")
        }
        .start()
        .processAllAvailable()
    }
    // run retry
    runUploadToKafkaApp(config(targetTopic, checkpointDir, testKafkaUrl, sourcePath))
    // assert
    val expectedResult = firstBatchInput ::: secondBatchInput ::: thirdBatchInput ::: fourthBatchInput
    eventually {
      readFromKafka should contain theSameElementsAs expectedResult
    }
  }

  test("process all available data with multiple batches") {
    // Prepare records
    prepareData(fifthBatchInput, 10)
    // run app
    runUploadToKafkaApp(config(targetTopic, checkpointDir, testKafkaUrl, sourcePath, 2))
    // assert
    val expectedResult =
      firstBatchInput ::: secondBatchInput ::: thirdBatchInput ::: fourthBatchInput ::: fifthBatchInput
    eventually {
      readFromKafka should contain theSameElementsAs expectedResult
    }
  }

  test("state management if no new data in source") {
    runUploadToKafkaApp(config(targetTopic, checkpointDir, testKafkaUrl, sourcePath))
    val expectedResult =
      firstBatchInput ::: secondBatchInput ::: thirdBatchInput ::: fourthBatchInput ::: fifthBatchInput
    eventually {
      readFromKafka should contain theSameElementsAs expectedResult
    }
  }

  def config(
      targetTopic: String,
      checkpointDir: String,
      bootstrapServers: String,
      sourcePath: String,
      maxFilesPerTrigger: Int = 200): Config =
    ConfigFactory.parseString(
      s"""
         |stream-uploading {
         |  source-config {
         |    format = "parquet"
         |    explicit-infer-schema = true
         |    options {
         |      path = "$sourcePath"
         |      maxFilesPerTrigger = $maxFilesPerTrigger
         |    }
         |  }
         |  bootstrap-servers = "$bootstrapServers"
         |  target-topic = "$targetTopic"
         |  checkpoint-dir = "$checkpointDir"
         |  id-column = "$idColumnName"
         |}
         |""".stripMargin
    )

  private def readFromKafka =
    localSparkSession
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", testKafkaUrl)
      .option("subscribe", targetTopic)
      .option("startingOffsets", "earliest")
      .load()
      .select(from_json($"value".cast("string"), Encoders.product[TestRow].schema).as("data"))
      .select($"data.*")
      .as[TestRow].collect()

  private def prepareData(expectedResult: Seq[TestRow], numOfFiles: Int = 2): Unit =
    expectedResult.toDS().repartition(numOfFiles).write.mode(SaveMode.Append).parquet(sourcePath)

}
