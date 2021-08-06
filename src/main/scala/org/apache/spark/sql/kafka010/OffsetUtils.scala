package org.apache.spark.sql.kafka010

import org.apache.kafka.common.TopicPartition

object OffsetUtils {

  def fromJson(jsonOffsetString: String): Map[TopicPartition, Long] = JsonUtils.partitionOffsets(jsonOffsetString)

  def toJson(partitionOffsets: Map[TopicPartition, Long]): String = JsonUtils.partitionOffsets(partitionOffsets)

}
