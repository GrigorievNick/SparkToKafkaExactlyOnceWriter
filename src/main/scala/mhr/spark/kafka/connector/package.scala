package mhr.spark.kafka

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.streaming.DataStreamWriter

package object connector {

  implicit class SparkDataWriterScalaSamWorkAround[T](writer: DataStreamWriter[T]) {

    /**
      * Support Scala Native function2 for foreachBatch, root cause
      * https://docs.databricks.com/release-notes/runtime/7.0.html#!#other-behavior-changes
      */
    def foreachBatchNS(function: (Dataset[T], Long) => Unit): DataStreamWriter[T] =
      writer.foreachBatch(function)

  }

}
