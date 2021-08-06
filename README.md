# SparkToKafkaExactlyOnceWriter

Write to kafka from Spark with exactly-once delivery guarantee.

##### Solution

Kafka is a distributed log. Kafka grant that every new message produced to Kafka partition will have unique and **
sequential** id.\
Kafka offsets can be used as idempotent keys. In case every unique message will always be produced in the same Kafka
partition in the same order. \
So it will be possible: Pre calculates Kafka offset and drops all records which already in Kafka. \
To achieve this goal, several issues must be resolved:

* Every Spark Application Application must read input data source by batches(Spark Job) in sequential.
* Every Spark Job(batch) must **always** read the same dataset from the source.
* Spark Job(batch) must be able to identify Kafka topic latest partition->offset on every unique run.
* Source data must be deduplicated by the idempotent key.
* Every row must be bucketed to the same spark partition using the hash function. Spark Partition id must be aligned
  with the Kafka partition.
* Every Partition must be sorted by id.
* Every Spark Kafka Sink task must produce data:
    * To only one Kafka partition
    * Without reordering.

##### Implementation

* Spark Structured Streaming by default slice any source by idempotent batch.
    * Spark SS slice source on batches.
    * Store offset per batch to the special checkpoint, before computing batch.
    * Manage state per batch.
* On the start of every Spark Batch, get and persist Kafka topic latest offset and store to checkpoint per batch id, in
  the same way as to Spark SS.
* Repartition(bucketing) batch dataset by id with the same number of partitions as Kafka topic.
* Sort by id within the partition.
* Create one Kafka Producer per Spark Partition
* Configure Kafka Producer to use only one connection in flight.
    * For Kafka version less or equal to 10.0.x.x, configure retry to 0. Because
      of [KAFKA-3197](https://issues.apache.org/jira/browse/KAFKA-3197)
* On the start of every Sink to Kafka task get Kafka latest offset.
* Using initial Kafka offset from batch Start and Task Start - identify and skip already uploaded msg.
* Produce data to Kafka.

##### limitation

* To achieve an end to end Exactly Once semantic, the input data source must be deduplicated.
* In case source data stored on s3 - to achieve an end to end Exactly Once semantic, the input data source format must
  be able to handle eventually consistency issue.
* Any Update will be treated as a new record.
* Only one application must produce data to topic at same time. In case fail, no one else must not produce data to this
  topic, until retry attempt will be succeed
