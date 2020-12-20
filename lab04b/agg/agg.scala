import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger

object agg {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("lab04b")
      .getOrCreate

    // ------------------------------ Настройки Kafka

    val kafkaServer = "spark-master-1:6667"
    val kafkaTopicIn = "lev_osipov"
    val kafkaTopicOut = "lev_osipov_lab04b_out"
    val kafkaChk = "lab04b_chk_lo3"

    val schema = StructType(Seq(
      StructField("event_type", StringType, nullable = true),
      StructField("category", StringType, nullable = true),
      StructField("item_id", StringType, nullable = true),
      StructField("item_price", IntegerType, nullable = true),
      StructField("uid", StringType, nullable = true),
      StructField("timestamp", LongType, nullable = true)
    ))

    // ------------------------------ Настройка чтения

    val agg = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServer)
      .option("subscribe", kafkaTopicIn)
      .option("failOnDataLoss", value = false)
      .load
      .withWatermark("timestamp", "1 hour")
      .withColumn("valueParsed", from_json(col("value").cast("string"), schema))
      .select(col("valueParsed.*"), col("timestamp").alias("msg_ts"))
      .dropDuplicates(Seq("uid", "msg_ts"))
      .groupBy(window(to_timestamp(col("timestamp") / 1000), "1 hour", "1 hour"))
      .agg(expr("SUM(CASE WHEN event_type = 'buy' THEN item_price ELSE 0 END) AS revenue"),
        count(col("uid")).alias("visitors"),
        expr("SUM(CASE WHEN event_type = 'buy' THEN 1 ELSE 0 END) AS purchases")
      )
      .select(lit(null).cast(StringType), to_json(struct(
        col("window.start").cast(LongType).alias("start_ts"),
        col("window.end").cast(LongType).alias("end_ts"),
        col("revenue"),
        col("visitors"),
        col("purchases"),
        (col("revenue") / col("purchases")).alias("aov")
      )).alias("value"))

    // ------------------------------ Настройка записи

    val kafkaSink = agg
      .writeStream
      .format("kafka")
      .outputMode("update")
      .option("kafka.bootstrap.servers", kafkaServer)
      .option("topic", kafkaTopicOut)
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .option("checkpointLocation", s"chk/$kafkaChk")

    // ------------------------------ Запуск и ожидание окончания

    kafkaSink.start.awaitTermination

    // ------------------------------ Освобождение ресурсов

    spark.stop
  }
}
