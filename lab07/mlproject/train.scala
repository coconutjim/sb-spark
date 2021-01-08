import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{CountVectorizer, StringIndexer, IndexToString}
import org.apache.spark.ml.{Pipeline, PipelineModel}

object train {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("lab07_train")
      .getOrCreate

    val trainPath = spark.conf.get("spark.mlproject.train_path") // "/labs/laba07/laba07.json"
    val modelPath = spark.conf.get("spark.mlproject.model_path") // "/user/lev.osipov/laba07/model/"
    val maxIter = spark.conf.get("spark.mlproject.max_iter") // 10
    val regParam = spark.conf.get("spark.mlproject.reg_param") // 0.001

    // ------------------------------ Чтение и парсинг логов

    val exprGetDomain = "regexp_replace(parse_url(visits.url, 'HOST'), '^www.', '')"

    val logs = spark
      .read
      .json(trainPath)
      .select(col("uid"), col("gender_age"), explode(col("visits")).alias("visits"))
      .withColumn("domain", expr(exprGetDomain))
      .groupBy(col("uid"), col("gender_age"))
      .agg(collect_list("domain").alias("domains"))

    // ------------------------------ Подготовка параметров для обучения модели

    val cv = new CountVectorizer()
      .setInputCol("domains")
      .setOutputCol("features")

    val indexer = new StringIndexer()
      .setInputCol("gender_age")
      .setOutputCol("label")
      .fit(logs)

    val lr = new LogisticRegression()
      .setMaxIter(maxIter.toInt)
      .setRegParam(regParam.toDouble)

    val converter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("label_string")
      .setLabels(indexer.labels)

    val pipeline = new Pipeline()
      .setStages(Array(cv, indexer, lr, converter))

    // ------------------------------ Обучение и сохранение модели

    val model = pipeline.fit(logs)
    model.write.overwrite.save(modelPath)

    // ------------------------------ Освобождение ресурсов

     spark.stop
  }
}