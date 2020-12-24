import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.collection.mutable.Set

object users_items {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("lab05")
      .getOrCreate

    import spark.implicits._
    
    // ------------------------------ Параметры

    val inputDir = spark.conf.get("spark.users_items.input_dir")
    val outputDir = spark.conf.get("spark.users_items.output_dir")
    val update = spark.conf.get("spark.users_items.update")

    // ------------------------------ Чтение логов из lab04a

    val views = spark
      .read
      .json(inputDir + "/view")
    val buys = spark
      .read
      .json(inputDir + "/buy")

    // ------------------------------ Определение последней даты

    val logs = views.union(buys)
    val maxDate = logs.agg(max(col("p_date"))).as[Integer].collect()(0)

    // ------------------------------ Pivot и соединение просмотров и покупок

    val exprGetItemName = "regexp_replace(lower(item_id), '-| ', '_')"
    val viewsAgg = views
      .groupBy(col("uid"))
      .pivot(expr("concat('view_', " + exprGetItemName + ")"))
      .count
    val buysAgg = buys
      .groupBy(col("uid"))
      .pivot(expr("concat('buy_', " + exprGetItemName + ")"))
      .count

    // var, так как для работы с дубликатами необходимы динамические изменения
    var userItems = viewsAgg.join(buysAgg, Seq("uid"), "outer")

    // ------------------------------ Обновление данных, если update = 1

    if (update == "1") {
      // Чтение старой матрицы
      // var, так как для работы с дубликатами необходимы динамические изменения
      var userItemsOld = spark.read.parquet(s"$outputDir/20200429")

      // Формирование множеств наименований колонок двух таблиц
      val leftItemCols = (Set() ++ userItems.columns) -= "uid"
      val rightItemCols = (Set() ++ userItemsOld.columns) -= "uid"
      val itemCols = leftItemCols ++ rightItemCols

      // Добавление недостающих колонок с null-значениями
      (itemCols -- leftItemCols).foreach(x => userItems = userItems.withColumn(x, lit(null)))
      (itemCols -- rightItemCols).foreach(x => userItemsOld = userItemsOld.withColumn(x, lit(null)))

      // Выборка колонок в одинаковом порядке для корректного union
      val allItemColsOrdered = itemCols.toSeq.sorted
      userItems = userItems.select((Seq("uid") ++ allItemColsOrdered).map(col): _*)
      userItemsOld = userItemsOld.select((Seq("uid") ++ allItemColsOrdered).map(col): _*)

      // Соединение, агрегация и суммирование
      userItems = userItems
        .union(userItemsOld)
        .groupBy(col("uid"))
        // При суммировании null обрабатываются нормально
        // Колонка foo_mock нужна только чтобы попасть в сигнатуру метода agg, просто :_* сделать не позволяет
        .agg(lit(0).alias("foo_mock"), allItemColsOrdered.map(x => sum(col(x)).alias(x)): _*)
        .drop(col("foo_mock"))
    }

    // ------------------------------ Очистка данных - 0 вместо null

    val userItemsClean = userItems.na.fill(0, userItems.columns)

    // ------------------------------ Запись в файл

    userItemsClean
      .repartition(200)
      .write
      .mode("append")
      .parquet(s"$outputDir/$maxDate")

    // ------------------------------ Освобождение ресурсов

    spark.stop
  }
}