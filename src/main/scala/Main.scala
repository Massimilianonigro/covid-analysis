import org.apache.spark.sql.catalyst.util.DateFormatter
import org.apache.spark.sql.{
  Column,
  ColumnName,
  Dataset,
  Row,
  SparkSession,
  functions
}
import org.apache.spark.sql.functions.{col, lag, min}

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.ArrayType

import java.text.SimpleDateFormat
import java.time.temporal.ChronoUnit.DAYS
import java.util.Base64.Encoder
import java.util.Date
import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, ExecutionContext, Future}

object Main {

  def main(args: Array[String]): Unit = {

    // initialize session
    val spark = SparkSession
      .builder()
      .appName("Covid-Analysis")
      .config("spark.master", "local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    // read dataset
    var df = spark.read.csv(
      "./resources/data.csv"
    )
    df = PreprocessingHandler.dfPreprocessing(df)
    var session = spark.sqlContext.sparkSession
    df.createOrReplaceTempView("df")
    var countries = CountryHandler.getCountries(df)
    var views = scala.collection.concurrent.TrieMap.empty[String, Dataset[Row]]
    var dateRange =
      scala.collection.concurrent.TrieMap.empty[String, (String, String)]
    var executor =
      Executors.newFixedThreadPool(Runtime.getRuntime.availableProcessors())
    var ec: ExecutionContext = ExecutionContext.fromExecutor(executor)
    var country_str: String = ""
    val results: Seq[Future[Int]] =
      countries.map(country => {
        Future {
          country_str = CountryHandler.getCountryName(country)
          print("\nHandling country: " + country_str)
          views(country_str) = CountryHandler.getCountryView(country, session)
          views(country_str).withColumn("cases", col("cases").cast("Double"))
          views(country_str) =
            CountryHandler.fillMissingDates(views(country_str), session)
          dateRange(country_str) = (
            views(country_str)
              .select(functions.max("dateRep"))
              .collect()(0)(0)
              .toString,
            views(country_str)
              .select(functions.min("dateRep"))
              .collect()(0)(0)
              .toString
          )
          views(country_str) =
            StatisticsHandler.calculateMovingAverage(views(country_str))
          views(country_str) = StatisticsHandler
            .calculatePercentageIncrease(views(country_str), session)
          print("\nReturning from country: " + country_str)
          0
        }(ec)
      })
    var allDone: Future[Seq[Int]] = Future.sequence(results)
    //wait for results
    Await.result(allDone, scala.concurrent.duration.Duration.Inf)
    executor.shutdown() //otherwise jvm will probably not exit
    var reportingInterval = StatisticsHandler.getReportingInterval(dateRange)
    var minMax = Seq
      .empty[String]
      .toDS()
      .withColumnRenamed("value", "day")
      .withColumn("top_ten", lit(null: ArrayType))
    val daysCount = DAYS.between(
      reportingInterval._2.toInstant,
      reportingInterval._1.toInstant
    )
    var topTen = TopTen
    val pattern = "yyyy-MM-dd"
    val simpleDateFormat = new SimpleDateFormat(pattern)
    executor =
      Executors.newFixedThreadPool(Runtime.getRuntime.availableProcessors())
    ec = ExecutionContext.fromExecutor(executor)
    (0 until daysCount.toInt)
      .map(days => reportingInterval._2.toInstant.plus(days, DAYS))
      .map(day => {
        Future {
          val dateDay = simpleDateFormat.format(Date.from(day))
          print("\n \n Day is now " + dateDay)
          var countries =
            StatisticsHandler.reportingCountries(dateRange, dateDay)
          countries.foreach(country => {
            topTen.add(
              country,
              views(country)
                .select("perc_increase")
                .filter(views(country)("dateRep") === dateDay)
                .collect()(0)(0)
                .toString
                .toDouble
            )
          })
          //At the end of the day the topTen list has to be added to the minMax dataset
          var rowToAdd = Seq(
            (dateDay, topTen.getCountries)
          ).toDF("day", "top_ten")
          minMax = minMax.union(rowToAdd)
          topTen.clear()
          0
        }(ec)
      })
    allDone = Future.sequence(results)
    //wait for results
    Await.result(allDone, scala.concurrent.duration.Duration.Inf)
    executor.shutdown() //otherwise jvm will probably not exit
    minMax.show(100)
    //views("Afghanistan").show(500, truncate = false)
    //views.values.foreach(view => view.show())
  }
}
