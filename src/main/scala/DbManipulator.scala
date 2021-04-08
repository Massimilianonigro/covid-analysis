import org.apache.spark.sql
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession, functions}

import java.text.SimpleDateFormat
import java.time.temporal.ChronoUnit.DAYS
import java.util.Date
import java.util.concurrent.{ExecutorService, Executors}
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, ExecutionContext, Future}

class DbManipulator(df: sql.DataFrame, sparkSession: SparkSession) {

  var executor: ExecutorService =
    Executors.newFixedThreadPool(Runtime.getRuntime.availableProcessors())

  var ec: ExecutionContext = ExecutionContext.fromExecutor(executor)
  val session: SparkSession = sparkSession.sqlContext.sparkSession

  import sparkSession.implicits._

  def computeMovingAverageAndPercentageIncrease(): (
      mutable.Map[String, Dataset[Row]],
      mutable.Map[String, (String, String)]
  ) = {

    val views: mutable.Map[String, Dataset[Row]] =
      scala.collection.concurrent.TrieMap.empty[String, Dataset[Row]]
    val dateRange: mutable.Map[String, (String, String)] =
      scala.collection.concurrent.TrieMap.empty[String, (String, String)]
    var country_str: String = ""
    df.show(200, truncate = true)
    val countries: Array[Row] = CountryHandler.getCountries(df)
    val results: Seq[Future[Int]] =
      countries.map(country => {
        Future {
          country_str = CountryHandler.getCountryName(country)
          print("\nHandling country: " + country_str)
          views(country_str) = CountryHandler.getCountryView(country, session)
          views(country_str) =
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
    val allDone: Future[Seq[Int]] = Future.sequence(results)
    //wait for results
    Await.result(allDone, scala.concurrent.duration.Duration.Inf)
    (views, dateRange)
  }

  def computeTopTen(
      views: mutable.Map[String, Dataset[Row]],
      dateRange: mutable.Map[String, (String, String)]
  ): DataFrame = {

    val reportingInterval = StatisticsHandler.getReportingInterval(dateRange)
    val daysCount = DAYS.between(
      reportingInterval._2.toInstant,
      reportingInterval._1.toInstant
    )
    var minMax: DataFrame = Seq
      .empty[String]
      .toDS()
      .withColumnRenamed("value", "day")
      .withColumn("top_ten", lit(null: ArrayType))
    val topTen = TopTen
    val pattern = "yyyy-MM-dd"
    val simpleDateFormat = new SimpleDateFormat(pattern)
    executor =
      Executors.newFixedThreadPool(Runtime.getRuntime.availableProcessors())
    ec = ExecutionContext.fromExecutor(executor)
    val results: Seq[Future[Int]] = (0 until daysCount.toInt)
      .map(days => reportingInterval._2.toInstant.plus(days, DAYS))
      .map(day => {
        Future {
          val dateDay = simpleDateFormat.format(Date.from(day))
          print("\n \n Day is now " + dateDay)
          val countries =
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
          val rowToAdd = Seq(
            (dateDay, topTen.getCountries)
          ).toDF("day", "top_ten")
          minMax = minMax.union(rowToAdd)
          topTen.clear()
          0
        }(ec)
      })

    val allDone = Future.sequence(results)
    //wait for results
    Await.result(allDone, scala.concurrent.duration.Duration.Inf)
    minMax
  }

  def shutdown(): Unit = {
    executor.shutdown()
  }

}
