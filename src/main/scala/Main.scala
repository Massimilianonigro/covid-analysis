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
import java.time.temporal.ChronoUnit.DAYS

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

    // remove useless columns and rename useful ones
    df = df
      .drop("_c1") //day
      .drop("_c2") //month
      .drop("_c3") //year
      .drop("_c5") //deaths
      .drop("_c7") //geoId
      .drop("_c8") //countryTerritoryCode
      .drop("_c9") //popData2019
      .drop("_c10") //continentExp
      .drop("_c11") //Cumulative_number
    df = df
      .withColumnRenamed("_c0", "dateRep")
      .withColumnRenamed("_c6", "country")
      .withColumnRenamed("_c4", "cases")
    var first_row = df.first()
    df = df.filter(row => row != first_row)
    var countriesRow = df.select("country").distinct()
    var session = spark.sqlContext.sparkSession
    df.createOrReplaceTempView("df")
    var countries = countriesRow.collect()
    var views = scala.collection.mutable.Map.empty[String, Dataset[Row]]
    var dateRange = scala.collection.mutable.Map.empty[String, (String, String)]
    var country_str: String = ""
    countries.foreach(country => {
      country_str = CountryHandler.getCountryName(country)
      //print("\nHandling country: " + country_str)
      if (country_str.equals("Afghanistan")) {
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
      }
    })
    var reportingInterval = StatisticsHandler.getReportingInterval(dateRange)
    case class MinMax(min: String, max: String)
    var minMax = spark.emptyDataset[MinMax]
    minMax.withColumn("min", lit(reportingInterval._1.toString))
    minMax.withColumn("max", lit(reportingInterval._2.toString))
    val daysCount = DAYS.between(
      reportingInterval._1.toInstant,
      reportingInterval._2.toInstant
    )
    (0 until daysCount)
      .map(days => reportingInterval._1.toInstant.plus(days, DAYS))
      .foreach(day => {
        print("Day is now " + day.toString)
        var countries =
          StatisticsHandler.reportingCountries(dateRange, day.toString)
        var topTen = TopTen
        for (country <- countries) {
          topTen.add(
            country,
            views(country)
              .select("perc_increase")
              .withColumn("dateRep", col("dateRep").equalTo(day.toString))
              .collect()(0)(0)
              .toString
              .toDouble
          )
        }
      })
    views("Afghanistan").show(500, truncate = false)
    //views.values.foreach(view => view.show())
  }
}
