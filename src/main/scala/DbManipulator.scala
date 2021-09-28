import org.apache.spark.sql
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession, functions}

import java.text.SimpleDateFormat
import java.time.temporal.ChronoUnit.DAYS
import java.util.Date

// Performs the required operations over the dataset
class DbManipulator(df: sql.DataFrame, sparkSession: SparkSession) {

  import sparkSession.implicits._

  // seven days moving average and daily performance increase computation
  def computeMovingAverageAndPercentageIncrease(): (
      Map[String, Dataset[Row]],
      Map[String, (String, String)]
  ) = {
    val session: SparkSession = sparkSession.sqlContext.sparkSession

    // initializes empty set of views made of country name and related rows
    var views: Map[String, Dataset[Row]] = Map.empty[String, Dataset[Row]]
    var dateRange: Map[String, (String, String)] =
      Map.empty[String, (String, String)]
    var country_str: String = ""
    val countries: Array[Row] = CountryHandler.getCountries(df)
    countries.foreach(country => {
      country_str = CountryHandler.getCountryName(country)
      print("\nHandling country: " + country_str)
      views = views.updated(
        country_str,
        CountryHandler.getCountryView(country, session)
      )

      views = views.updated(
        country_str,
        views(country_str).withColumn("cases", col("cases").cast("Double"))
      )

      // missing dates are added to countries with gaps
      views = views.updated(
        country_str,
        CountryHandler.fillMissingDates(views(country_str), session)
      )

      // computes range of report dates of each country
      dateRange = dateRange.updated(
        country_str,
        (
          views(country_str)
            .select(functions.max("dateRep"))
            .collect()(0)(0)
            .toString,
          views(country_str)
            .select(functions.min("dateRep"))
            .collect()(0)(0)
            .toString
        )
      )

      views = views.updated(
        country_str,
        StatisticsHandler.calculateMovingAverage(views(country_str))
      )

      views = views.updated(
        country_str,
        StatisticsHandler
          .calculatePercentageIncrease(views(country_str), session)
      )
      print("\nReturning from country: " + country_str)
    })
    (views, dateRange)
  }

  // using the data computed, the top ten countries with the highest percentage increase are computed
  def computeTopTen(
      views: Map[String, Dataset[Row]],
      dateRange: Map[String, (String, String)]
  ): DataFrame = {
    var temp_list: List[(String, Double)] = List.empty
    val reportingInterval = StatisticsHandler.getReportingInterval(dateRange)
    val daysCount = DAYS.between(
      reportingInterval._2.toInstant,
      reportingInterval._1.toInstant
    )

    // stores the top ten every day
    var minMax: DataFrame = Seq
      .empty[String]
      .toDS()
      .withColumnRenamed("value", "day")
      .withColumn("top_ten", lit(null: ArrayType))
    val topTen = TopTen
    val pattern = "yyyy-MM-dd"
    val simpleDateFormat = new SimpleDateFormat(pattern)

    // iterates over days
    (0 until daysCount.toInt)
      .map(days => reportingInterval._2.toInstant.plus(days, DAYS))
      .foreach(day => {
        val dateDay = simpleDateFormat.format(Date.from(day))
        print("\n \n Day is now " + dateDay)
        val countries =
          StatisticsHandler.reportingCountries(dateRange, dateDay)

        // iterates over countries and orders them by percentage increase
        countries.foreach(country => {
          temp_list = topTen.add(
            temp_list,
            country,
            views(country)
              .select("perc_increase")
              .filter(views(country)("dateRep") === dateDay)
              .collect()(0)(0)
              .toString
              .toDouble
          )
        })

        // At the end of the day, the topTen list is added to the minMax dataset
        val rowToAdd = Seq(
          (dateDay, topTen.getCountries(temp_list))
        ).toDF("day", "top_ten")
        minMax = minMax.union(rowToAdd)
        topTen.clear(temp_list)
      })

    minMax
  }

}
