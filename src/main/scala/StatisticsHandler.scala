import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, lag, lit, when}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import java.text.SimpleDateFormat
import java.util.Date
import collection.immutable.List
object StatisticsHandler {

  val formatter = new SimpleDateFormat("yyyy-MM-dd")
  def calculateMovingAverage(country: Dataset[Row]): Dataset[Row] = {
    country.withColumn(
      "mov_average",
      avg(
        country("cases")
      ).over(Window.rowsBetween(-6, 0))
    )
  }

  def calculatePercentageIncrease(
      country: Dataset[Row],
      session: SparkSession
  ): Dataset[Row] = {
    import session.implicits._
    country
      .withColumn(
        "perc_increase",
        (country("mov_average") - lag($"mov_average", 1)
          .over(Window.orderBy("dateRep"))) / 100
      )
      .withColumn(
        "perc_increase",
        when(col("perc_increase").isNull, lit("0.0"))
          .otherwise($"perc_increase")
      )
  }
  def reportingCountries(
      dateRange: scala.collection.mutable.Map[String, (String, String)],
      date: String
  ): List[String] = {
    var countries = List.empty[String]
    val formattedDate = formatter.parse(date)
    for ((k, v) <- dateRange) {
      val formattedEnd = formatter.parse(v._1)
      val formattedStart = formatter.parse(v._2)
      if (
        (formattedDate
          .before(formattedEnd) || formattedDate.equals(
          formattedEnd
        ) && (formattedDate
          .after(formattedStart) || formattedDate.equals(formattedStart)))
      ) {
        countries = countries :+ k
      }
    }
    countries
  }
  def getReportingInterval(
      dateRange: scala.collection.mutable.Map[String, (String, String)]
  ): (Date, Date) = {
    var minDate: Date = new Date(Long.MaxValue)
    var maxDate: Date = new Date(Long.MinValue)
    dateRange.values.foreach(interval => {
      val formattedStart = formatter.parse(interval._1)
      val formattedEnd = formatter.parse(interval._2)
      if (formattedStart.before(minDate))
        minDate = formattedStart
      if (formattedEnd.after(maxDate))
        maxDate = formattedEnd
    })
    (minDate, maxDate)
  }

}
