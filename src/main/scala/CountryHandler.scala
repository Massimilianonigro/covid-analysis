import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}

import java.time.LocalDate
import java.time.format.DateTimeFormatter

object CountryHandler {
  def getCountries(df: Dataset[Row]): Array[Row] = {
    val countriesRow = df.select("country").distinct
    countriesRow.collect()
  }
  def getCountryView(
      country: Row,
      session: SparkSession
  ): Dataset[Row] = {
    var country_str: String = ""

    country_str = getCountryName(country)
    session
      .sql(
        "CREATE TEMPORARY VIEW `" + country_str + "` AS SELECT dateRep,cases FROM df WHERE country = '" + country_str + "'"
      )
    session.sql("SELECT * FROM `" + country_str + "`").orderBy("dateRep")
  }

  def getCountryName(country: Row): String = {
    country
      .toString()
      .substring(1, country.toString().length - 1)
  }

  def fillMissingDates(
      countryView: Dataset[Row],
      session: SparkSession
  ): Dataset[Row] = {

    import session.implicits._

    var dateView =
      countryView.withColumn("dateRep", to_date($"dateRep", "dd/MM/yyyy"))

    val w = Window.orderBy($"dateRep")

    val tempDf = dateView
      .withColumn("diff", datediff(lead($"dateRep", 1).over(w), $"dateRep"))
      .filter(
        $"diff" > 1
      ) // Pick date diff more than one day to generate our date
      .withColumn("next_dates", fill_dates($"dateRep", $"diff"))
      .withColumn(
        "cases",
        lit(0)
      )
      .withColumn("dateRep", explode($"next_dates"))
      .withColumn("dateRep", $"dateRep")

    dateView = dateView
      .union(
        tempDf
          .select("dateRep", "cases")
      )
      .orderBy("dateRep")

    val intervals_to_fill =
      tempDf
        .select("next_dates", "diff")
        .distinct()
        .withColumn("diff", col("diff").cast("Double"))
        .collect()
    intervals_to_fill.foreach(interval => {
      val indexOfFirstDate = dateView
        .select("dateRep")
        .collect()
        .indexOf(
          dateView
            .select("dateRep")
            .filter(col("dateRep").isInCollection(interval.getList(0)))
            .collect()(0)
        )
      val indexOfDateOfReport =
        indexOfFirstDate + interval.getDouble(1).toInt - 1
      val casesReported = dateView
        .collectAsList()
        .get(indexOfDateOfReport.asInstanceOf[Int])
        .getString(1)
        .toDouble
      dateView = dateView.withColumn(
        "cases",
        when(
          col("dateRep").isInCollection(interval.getList(0)),
          lit((casesReported / (interval.getDouble(1) + 1.0)))
        ).otherwise($"cases")
      )
    })
    dateView
  }

  def fill_dates: UserDefinedFunction =
    udf((start: String, excludedDiff: Int) => {
      val dtFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
      val fromDt = LocalDate.parse(start, dtFormatter)
      (1 until excludedDiff).map(day => {
        val dt = fromDt.plusDays(day)
        "%4d-%2d-%2d"
          .format(dt.getYear, dt.getMonthValue, dt.getDayOfMonth)
          .replace(" ", "0")
      })
    })

}
