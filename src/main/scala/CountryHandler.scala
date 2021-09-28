import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}

import java.time.LocalDate
import java.time.format.DateTimeFormatter

// helper class for country operations
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

  // fills gaps of at least one missing daily report by dividing data of the first report after the gap over missing days
  def fillMissingDates(
      countryView: Dataset[Row],
      session: SparkSession
  ): Dataset[Row] = {

    import session.implicits._

    var dateView =
      countryView.withColumn("dateRep", to_date($"dateRep", "dd/MM/yyyy"))

    // creates a window aggregation function used to order dates and detect missing reports
    val w = Window.orderBy($"dateRep")

    // orders rows by date, keeps rows with gaps, adds column with gap values and missing dates
    val tempDf = dateView
      .withColumn("diff", datediff(lead($"dateRep", 1).over(w), $"dateRep"))
      .filter(
        $"diff" > 1
      ) // Pick date difference greater than one day
      .withColumn("next_dates", fill_dates($"dateRep", $"diff"))
      .withColumn(
        "cases",
        lit(0)
      )
      .withColumn("dateRep", explode($"next_dates"))
      .withColumn("dateRep", $"dateRep")

    // adds computed rows to view
    dateView = dateView
      .union(
        tempDf
          .select("dateRep", "cases")
      )
      .orderBy("dateRep")

    // stores intervals of missing reports
    val intervals_to_fill =
      tempDf
        .select("next_dates", "diff")
        .distinct()
        .withColumn("diff", col("diff").cast("Double"))
        .collect()

    // for each interval, it stores the first date
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

      // computes first date of available report
      val indexOfDateOfReport =
        indexOfFirstDate + interval.getDouble(1).toInt - 1

      // obtains the reported cases on that date
      val casesReported = dateView
        .collectAsList()
        .get(indexOfDateOfReport.asInstanceOf[Int])
        .getDouble(1)

      // updates the missing daily reports
      dateView = dateView.withColumn(
        "cases",
        when(
          col("dateRep").isInCollection(interval.getList(0)),
          lit(casesReported / (interval.getDouble(1) + 1.0))
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
