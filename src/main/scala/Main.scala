import org.apache.spark.sql.catalyst.util.DateFormatter
import org.apache.spark.sql.{
  Column,
  ColumnName,
  Dataset,
  Row,
  SparkSession,
  functions
}
import org.apache.spark.sql.functions.{col, min}

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, LongType, TimestampType}
object Main {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Covid-Analysis")
      .config("spark.master", "local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    var df = spark.read.csv(
      "./resources/data.csv"
    )
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
    var country_str: String = ""
    countries.foreach(country => {
      country_str = country
        .toString()
        .substring(1, country.toString().length - 1)
      session
        .sql(
          "CREATE TEMPORARY VIEW `" + country_str + "` AS SELECT dateRep,cases FROM df WHERE country = '" + country_str + "'"
        )
      views(country_str) =
        session.sql("SELECT * FROM `" + country_str + "`").orderBy("dateRep")
    })

    //Now for the values of each view in country we have to iterate on the days and fill the gaps

    views("Afghanistan") = views("Afghanistan")
      .withColumn("dateRep", to_date($"dateRep", "dd/MM/yyyy"))
    val w = Window.orderBy($"dateRep")
    val tempDf = views("Afghanistan")
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
    views("Afghanistan") = views("Afghanistan")
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
      views("Afghanistan") = views("Afghanistan").withColumn(
        "cases",
        when(
          col("dateRep").isInCollection(interval.getList(0)),
          lit(($"cases" / (interval.getDouble(1) + 1.0)))
        ).otherwise($"cases")
      )
    })
    print(intervals_to_fill.mkString("Array(", ", ", ")"))
    views("Afghanistan").show(100)
    //views.values.foreach(view => view.show())
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
