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
import CountryHandler._

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
    var country_str: String = ""

    countries.foreach(country => {
      country_str = CountryHandler.getCountryName(country)
      print("\nHandling country: " + country_str)
      views(country_str) = CountryHandler.getCountryView(country, session)
      views(country_str).withColumn("cases", col("cases").cast("Double"))
      views(country_str) =
        CountryHandler.fillMissingDates(views(country_str), session)
    })

    views("Afghanistan").show(100)
    //views.values.foreach(view => view.show())
  }
}
