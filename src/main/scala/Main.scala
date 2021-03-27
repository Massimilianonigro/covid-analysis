import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
object Main {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Covid-Analysis")
      .config("spark.master", "local")
      .getOrCreate()

    import spark.implicits._

    var df = spark.read.csv(
      "./resources/COVID-19-geographic-disbtribution-worldwide-2020-12-14.csv"
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
    countriesRow.show()
    var countries = countriesRow.collect()
    countries.foreach(country =>
      session.sql(
        "CREATE TEMPORARY VIEW `" + country
          .toString()
          .substring(
            1,
            country.toString().length - 1
          ) + "` AS SELECT dateRep,cases FROM df WHERE country = '" + country
          .toString()
          .substring(1, country.toString().length - 1) + "'"
      )
    )
    var chad = session.sql("SELECT * FROM Chad")
    chad.show()
  }
}
