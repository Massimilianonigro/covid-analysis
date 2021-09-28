import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.functions.array_contains

// Performs an initial cleanup of the dataset
object PreprocessingHandler {
  def dfPreprocessing(
      df: Dataset[Row],
      max_country_number: String
  ): Dataset[Row] = {
    // removes unused columns and renames used columns
    var out = df
      .drop("_c1") //day
      .drop("_c2") //month
      .drop("_c3") //year
      .drop("_c5") //deaths
      .drop("_c7") //geoId
      .drop("_c8") //countryTerritoryCode
      .drop("_c9") //popData2019
      .drop("_c10") //continentExp
      .drop("_c11") //Cumulative_number
      .withColumnRenamed("_c0", "dateRep")
      .withColumnRenamed("_c6", "country")
      .withColumnRenamed("_c4", "cases")

    var distinct_countries =
      out
        .select(out("country"))
        .distinct()
        .limit(max_country_number.toInt)
        .withColumnRenamed("country", "countries_to_keep")
    out = out
      .join(
        distinct_countries,
        out("country") === distinct_countries("countries_to_keep")
      )
      .drop("countries_to_keep")

    // deleting the first row, which contains the name of the columns
    val firstRow = out.first()
    out
      .filter(row => row != firstRow)
  }
}
