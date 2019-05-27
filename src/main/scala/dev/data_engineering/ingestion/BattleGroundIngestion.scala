package dev.data_engineering.ingestion

import org.apache.spark.sql.functions.{lit, when}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object BattleGroundIngestion {
  val spark: SparkSession = SparkSession.builder().getOrCreate()
  import spark.implicits._

  def main(args: Array[String]): Unit = {

    val battlegroundDF = spark.read
      .option("header", value = true)
      .option("inferSchema", value = true)
      .csv("/wow_data/wow_bg_raw/wowbgs2.csv")

    val cleanedBGDF = battlegroundDF
      .transform(withColumnsRenamed())
      .transform(withMatchResult())
      .transform(withBonusEvent())
      .transform(withCleanerRoles())
      .transform(withVerboseBattlegroundNames())

    cleanedBGDF.write
      .mode(SaveMode.Overwrite)
      .option("header", value = true)
      .csv("/wow_data/all_bgs.csv")
  }

  /**
    * Renames the columns to upper snake case, with more obvious names.
    * @param df the DataFrame with the columns to rename
    * @return a DataFrame with renamed columns
    */
  def withColumnsRenamed()(df: DataFrame): DataFrame = {
    val newColNamesMap = Map(
      "Battleground" -> "BATTLEGROUND",
      "Code"         -> "BG_ID",
      "Faction"      -> "FACTION",
      "Class"        -> "CLASS",
      "KB"           -> "KILLING_BLOWS",
      "D"            -> "DEATHS",
      "HK"           -> "HONORABLE_KILLS",
      "DD"           -> "DAMAGE_DONE",
      "HD"           -> "HEALING_DONE",
      "Honor"        -> "HONOR",
      "Win"          -> "WON_MATCH",
      "Lose"         -> "LOST_MATCH",
      "Rol"          -> "ROLE",
      "BE"           -> "BONUS_EVENT"
    )
    var renamedDF = df
    newColNamesMap.foreach(newCol => renamedDF = renamedDF.withColumnRenamed(newCol._1, newCol._2))
    renamedDF
  }

  /**
    * Creates a new column called `MATCH_RESULT` which is based on the `WON_MATCH` column. If `WON_MATCH` is equal to 1, then `MATCH_RESULT` is set to "WON", otherwise `MATCH_RESULT` is "LOST".
    * @param df the DataFrame with the battleground data to transform
    * @return a DataFrame with the `MATCH_RESULT` column
    */
  def withMatchResult()(df: DataFrame): DataFrame = {
    val dfWithMatchResult =
      df.withColumn("MATCH_RESULT", when($"WON_MATCH" === 1, lit("WON")).otherwise("LOST"))
    dfWithMatchResult.drop("WON_MATCH").drop("LOST_MATCH")
  }

  /**
    * Changes the `BONUS_EVENT` column to contain true/false instead of 1 and null.
    * @param df the DataFrame with the `BONUS_EVENT` column to be transformed
    * @return a DataFrame with a `BONUS_EVENT` column where the values are true/false
    */
  def withBonusEvent()(df: DataFrame): DataFrame = {
    df.withColumn("BONUS_EVENT", when($"BONUS_EVENT" === 1, lit(true)).otherwise(lit(false)))
  }

  /**
    * Cleans up the `ROLE` column values by changing them from "dps" to "damage" and "heal" to "healer".
    * @param df the DataFrame with the `ROLE` column
    * @return the DataFrame with a `ROLE` column with the values "damage" or "healer"
    */
  def withCleanerRoles()(df: DataFrame): DataFrame = {
    df.withColumn("ROLE", when($"ROLE" === "dps", lit("damage")).otherwise(lit("healer")))
  }

  /**
    * Transforms the `BATTLEGROUND` column to have the full name of the battleground as opposed to an acronym.
    * @param df the DataFrame with the `BATTLEGROUND` column
    * @return a DataFrame with more verbose names in the `BATTLEGROUND` column
    */
  def withVerboseBattlegroundNames()(df: DataFrame): DataFrame = {
    val verboseBGNamesDF = Seq(
      ("SM", "Silvershard Mines"),
      ("TP", "Twin Peaks"),
      ("SA", "Strand of the Ancients"),
      ("ES", "Eye of the Storm"),
      ("WG", "Warsong Gulch"),
      ("DG", "Deepwind Gorge"),
      ("BG", "Battle for Gilneas"),
      ("AB", "Arathi Basin"),
      ("SS", "Seething Shore"),
      ("TK", "Temple of Kotmogu")
    ).toDF("BATTLEGROUND", "LONG_BG_NAME")

    df.join(verboseBGNamesDF, Seq("BATTLEGROUND"))
      .drop("BATTLEGROUND")
      .withColumnRenamed("LONG_BG_NAME", "BATTLEGROUND")
  }
}
