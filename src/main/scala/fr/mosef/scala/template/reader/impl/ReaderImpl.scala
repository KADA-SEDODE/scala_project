package fr.mosef.scala.template.reader.impl

import org.apache.spark.sql.{DataFrame, SparkSession}
import fr.mosef.scala.template.reader.Reader


// Implémentation de la classe Reader pour lire des données dans Spark
class ReaderImpl(sparkSession: SparkSession) extends Reader {

  // Méthode pour lire les données en spécifiant le format, les options et le chemin
  def read(format: String, options: Map[String, String], path: String): DataFrame = {
    sparkSession
      .read
      .options(options)
      .format(format)
      .load(path)
  }

  // Méthode pour lire les données CSV avec des options par défaut
  def read(path: String): DataFrame = {
    sparkSession
      .read
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .format("csv")
      .load(path)
  }

  def read(): DataFrame = {
    sparkSession.sql("SELECT 'Empty DataFrame for unit testing implementation")
  }

  def readFromProperties(): DataFrame = {
    val props = new java.util.Properties()
    props.load(getClass.getResourceAsStream("/application.properties"))

    val format = props.getProperty("input.format")
    val sep = props.getProperty("input.separator")
    val hasHeader = props.getProperty("input.hasHeader")
    val path = props.getProperty("input.path")

    // Lecture des données en fonction du format spécifié
    format match {
      case "csv" =>
        sparkSession.read
          .option("sep", sep)
          .option("header", hasHeader)
          .option("inferSchema", "true")
          .format("csv")
          .load(path)

      case "parquet" =>
        sparkSession.read.parquet(path) // Charge les données au format Parquet

      case _ =>
        throw new IllegalArgumentException(s"Format non supporté: $format") // Exception pour format invalide
    }
  }
  // Récupère le chemin d'entrée depuis les propriétés
  def getInputPathFromProperties(): String = {
    val props = new java.util.Properties()
    props.load(getClass.getClassLoader.getResourceAsStream("application.properties"))
    props.getProperty("input.path") // Renvoie le chemin d'entrée
  }

  // Récupère le chemin de sortie depuis les propriétés
  def getOutputPathFromProperties(): String = {
    val props = new java.util.Properties()
    props.load(getClass.getClassLoader.getResourceAsStream("application.properties"))
    props.getProperty("output.path") // Renvoie le chemin de sortie
  }



}
