package fr.mosef.scala.template.reader.impl

import org.apache.spark.sql.{DataFrame, SparkSession}
import fr.mosef.scala.template.reader.Reader

class ReaderImpl(sparkSession: SparkSession) extends Reader {

  def read(format: String, options: Map[String, String], path: String): DataFrame = {
    sparkSession
      .read
      .options(options)
      .format(format)
      .load(path)
  }

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

    format match {
      case "csv" =>
        sparkSession.read
          .option("sep", sep)
          .option("header", hasHeader)
          .option("inferSchema", "true")
          .format("csv")
          .load(path)

      case "parquet" =>
        sparkSession.read.parquet(path)

      case _ =>
        throw new IllegalArgumentException(s"Format non supporté: $format")
    }
  }
  def getInputPathFromProperties(): String = {
    val props = new java.util.Properties()
    props.load(getClass.getClassLoader.getResourceAsStream("application.properties"))
    props.getProperty("input.path")
  }

  def getOutputPathFromProperties(): String = {
    val props = new java.util.Properties()
    props.load(getClass.getClassLoader.getResourceAsStream("application.properties"))
    props.getProperty("output.path")
  }



}
