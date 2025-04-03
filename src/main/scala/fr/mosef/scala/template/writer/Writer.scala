package fr.mosef.scala.template.writer

import org.apache.spark.sql.DataFrame
import java.io.File
import scala.util.Try
import java.util.Properties

class Writer {

  private def getOutputFormatFromProperties(): String = {
    val props = new Properties()
    props.load(getClass.getClassLoader.getResourceAsStream("application.properties"))
    props.getProperty("output.format", "csv") // par défaut csv
  }

  def write(df: DataFrame, mode: String = "overwrite", outputDir: String, filename: String): Unit = {
    val format = getOutputFormatFromProperties()

    format match {
      case "csv" =>
        val tmpDir = outputDir + "_tmp"

        df
          .coalesce(1)
          .write
          .mode(mode)
          .option("header", "true")
          .option("sep", ",")
          .csv(tmpDir)

        val dir = new File(tmpDir)
        val partFile = dir.listFiles().find(_.getName.startsWith("part-")).get
        val finalDir = new File(outputDir)
        finalDir.mkdirs()
        val renamedFile = new File(finalDir, filename + ".csv")

        Try(partFile.renameTo(renamedFile))
        dir.listFiles().foreach(_.delete())
        dir.delete()

      case "parquet" =>
        df
          .coalesce(1)
          .write
          .mode(mode)
          .parquet(outputDir + s"/${filename}_parquet")
        println(s"[INFO] Écriture Parquet terminée : $outputDir/${filename}_parquet")

      case other =>
        throw new IllegalArgumentException(s"Format de sortie non supporté : $other")
    }
  }
}
