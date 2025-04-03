package fr.mosef.scala.template.writer

import org.apache.spark.sql.DataFrame
import java.io.File
import scala.util.Try
import java.util.Properties

// Classe Writer pour gérer l'écriture des DataFrames dans des fichiers
class Writer {

  // Récupère le format de sortie à partir du fichier de propriétés (par défaut : csv)
  private def getOutputFormatFromProperties(): String = {
    val props = new Properties()
    props.load(getClass.getClassLoader.getResourceAsStream("application.properties"))
    props.getProperty("output.format", "csv") // par défaut csv
  }

  // Écrit les données dans un fichier selon le format spécifié dans les propriétés
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
        // Lance une exception si le format de sortie n'est pas supporté
        throw new IllegalArgumentException(s"Format de sortie non supporté : $other")
    }
  }
}
