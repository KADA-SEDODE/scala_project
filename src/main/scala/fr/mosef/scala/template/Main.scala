package fr.mosef.scala.template

import fr.mosef.scala.template.job.Job
import fr.mosef.scala.template.processor.Processor
import fr.mosef.scala.template.processor.impl.ProcessorImpl
import fr.mosef.scala.template.reader.Reader
import fr.mosef.scala.template.reader.impl.ReaderImpl
import org.apache.spark.sql.{DataFrame, SparkSession}
import fr.mosef.scala.template.writer.Writer
import org.apache.spark.SparkConf
import com.globalmentor.apache.hadoop.fs.BareLocalFileSystem
import org.apache.hadoop.fs.FileSystem

// Point d'entrée principal de l'application Scala
object Main extends App with Job {

  // Récupération des arguments de ligne de commande
  val cliArgs = args

  // Définition de l'URL Master Spark en fonction des arguments (ou valeur par défaut)
  val MASTER_URL: String = try {
    cliArgs(0)
  } catch {
    case e: java.lang.ArrayIndexOutOfBoundsException => "local[1]" // Valeur par défaut pour exécution locale
  }

  // Configuration Spark, incluant mémoire du driver et mémoire de test
  val conf = new SparkConf()
  conf.set("spark.driver.memory", "64M")
  conf.set("spark.testing.memory", "471859200")

  // Création de la session Spark avec prise en charge Hive
  val sparkSession = SparkSession
    .builder
    .master(MASTER_URL)
    .config(conf)
    .appName("Scala Template")
    .enableHiveSupport()
    .getOrCreate()

  // Configuration du FileSystem pour utiliser BareLocalFileSystem
  sparkSession
    .sparkContext
    .hadoopConfiguration
    .setClass("fs.file.impl",  classOf[BareLocalFileSystem], classOf[FileSystem])

  // Implémentations des interfaces Job
  override val reader: Reader = new ReaderImpl(sparkSession) // Instancie ReaderImpl
  override val processor: Processor = new ProcessorImpl()  // Instancie ProcessorImpl
  override val writer: Writer = new Writer()  // Instancie Writer

  // Récupération des chemins d'entrée et de sortie à partir des propriétés
  override val src_path: String = reader.asInstanceOf[ReaderImpl].getInputPathFromProperties()
  override val dst_path: String = reader.asInstanceOf[ReaderImpl].getOutputPathFromProperties()


  /*  val inputDF: DataFrame = reader.read(src_path)*/
  override val inputDF: DataFrame = reader.asInstanceOf[ReaderImpl].readFromProperties()
  override val processedDF: DataFrame = processor.process(inputDF)._1 // Choix du rapport 1 pour satisfaire Job
  inputDF.show(10)

  // Récupère les 3 rapports résultants du traitement
  val (report1, report2, report3) = processor.process(inputDF)

  // Écrit les 3 rapports dans des fichiers distincts avec des suffixes et des noms logiques
  writer.write(report1, "overwrite", dst_path + "_report1", "salaire_moyen_par_sexe")
  writer.write(report2, "overwrite", dst_path + "_report2", "salaire_moyen_par_tranche_age_et_sexe")
  writer.write(report3, "overwrite", dst_path + "_report3", "top_10_regions_mieux_payees")

}
