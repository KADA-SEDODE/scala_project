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

object Main extends App with Job {

  val cliArgs = args
  val MASTER_URL: String = try {
    cliArgs(0)
  } catch {
    case e: java.lang.ArrayIndexOutOfBoundsException => "local[1]"
  }
  /*val SRC_PATH: String = try {
    cliArgs(1)
  } catch {
    case e: java.lang.ArrayIndexOutOfBoundsException => {
      print("No input defined")
      sys.exit(1)
    }
  }
  val DST_PATH: String = try {
    cliArgs(2)
  } catch {
    case e: java.lang.ArrayIndexOutOfBoundsException => {
      "./default/output-writer"
    }
  }*/

  val conf = new SparkConf()
  conf.set("spark.driver.memory", "64M")
  conf.set("spark.testing.memory", "471859200")

  val sparkSession = SparkSession
    .builder
    .master(MASTER_URL)
    .config(conf)
    .appName("Scala Template")
    .enableHiveSupport()
    .getOrCreate()

  sparkSession
    .sparkContext
    .hadoopConfiguration
    .setClass("fs.file.impl",  classOf[BareLocalFileSystem], classOf[FileSystem])


  override val reader: Reader = new ReaderImpl(sparkSession)
  override val processor: Processor = new ProcessorImpl()
  override val writer: Writer = new Writer()
  /*  val src_path = SRC_PATH
    val dst_path = DST_PATH*/
  override val src_path: String = reader.asInstanceOf[ReaderImpl].getInputPathFromProperties()
  override val dst_path: String = reader.asInstanceOf[ReaderImpl].getOutputPathFromProperties()


  /*  val inputDF: DataFrame = reader.read(src_path)*/
  override val inputDF: DataFrame = reader.asInstanceOf[ReaderImpl].readFromProperties()
  override val processedDF: DataFrame = processor.process(inputDF)._1 // Choix du rapport 1 pour satisfaire Job
  inputDF.show(10)
  //  Récupération des 3 rapports
  val (report1, report2, report3) = processor.process(inputDF)

  // Écriture des 3 rapports avec des suffixes
  writer.write(report1, "overwrite", dst_path + "_report1", "salaire_moyen_par_sexe")
  writer.write(report2, "overwrite", dst_path + "_report2", "salaire_moyen_par_tranche_age_et_sexe")
  writer.write(report3, "overwrite", dst_path + "_report3", "top_10_regions_mieux_payees")

}
