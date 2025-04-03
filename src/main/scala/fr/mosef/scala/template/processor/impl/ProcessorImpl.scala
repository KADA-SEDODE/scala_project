package fr.mosef.scala.template.processor.impl


import fr.mosef.scala.template.processor.Processor
import org.apache.spark.sql.{DataFrame, functions => F}

class ProcessorImpl() extends Processor {

  def process(inputDF: DataFrame): (DataFrame, DataFrame, DataFrame) = {
    // Rapport 1 : salaire moyen par sexe
    val report1 = inputDF
      .groupBy("sexe")
      .agg(F.avg("salaire_annuel_brut").alias("salaire_moyen"))
    // Rapport 2 : salaire moyen par tranche d'âge et sexe
    val report2 = inputDF
      .groupBy("tranche_age", "sexe")
      .agg(F.avg("salaire_annuel_brut").alias("salaire_moyen"))
    // Rapport 3 : top 10 des régions les mieux payées
    val report3 = inputDF
      .groupBy("region")
      .agg(F.avg("salaire_annuel_brut").alias("salaire_moyen"))
      .orderBy(F.col("salaire_moyen").desc)
      .limit(10)
    (report1, report2, report3)
  }

}
