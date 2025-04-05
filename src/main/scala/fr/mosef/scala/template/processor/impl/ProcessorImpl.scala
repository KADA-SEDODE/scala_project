package fr.mosef.scala.template.processor.impl

import fr.mosef.scala.template.processor.Processor
import org.apache.spark.sql.{DataFrame, functions => F}

class ProcessorImpl() extends Processor {

  def process(inputDF: DataFrame): (DataFrame, DataFrame, DataFrame, DataFrame, DataFrame) = {

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

    // Rapport 4 : Écart salarial homme/femme par catégorie socioprofessionnelle
    val salaireParCategorieEtSexe = inputDF
      .groupBy("categorie_socioprofessionnelle", "sexe")
      .agg(F.avg("salaire_annuel_brut").alias("salaire_moyen"))

    val report4 = salaireParCategorieEtSexe
      .groupBy("categorie_socioprofessionnelle")
      .pivot("sexe")
      .agg(F.first("salaire_moyen"))
      .withColumn("écart_absolu", F.col("Homme") - F.col("Femme"))
      .orderBy(F.col("écart_absolu").desc_nulls_last)

    // Rapport 5 : Évolution de l'écart salarial hommes/femmes au fil des années
    val salaireParAnneeEtSexe = inputDF
      .groupBy("annee", "sexe")
      .agg(F.avg("salaire_annuel_brut").alias("salaire_moyen"))

    val report5 = salaireParAnneeEtSexe
      .groupBy("annee")
      .pivot("sexe")
      .agg(F.first("salaire_moyen"))
      .withColumn("écart_pourcentage",
        ((F.col("Homme") - F.col("Femme")) / F.col("Homme")) * 100
      )
      .orderBy("annee")

    (report1, report2, report3, report4, report5)
  }

}
