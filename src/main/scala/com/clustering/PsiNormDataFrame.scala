import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.{PCA, VectorAssembler}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object PsiNormV4 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("PsiNormCSVTest")
      .master("local[2]")
      .config("spark.driver.maxResultSize", "40g")
      .getOrCreate()

    // Percorso del file CSV
    val filePath = "sc_10x.countInt.csv"

    // Leggi il CSV assicurandoti che Spark infersca lo schema
    val df = spark.read
      .option("header", "false") // Se il CSV ha un'intestazione
      .option("inferSchema", "true") // Inferisci automaticamente il tipo di dato
      .csv(filePath)

    // Mostra lo schema per confermare i tipi di dato inferiti
    df.printSchema()

    // Controlla se ci sono colonne non numeriche e convertile a DoubleType
    val numericColumns = df.columns.filter(c => df.schema(c).dataType.isInstanceOf[NumericType])

    if (numericColumns.isEmpty) {
      println("Errore: non ci sono colonne numeriche nel dataset!")
      return
    }

    // Se necessario, converti tutte le colonne numeriche a DoubleType
    val dfNumeric = df.select(numericColumns.map(c => col(c).cast(DoubleType).alias(c)): _*)

    // Verifica che tutte le colonne siano ora DoubleType
    dfNumeric.printSchema()

    // Mostra un campione di dati per vedere il formato
    dfNumeric.show(5)

    // Ottieni il numero di righe e colonne
    val numberOfRows = dfNumeric.count()
    println(s"Numero di righe: $numberOfRows")
    println(s"Numero di colonne numeriche: ${dfNumeric.columns.length}")

    // Preprocessing: calcolo del logaritmo delle colonne
    val logTransformedDF = dfNumeric.select(dfNumeric.columns.map(c => log1p(col(c)).alias(c)): _*)

    val aggExprs = logTransformedDF.columns.map(c => sum(col(c)).alias(c))

    // Usa la mappa per l'aggregazione
    val colSums: Row = logTransformedDF
      .agg(aggExprs.head, aggExprs.tail: _*) // Passiamo la prima espressione e poi le restanti
      .first()

    // Converte i risultati della somma in un array
    val sumsArray = colSums.toSeq.map(_.toString.toDouble).toArray
    println(s"Somme delle colonne: ${sumsArray.mkString(", ")}")

    val normalizationUDF = udf((row: Seq[Double]) => {
      row.zip(sumsArray).map { case (value, sum) => value * numberOfRows / sum }
    })

    // Applica la normalizzazione a tutte le righe della matrice
    val normalizedDF = dfNumeric.withColumn("normalized_features", normalizationUDF(array(dfNumeric.columns.map(col): _*)))

    // Mostra i risultati normalizzati
    normalizedDF.select("normalized_features").show(10, truncate = false)

////////////////////////////////////////////////////////////////////////////////////////////////

    // Assemblare i dati normalizzati in un vettore per la PCA e il clustering
    val assembler = new VectorAssembler()
      .setInputCols(dfNumeric.columns)
      .setOutputCol("features")

    val assembledDF = assembler.transform(normalizedDF)

    // Applica PCA per ridurre le dimensioni a 100 componenti principali
    val pca = new PCA()
      .setInputCol("features")
      .setOutputCol("pca_features")
      .setK(100) // Riduci a 100 dimensioni
      .fit(assembledDF)

    val pcaDF = pca.transform(assembledDF).select("pca_features")
    pcaDF.show(10, truncate = false)

    // Clustering KMeans sui dati ridotti
    val numClusters = 2
    val kmeans = new KMeans()
      .setK(numClusters)
      .setSeed(1L)
      .setFeaturesCol("pca_features")
      .setPredictionCol("cluster")

    // Addestra il modello KMeans
    val model = kmeans.fit(pcaDF)

    // Assegna i cluster
    val clusteredDF = model.transform(pcaDF)
    clusteredDF.show(10, truncate = false)

    // Mostra i centri dei cluster
    val centers = model.clusterCenters
    println("Centri dei cluster:")
    centers.foreach(center => println(center))

    // Calcola il costo WSSSE
    val WSSSE = model.computeCost(pcaDF)
    println(s"Within Set Sum of Squared Errors (WSSSE) = $WSSSE")

    spark.stop()
  }

}
