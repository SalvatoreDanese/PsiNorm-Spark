import org.apache.log4j.Logger
import org.apache.spark
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.feature.PCA
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Vector, Vectors}

import math.log1p
import scala.collection.mutable.ArrayBuffer

object PsiNormV4 {
  var sums: Array[Double] = null
  var numberOfRows: Long = 0
  val logger = Logger.getLogger(getClass)

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setAppName("PsiNormTest").setMaster("yarn").set("spark.driver.maxResultSize", "40g")
    val sc = new SparkContext(conf)

    val data = sc.textFile("dataset5.csv")


    val parsedData: RDD[Array[Double]] = data.map(s => (s.split(',').map(_.toDouble))).cache()
    val nCols: Int = parsedData.take(1)(0).length
    numberOfRows = parsedData.count()

    val zero: Array[Double] = Array.fill[Double](nCols)(0)
    sums = parsedData.mapPartitions(preProcessing).
      aggregate(zero)(aggregate, combine)


    val normalized = parsedData.mapPartitions(normalization(sums, numberOfRows))
    normalized.take(1)(0).foreach(d => println(d))

    val normalizedVectors: RDD[Vector] = normalized.map( it =>  Vectors.dense(it) )

    val pca = new PCA(100).fit(normalizedVectors).transform(normalizedVectors)

    pca.take(10).foreach(v => println(v))

    val numClusters = 2
    val numIterations = 20
    val clusters = KMeans.train(pca, numClusters, numIterations)
    
    
    val centers = clusters.clusterCenters
    
    println("Centers:")
    centers.foreach(v => v.toArray.foreach(d => print(d +", ")))


    val WSSSE = clusters.computeCost(pca)
    println(s"Within Set Sum of Squared Errors = $WSSSE")


    /*val rwmat = new RowMatrix(normalizedVectors)
    val pc = rwmat.computePrincipalComponents(10)
    val projected = rwmat.multiply(pc)
    projected.rows.take(1).foreach(v => v.toArray.foreach(d => println(d)))*/


  }

  def preProcessing(it: Iterator[Array[Double]]): Iterator[Array[Double]] = {

    val arr: Array[Array[Double]] = new Array[Array[Double]](1)
    arr(0) = it.next()
    arr(0) = arr(0).map(d => log1p(d))
    while(it.hasNext) {
      arr(0) = arr(0).
        zip(it.next()).
        map {
          x => x._1 + log1p(x._2)
        }
    }

    arr.iterator

  }

 def normalization(colSums: Array[Double], nRows: Long)(it: Iterator[Array[Double]]): Iterator[Array[Double]] = {
    val arr: ArrayBuffer[Array[Double]] = new ArrayBuffer[Array[Double]]()
    while (it.hasNext) {
        arr.+=(it.next().zip(colSums).map { case (value, sum) =>
            if (sum != 0) value * nRows / sum else 0.0 // Se `sum` è zero, restituisce 0.0 invece di NaN
        })
    }
    arr.iterator
}

  def aggregate(accu: Array[Double], arr: Array[Double]): Array[Double] = {
    for (i <- 0 to arr.length - 1) {
      accu(i) += arr(i)
    }
    accu
  }

  def combine(arr1: Array[Double], arr2: Array[Double]): Array[Double] = {
    for (i <- 0 to arr1.length - 1) {
      arr1(i) += arr2(i)
    }
    arr1
  }

}
