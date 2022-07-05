package com.clustering

import com.clustering.Utility.{addToMatrix, colSums, logMatrix, normalize, rowsDivided, subtractMins, toIndexRowMatrix}
import org.apache.spark.mllib.linalg.distributed.{IndexedRowMatrix, RowMatrix}
import org.apache.spark.mllib.linalg.{Vectors}
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.math.log

object PsiNorm {
  /*var arraySum: Array[Double] = null
  var parArr: RDD[Double] = null*/

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("PsiNormTest1").setMaster("local[2]").set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)

    val data =  sc.textFile("data.txt")

    val parsedData = data.map(s => Vectors.dense(s.split(';').map(_.toDouble))).cache()
    val nRows : Long = parsedData.count();     //count rows
    //val nRowsApprx : Int = parsedData.countApprox(800).getFinalValue().mean.toInt; approximate rows count in 800ms


    /*val vectors = parsedData.collect()
    val nCols = vectors.apply(1).size
    val arraySum = new Array[Double](nCols)
    parsedData.foreach(v => v.(num => arraySum(i)+= arraySum(i) + log(num+1)))

    parArr = sc.parallelize(arraySum)*/

    val mat: RowMatrix = new RowMatrix(parsedData)


    val normalized = psiNorm(mat, nRows)

    normalized.rows.foreach(x=>println(x))

  }

  def psiNorm(mat: RowMatrix, nRows: Long): IndexedRowMatrix = {
    val invSf = paretoMLE(mat, nRows)
    val normalized = normalize(toIndexRowMatrix(mat), invSf)
    normalized
  }

  def paretoMLE(mat: RowMatrix, nRows: Long): RDD[Double] = {
    //val summary: MultivariateStatisticalSummary = mat.computeColumnSummaryStatistics()
    //val mins = summary.min.toDense  //DenseVector of minimum of each column
    //val a = rowsDivided(nRows,
      //colSums(subtractMins(logMatrix(mat), mins)))

    val a = rowsDivided(nRows, colSums(logMatrix(mat)))
    //val a = rowsDivided(nRows, parArr)
    a
  }
}
