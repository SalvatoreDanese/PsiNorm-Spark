package com.clustering

import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix, RowMatrix}
import org.apache.spark.mllib.linalg.{DenseVector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.distributed.BlockMatrix
import scala.math.log


object Utility {

  def addToMatrix(matrix: RowMatrix, i: Int): RowMatrix = {
    val incremented = matrix.rows.map(row => Vectors.dense(row.toArray.map(x=>x+i)))
    new RowMatrix(incremented)

  }

  def toIndexRowMatrix(m: RowMatrix): IndexedRowMatrix = {
    val indexedRM = new IndexedRowMatrix(m.rows.zipWithIndex.map({
      case (row, idx) => IndexedRow(idx, row)}))

    indexedRM
  }
  def transposeRowMatrix2(m: RowMatrix): IndexedRowMatrix = {

    val indexedRM = toIndexRowMatrix(m)
    val matA: BlockMatrix = indexedRM.toBlockMatrix().cache()
    matA.validate()

    val matB: BlockMatrix = matA.transpose
    matB.toIndexedRowMatrix()
  }

  def transposeIndexedRowMatrix(m: IndexedRowMatrix): IndexedRowMatrix = {
    val matA: BlockMatrix = m.toBlockMatrix().cache()
    matA.validate()

    val matB: BlockMatrix = matA.transpose
    matB.toIndexedRowMatrix()
  }

  def colSums(m: IndexedRowMatrix): RDD[Double] = {
    val sums = transposeIndexedRowMatrix(m).rows.map{
      case IndexedRow(i, values) => (i, values.toArray.sum)
    }
    sums.values
  }

  def logMatrix(m: RowMatrix): IndexedRowMatrix = {
    val indexedRM= toIndexRowMatrix(m)
    val logRDD = indexedRM.rows.map{
      case IndexedRow(i, values) => (i,Vectors.dense(values.toArray.map(x => log(x+1))))
    }
    val logMat: IndexedRowMatrix = new IndexedRowMatrix(logRDD.values.zipWithIndex.map({
      case (row, idx) => IndexedRow(idx, row)}))
    logMat
  }

  def subtractMins(mat: IndexedRowMatrix, mins: DenseVector): IndexedRowMatrix = {
    val m: IndexedRowMatrix = transposeIndexedRowMatrix(mat)
    val subtraction = m.rows.map{
      case IndexedRow(i, values) => (i, Vectors.dense(values.toArray.map(x => x - log(mins.apply(i.toInt)))))
    }
    val subtractedMatrix = new IndexedRowMatrix(subtraction.values.zipWithIndex().map({
      case (row, idx) => IndexedRow(idx, row)
    }))

    transposeIndexedRowMatrix(subtractedMatrix)
  }

  def rowsDivided(nRows: Long, colSums: RDD[Double]): RDD[Double] = {
    val divided = colSums.map(x=> nRows/x)
    divided
  }

  def normalize(mat: IndexedRowMatrix, a:RDD[Double]): IndexedRowMatrix = {
    val m = transposeIndexedRowMatrix(mat)
    val aValues = a.collect()
    val multiplication = m.rows.map{
      case IndexedRow(i, values) => (i, Vectors.dense(values.toArray.map(x => x * aValues(i.toInt))))
    }

    val normalizedMatrix = new IndexedRowMatrix(multiplication.values.zipWithIndex().map({
      case (row, idx) => IndexedRow(idx, row)
    }))
    transposeIndexedRowMatrix(normalizedMatrix)
  }

}
