# PsiNorm-Spark

data.txt contains example data.

src/main/scala/com/clustering/KMeansExample.scala is an example of k-means clustering using MLlib framework.

src/main/scala/com/clustering/PsiNorm.scala contains main method and normalizes data.txt using Pareto Normalization.

src/main/scala/com/clustering/Utility.scala is the implementation of the methods used by PsiNorm on RowMatrix object.

src/main/scala/com/clustering/PsiNormV4.scala is the up-to-date implementation of Pareto Normalization based on RDD.

The mean %deltaDifference between the R implementation of PsiNorm and PsiNormV4 using sc_10x.count2.csv dataset (first 7804 rows of sc_10x.count.csv) is 0.0061%.

Link to generated dataset (10.000 x 100.000): https://drive.google.com/file/d/1Jc69EX0YSd06CCOPI7PUnSdU9xQf7_ZA/view?usp=sharing
