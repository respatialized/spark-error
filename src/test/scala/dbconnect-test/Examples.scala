package test

import java.util.Random
import org.scalatest.FunSuite
import scala.collection.mutable

import org.apache.spark.ml.stat.Correlation
import org.apache.spark.sql._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.{Vectors, Matrix}
import org.apache.spark.sql.functions.col
import org.apache.spark.ml.feature.Bucketizer

class Examples extends FunSuite with SparkSessionTestWrapper {

  test("dbconnect error") {
    import spark.implicits._

    val sampleCount = 100000000L

    val count = sc.parallelize(1L to sampleCount).filter { _ =>
      val x = math.random
      val y = math.random
      x*x + y*y < 1
    }.count()
    info(s"Pi is roughly ${4.0 * count / sampleCount}")

    val numMappers = 2
    val numKVPairs = 1000
    val valSize = 1000
    val numReducers = numMappers

    val pairs1 = spark.sparkContext.parallelize(0 until numMappers, numMappers).flatMap { p =>
      val ranGen = new Random
      val arr1 = new Array[(Int, Array[Byte])](numKVPairs)
      for (i <- 0 until numKVPairs) {
        val byteArr = new Array[Byte](valSize)
        ranGen.nextBytes(byteArr)
        arr1(i) = (ranGen.nextInt(Int.MaxValue), byteArr)
      }
      arr1
    }.cache()
    // Enforce that everything has been calculated and in cache
    pairs1.count()

    info(pairs1.groupByKey(numReducers).count().toString)
  

    spark.sparkContext.parallelize(0 until spark.sparkContext.defaultParallelism).foreach { i =>
      if (math.random > 0.75) {
        throw new Exception("Testing exception handling")
      }
    }

    val numEdges = 200
    val numVertices = 100
    val rand = new Random(42)

    def generateGraph: Seq[(Int, Int)] = {
      val edges: mutable.Set[(Int, Int)] = mutable.Set.empty
      while (edges.size < numEdges) {
        val from = rand.nextInt(numVertices)
        val to = rand.nextInt(numVertices)
        if (from != to) edges.+=((from, to))
      }
      edges.toSeq
    }

    val slices = 2
    var tc = spark.sparkContext.parallelize(generateGraph, slices).cache()

    // Linear transitive closure: each round grows paths by one edge,
    // by joining the graph's edges with the already-discovered paths.
    // e.g. join the path (y, z) from the TC with the edge (x, y) from
    // the graph to obtain the path (x, z).

    // Because join() joins on keys, the edges are stored in reversed order.
    val edges = tc.map(x => (x._2, x._1))

    // This join is iterated until a fixed point is reached.
    var oldCount = 0L
    var nextCount = tc.count()
    do {
      oldCount = nextCount
      // Perform the join, obtaining an RDD of (y, (z, x)) pairs,
      // then project the result to obtain the new (x, z) paths.
      tc = tc.union(tc.join(edges).map(x => (x._2._2, x._2._1))).distinct().cache()
      nextCount = tc.count()
    } while (nextCount != oldCount)

    info(s"TC has ${tc.count()} edges.")


    // $example on$
    val dfA = spark.createDataFrame(Seq(
      (0, Vectors.dense(1.0, 1.0)),
      (1, Vectors.dense(1.0, -1.0)),
      (2, Vectors.dense(-1.0, -1.0)),
      (3, Vectors.dense(-1.0, 1.0))
    )).toDF("id", "features")

    val dfB = spark.createDataFrame(Seq(
      (4, Vectors.dense(1.0, 0.0)),
      (5, Vectors.dense(-1.0, 0.0)),
      (6, Vectors.dense(0.0, 1.0)),
      (7, Vectors.dense(0.0, -1.0))
    )).toDF("id", "features")

    val key = Vectors.dense(1.0, 0.0)

    val brp = new BucketedRandomProjectionLSH()
      .setBucketLength(2.0)
      .setNumHashTables(3)
      .setInputCol("features")
      .setOutputCol("hashes")

    val model = brp.fit(dfA)

    println("The hashed dataset where hashed values are stored in the column 'hashes':")
    model.transform(dfA).show()

    println("Approximately joining dfA and dfB on Euclidean distance smaller than 1.5:")
    model.approxSimilarityJoin(dfA, dfB, 1.5, "EuclideanDistance")
      .select(col("datasetA.id").alias("idA"),
        col("datasetB.id").alias("idB"),
        col("EuclideanDistance")).show()

    println("Approximately searching dfA for 2 nearest neighbors of the key:")
    model.approxNearestNeighbors(dfA, key, 2).show()

    val splits = Array(Double.NegativeInfinity, -0.5, 0.0, 0.5, Double.PositiveInfinity)

    val data = Array(-999.9, -0.5, -0.3, 0.0, 0.2, 999.9)
    val dataFrame = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")

    val bucketizer = new Bucketizer()
      .setInputCol("features")
      .setOutputCol("bucketedFeatures")
      .setSplits(splits)

    // Transform original data into its bucket index.
    val bucketedData = bucketizer.transform(dataFrame)

    println(s"Bucketizer output with ${bucketizer.getSplits.length-1} buckets")
    bucketedData.show()
    // $example off$

    // $example on$
    val splitsArray = Array(
      Array(Double.NegativeInfinity, -0.5, 0.0, 0.5, Double.PositiveInfinity),
      Array(Double.NegativeInfinity, -0.3, 0.0, 0.3, Double.PositiveInfinity))

    val data2 = Array(
      (-999.9, -999.9),
      (-0.5, -0.2),
      (-0.3, -0.1),
      (0.0, 0.0),
      (0.2, 0.4),
      (999.9, 999.9))
    val dataFrame2 = spark.createDataFrame(data2).toDF("features1", "features2")

    val bucketizer2 = new Bucketizer()
      .setInputCols(Array("features1", "features2"))
      .setOutputCols(Array("bucketedFeatures1", "bucketedFeatures2"))
      .setSplitsArray(splitsArray)

    // Transform original data into its bucket index.
    val bucketedData2 = bucketizer2.transform(dataFrame2)

    println(s"Bucketizer output with [" +
      s"${bucketizer2.getSplitsArray(0).length-1}, " +
      s"${bucketizer2.getSplitsArray(1).length-1}] buckets for each input column")
    bucketedData2.show()

    // $example on$
    val vecData = Seq(
      Vectors.sparse(4, Seq((0, 1.0), (3, -2.0))),
      Vectors.dense(4.0, 5.0, 0.0, 3.0),
      Vectors.dense(6.0, 7.0, 0.0, 8.0),
      Vectors.sparse(4, Seq((0, 9.0), (3, 1.0)))
    )

    val df = vecData.map(Tuple1.apply).toDF("features")
    val Row(coeff1: Matrix) = Correlation.corr(df, "features").head
    println(s"Pearson correlation matrix:\n $coeff1")

    val Row(coeff2: Matrix) = Correlation.corr(df, "features", "spearman").head
    println(s"Spearman correlation matrix:\n $coeff2")
    // $example off$

    val elementWiseProductDF = spark.createDataFrame(
      Seq(
        ("a", Vectors.dense(1.0, 2.0, 3.0)),
        ("b", Vectors.dense(4.0, 5.0, 6.0)))).toDF("id", "vector")

    val transformingVector = Vectors.dense(0.0, 1.0, 2.0)
    val transformer = new ElementwiseProduct()
      .setScalingVec(transformingVector)
      .setInputCol("vector")
      .setOutputCol("transformedVector")

    transformer.transform(elementWiseProductDF).show()

    val hasherDF = spark.createDataFrame(Seq(
                                          (2.2, true, "1", "foo"),
                                          (3.3, false, "2", "bar"),
                                          (4.4, false, "3", "baz"),
                                          (5.5, false, "4", "foo")
                                        )).toDF("real", "bool", "stringNum", "string")

    val hasher = new FeatureHasher()
      .setInputCols("real", "bool", "stringNum", "string")
      .setOutputCol("features")

    val featurized = hasher.transform(hasherDF)
    featurized.show(false)
    // $example off$



  }
}
