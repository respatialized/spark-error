package test

import java.util.Random
import org.scalatest.FunSuite
import scala.collection.mutable

class Examples extends FunSuite with SparkSessionTestWrapper {

  test("dbconnect error") {

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


    case class Person(name: String, age: Long)

  private def runBasicDataFrameExample(spark: SparkSession): Unit = {
    // $example on:create_df$
    val df = spark.read.json("examples/src/main/resources/people.json")

    // Displays the content of the DataFrame to stdout
    df.show()
    // +----+-------+
    // | age|   name|
    // +----+-------+
    // |null|Michael|
    // |  30|   Andy|
    // |  19| Justin|
    // +----+-------+
    // $example off:create_df$

    // $example on:untyped_ops$
    // This import is needed to use the $-notation
    import spark.implicits._
    // Print the schema in a tree format
    df.printSchema()
    // root
    // |-- age: long (nullable = true)
    // |-- name: string (nullable = true)

    // Select only the "name" column
    df.select("name").show()
    // +-------+
    // |   name|
    // +-------+
    // |Michael|
    // |   Andy|
    // | Justin|
    // +-------+

    // Select everybody, but increment the age by 1
    df.select($"name", $"age" + 1).show()
    df.filter($"age" > 21).show()
    df.groupBy("age").count().show()

    df.createOrReplaceTempView("people")

    val sqlDF = spark.sql("SELECT * FROM people")
    sqlDF.show()
  }
}
