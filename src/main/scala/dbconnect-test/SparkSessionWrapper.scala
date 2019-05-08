package test

import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper extends Serializable {

  lazy val ss: SparkSession = {
    SparkSession.builder().master("local").appName("spark session").getOrCreate()
  }

  lazy val sqlc = ss.sqlContext
  lazy val sc = ss.sparkContext

}
