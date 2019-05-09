package test

import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {

  lazy val spark: SparkSession = {
    SparkSession.builder()
      .master("local")
      .appName("spark session")
      .config("spark.databricks.service.client.autoAddDeps", "true")
      .getOrCreate()
  }
  lazy val sc = spark.sparkContext

  sc.addJar(f"${System.getProperty("user.dir")}/target/scala-2.11/dbconnect-test_2.11-0.0.1-tests.jar")

  sc.addJar(f"/home/${System.getProperty("user.name")}/.ivy2/cache/org.apache.spark/spark-core_2.11/jars/spark-core_2.11-2.4.0.jar")
}
