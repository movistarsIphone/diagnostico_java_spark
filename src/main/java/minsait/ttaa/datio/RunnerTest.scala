package minsait.ttaa.datio

import minsait.ttaa.datio.common.Common.{HEADER, INFER_SCHEMA, INPUT_PATH}
import minsait.ttaa.datio.engine.Transformer

object RunnerTest {

  def main(args: Array[String]): Unit = {
    print("Prueba unitaria del metodo 4\n")
    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local[*]")
      .appName("RunnerTest")
      .getOrCreate;

    var df = spark.read.option(HEADER, true).option(INFER_SCHEMA, true).csv(INPUT_PATH);
    val engine = new Transformer()
    df = engine.TransformerPuntoCuatro(spark, df)
    df.show(500, false)
  }
}
