package minsait.ttaa.datio

import minsait.ttaa.datio.common.Common.{HEADER, INFER_SCHEMA, INPUT_PATH, PRUEBA_UNITARIA_4, SPARK_MODE}
import minsait.ttaa.datio.engine.Transformer
import org.apache.spark.sql.SparkSession

object RunnerTest {

  def main(args: Array[String]): Unit = {
    print(PRUEBA_UNITARIA_4)
    val spark = SparkSession.builder.master(SPARK_MODE).getOrCreate

    var df = spark.read.option(HEADER, true).option(INFER_SCHEMA, true).csv(INPUT_PATH);
    val engine = new Transformer()
    df = engine.TransformerPuntoCuatro(spark, df)
    df.show(500, false)
  }
}
