package study

import org.apache.spark.{SparkConf, SparkContext}


object top {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("es-data")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(Array("zhang", "li", "li", "zhang", "li", "wu"))

    val sortWord = rdd1
      .map(x => (x, 1))
      .reduceByKey((v1, v2) => v1 + v2)
      .filter(x => x._1 != "")
      .sortBy(x => x._2, false, 1)
      .top(1)
      .foreach(println)


    sc.stop()
  }
}
