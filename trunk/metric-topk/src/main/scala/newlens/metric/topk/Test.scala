package newlens.metric.topk

import org.apache.spark.{SparkConf, SparkContext, TaskContext}

/**
  * Created by wushang on 2018/1/5.
  * 2016-12-01	4407	0	97189	1525066380	0
  * |           245 |  24907175 |
  * |          6245 |   7778287 |
  * |              25863 |    517435 |
  * |         21320 |    542542 |
  *
  */
object Test {
  def main(args: Array[String]) {
    val sourceDir = args(0)
    val destDir = args(1)
    val appIdIn = args(2).toInt
    //    val sourceDir = "/Users/wushang/Downloads/aaa,/Users/wushang/Downloads/aab"
    //    val destDir = "/Users/wushang/Downloads/ccc"
    print("##source_dir=" + sourceDir)
    print("##dest_dir=" + destDir)
    print("##appId=" + appIdIn)


    val conf = new SparkConf().setMaster("local").setAppName("es-data")
    val sc = new SparkContext(conf)
    val textRdd = sc.textFile(sourceDir)
    sc.doubleAccumulator("")
    val rdd1 = textRdd.map(_.split("\t")).filter(_.length == 6).map(arr => (arr(0), arr(1),  arr(3),arr(4) ))
    val rdd2 = rdd1.filter(
      line => {
        val appId = line._2.toInt
        appId == appIdIn
      }
    )
    //    println("-----count=" + rdd2.count())
    val rdd3 = rdd2.filter(line => {
      val date = line._1
      date.startsWith("2017-12")
    })
    //    println("-----count3=" + rdd3.count())



    val rdd4 = rdd3.map(line => line._1 + "\t" + line._2 + "\t" + line._3 + "\t" + line._4).repartition(5).saveAsTextFile(destDir)
  }
}
