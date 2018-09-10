package newlens.metric.topk

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._

case class EsServerMetric(appId: String, content: String, count: Int, lastTimestamp: Int, insertTimestamp: Int, mark: Int)


object EsSink {
  def main(args: Array[String]) {

    var (master, appName, sourceDir, esHosts, port, esIndex, esType) = ("local[*]", "metric_topk", "/", " 127.0 .0 .1 ", " 9200", "server", "metric")
    if (args.length > 0) {
      master = args(0)
      appName = args(1)
      sourceDir = args(2)
      esHosts = args(3)
      port = args(4)
      esIndex = args(5)
      esType = args(6)
    }

    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster(master)
      .set("es.nodes", esHosts)
      .set("es.port", port)
      .set("es.index.auto.create", "true")

    val sc = new SparkContext(conf)

    val textRdd = sc.textFile(sourceDir)
    val esRdd = textRdd.map(_.split("\t", -1))
      .filter(_.length == 6)
      .map(line => EsServerMetric(line(0), line(1), line(2).toInt, line(3).toInt, line(4).toInt, line(5).toInt)).saveToEs(esIndex + "/" + esType)
    sc.stop()
  }
}
