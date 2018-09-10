package newlens.metric.topk

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.spark.sql.SparkSession
;

/*
原始文件格式:  appId metric count
写入es中文件格式:  appId content count lastTimestamp(最近出现的时间), insertTimestamp, isInLastBatch(在上一批次出现了为1, 本批数据产生的为0)
保留本地结果的文件格式: appId content count lastTime(最近出现的时间), updateTime(更新时间)

有SErver-dc将所有客户的Metric数据打入到Kafka中，由Flume接收kafka的数据，并根据时间，以每小时为周期存入hdfs。
并且，每小时执行一次 TopProcessor计算MetricTopK。 每次执行时需要指定当前时刻的sourceDir以及其他目录。例如（/data/source/2018-08-27-15）
原始数据：运维的hdfs目录
中间处理：读取原始数据+读取上次结果数据 -》求topK-》

聚合后数据保存到hdfs目录中（es目录）、当前结果（写入hdfs本次结果+Mysql）

*/
object TopProcessor {

  val dataFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def main(args: Array[String]) {

    if (args.length != 12) {
      println("###: check the args, expected length:12, but is:" + args.length)
      return;
    }
    val (sourceDir, lastResultDir, esDir, curResultDir, nowTimestamp, jdbcUrl, user, password, tableName, insertBatch, topK, maxMetricLength) =
      (args(0), args(1), args(2), args(3), args(4).toInt, args(5), args(6), args(7), args(8), args(9).toInt, args(10).toInt, args(11).toInt)
    val spark = SparkSession.builder().getOrCreate()

    println("##[ sourceDir(读取原始数据)=" + sourceDir + " ]\n" +
      "##[ lastResultDir(读取上次结果数据)=" + lastResultDir + " ]\n" +
      "##[ esDir(topK之外的数据)=" + esDir + " ]\n" +
      "##[ curResultDir(当前结果目录）=" + curResultDir + " ]\n" +
      "##[ timestamp=" + nowTimestamp + " ]\n" +
      "##[ jdbcUrl=" + jdbcUrl + " ]\n" +
      "##[ user=" + user + " ]\n" +
      "##[ password=" + password + " ]\n" +
      "##[ tableName=" + tableName + " ]\n" +
      "##[ insertBatch=" + insertBatch + " ]\n" +
      "##[ topK=" + topK + " ]\n" +
      "##[ maxMetricLength=" + maxMetricLength + " ]\n"
    )
    /*
    #for debug
        val sourceDir= "/Users/wushang/Downloads/spark/toytest/action-external-metrics/rawdata/2017041110"
        val lastResultDir="/Users/wushang/Downloads/spark/toytest/action-external-metrics/aggr/20170411/09"
        val esDir="/Users/wushang/Downloads/spark/toytest/action-external-metrics/es/20170411/10"
        val curResultDir="/Users/wushang/Downloads/spark/toytest/action-external-metrics/aggr/20170411/10"
        val nowTimestamp=1491876000
        val topK=3
    */
    //传入要读取的数据时间,用于定位需要读取的文件,
    val expireTimestamp = nowTimestamp - 24 * 60 * 60
    val updateTime = dataFormat.format(nowTimestamp * 1000L)

    val textRdd = spark.sparkContext.textFile(sourceDir)
    //appId content count lastTime(最近出现的时间), updateTime(更新时间)
    val sourceRDD = textRdd.map(_.split("\t", -1)).filter(_.length == 3).filter(line => line(1).length < maxMetricLength)
      .map(arr => ((arr(0).trim, arr(1).trim), (arr(2).trim.toInt, 0, nowTimestamp, 0))) // appId content count

    val lastResDs = spark.sparkContext.textFile(lastResultDir).repartition(20).map(_.split("\t", -1)).filter(_.length == 5).filter(arr => (arr(3).trim.toInt > expireTimestamp)) //如果为空
      .map(arr => ((arr(0).trim, arr(1).trim), (0, arr(2).trim.toInt, arr(3).trim.toInt, 1)))

    val unionRDD = sourceRDD.union(lastResDs)
      .reduceByKey((a, b) => {
        (a._1 + b._1, a._2 + b._2, Math.max(a._3, b._3), Math.max(a._4, b._4))
      }).map(line => {
      if (line._2._1 == 0)
        ((line._1._1, line._1._2), (line._2._2, line._2._2, line._2._3, line._2._4))
      else
        ((line._1._1, line._1._2), (line._2._1, line._2._2, line._2._3, line._2._4))
       //line._1._1 是 appId，
    }).map(line => ((line._1._1), Record(line._1._2, line._2._1, line._2._3, line._2._4)))
    unionRDD.persist()

    //保存聚合后的全量数据到esDir目录
    unionRDD.filter(_._2.count > 0).map(line => {
      val sb = new StringBuilder
      sb ++= line._1 ++= "\t" ++= line._2.content ++= "\t" ++= line._2.count.toString ++ "\t" ++= line._2.lastTimestamp.toString ++ "\t" ++ nowTimestamp.toString ++ "\t" ++= line._2.mark.toString
    }).saveAsTextFile(esDir)

    // 根据UnionRDD（聚合后的数据）求topK逻辑
    val topRDD = unionRDD.combineByKey(
      v => {
        val queue = new TopQueue(topK)
        queue.add(v)
        queue
      },
      (c: TopQueue, v: Record) => {
        c.add(v)
        c
      },
      (c1: TopQueue, c2: TopQueue) => {
        if (c1.size() > c2.size()) {
          c1.addAll(c2)
          c1
        } else {
          c2.addAll(c1)
          c2
        }
      }
    ).map(line => (line._1, line._2.toList())).flatMapValues(line => line)


    topRDD.cache()
    unionRDD.unpersist()
    //topN的结果保存到文件中
    topRDD.repartition(20).map(line => {
      val sb = new StringBuilder
      sb ++= line._1 ++= "\t" ++= line._2.content ++= "\t" ++= line._2.count.toString ++= "\t" ++= line._2.lastTimestamp.toString ++= "\t" ++= nowTimestamp.toString
    }).saveAsTextFile(curResultDir)

    import spark.implicits._
    //topK（topRDD）数据写入mysql
    val result = topRDD.map(attr => {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val lastTime = df.format(new Date(attr._2.lastTimestamp * 1000L))
      TopResultSchema(attr._1, attr._2.content, attr._2.count, lastTime, updateTime)
    })

    ResultSaver.save(result, Parameter(jdbcUrl, user, password, tableName, insertBatch))

    spark.stop()
  }
}
