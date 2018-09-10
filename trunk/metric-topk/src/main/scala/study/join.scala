package study

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author wushang@tingyun.com
  * @date 2018/8/30
  */
object join {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("es-data")
    val sc = new SparkContext(conf)

    val a = sc.parallelize(Array(("1", 4.0), ("1", 5.0), ("2", 8.0), ("3", 9.0)))
    val b = sc.parallelize(Array(("1", 2.0), ("1", 3.0),("2", 8.0),("4", 9.0)))

//    val c = a.join(b)
//    val  c = a.leftOuterJoin(b)
    val  c = a.rightOuterJoin(b)

    c.foreach(println)

    //join 打印结果出来如下：
//    (2,(8.0,8.0))
//    (1,(4.0,2.0))
//    (1,(4.0,3.0))
//    (1,(5.0,2.0))
//    (1,(5.0,3.0))
    //这里返回的结果很显然是3匹配不到过滤掉，合并匹配到。


    /*leftOuterJoin
    (2,(8.0,Some(8.0)))
(3,(9.0,None))
(1,(4.0,Some(2.0)))
(1,(4.0,Some(3.0)))
(1,(5.0,Some(2.0)))
(1,(5.0,Some(3.0)))
     */

    /*rightOuterJoin
    (4,(None,9.0))
(2,(Some(8.0),8.0))
(1,(Some(4.0),2.0))
(1,(Some(4.0),3.0))
(1,(Some(5.0),2.0))
(1,(Some(5.0),3.0))
     */
  }
}
