package study

import org.apache.spark.{SparkConf, SparkContext}

//参考：http://www.360doc.com/content/18/0818/07/57820247_779142954.shtml



object mapPartitons {
  class CustomIterator(iter: Iterator[Int]) extends Iterator[Int] {
    def hasNext: Boolean = {
      iter.hasNext
    }
    def next: Int = {
      val cur = iter.next
      cur * 3
    }
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("es-data")
    val sparkContext = new SparkContext(conf)

    val a = sparkContext.parallelize(1 to 20, 2)


    def terFunc(iter: Iterator[Int]): Iterator[Int] = {
      var res = List[Int]()
      while (iter.hasNext) {
        val cur = iter.next
        res.::=(cur * 3)
      }
      res.iterator
    }

    //传入的函数： 输入是一个迭代器，输出也是一个迭代器
    val result = a.mapPartitions(terFunc)
    println(result.collect().mkString(","))

    //2: 自定义迭代器，节省内存
    val result2 = a.mapPartitions(v => new CustomIterator(v))
    println(result2.collect().mkString(","))
  }
}
