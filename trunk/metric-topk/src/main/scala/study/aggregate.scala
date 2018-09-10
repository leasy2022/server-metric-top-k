package study

import org.apache.spark.{SparkConf, SparkContext}

/*
 aggregate 方法的使用：先在executor的各个partiton上聚合；然后在driver中聚合

    Array(1,5,3,2,4).iterator.aggregate()

 */

object aggregate {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("es-data")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(Array(1,5,3,2,4),2)
    var res = rdd1.aggregate((0,0))(
      // seqOp
      (acc, number) => (acc._1+number, acc._2+1),
      // combOp
      (par1, par2) => {
        println("---1:" + par1._1 + "," +par1._1)
        println("---2:" + par2._1 + "," +par2._2)
        (par1._1+par2._1, par1._2*par2._2)
      }
    )
    println(res)
    sc.stop()
  }
}
