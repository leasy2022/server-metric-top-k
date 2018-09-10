package newlens.metric.topk

import java.sql.Connection

import org.apache.spark.rdd.RDD
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer


/**
  * Created by wushang on 2017/4/28.
  */
object ResultSaver {
  private val logger: Logger = LoggerFactory.getLogger(ResultSaver.getClass)

  def singleSave(conn: Connection, sql: String): Unit = {
    val index = sql.indexOf("values ")
    val sqlPrefix = sql.substring(0, index + 7)
    var sqlRemain = sql.substring(index + 7)
    var index2 = sqlRemain.indexOf("),(")

    while (index2 > 0) {
      val sqlSuffix = sqlRemain.substring(0, index2 + 1)
      val sql = sqlPrefix + sqlSuffix
      try {
        val statement = conn.prepareStatement(sql)
        statement.execute()
        statement.close()
      } catch {
        case ex => {
          logger.error("insert error: ", ex)
          logger.error("error sql:" + sql)
        }
      }
      sqlRemain = sqlRemain.substring(index2 + 2)
      index2 = sqlRemain.indexOf("),(")
    }

    if (sqlRemain.length > 0) {
      val sql = sqlPrefix + sqlRemain
      try {
        val statement = conn.prepareStatement(sqlPrefix + sqlRemain)
        statement.execute()
        statement.close()
      } catch {
        case ex => {
          logger.error("insert error: ", ex)
          logger.error("error sql:" + sql)
        }
      }
    }
  }

  //每次都先trunk历史数据，然后写入结果数据
  def save(rdd: RDD[_], parameter: Parameter) {
    val batchSize = parameter.batch
    val conn = MysqlManager.getMysqlManager(parameter).getConnection
    val statement = conn.prepareStatement("truncate table " + parameter.tableName)
    statement.execute();
    statement.close();
    conn.close();

    if (!rdd.isEmpty()) {
      rdd.foreachPartition(partitionRecords => {
        var sql = new StringBuffer()
        sql.append("insert into ").append(parameter.tableName).append(" (`appId`, `content`, `count`, `lastTime`, `mTime`) values ")

        val buffer = new ArrayBuffer[TopResultSchema]()
        val conn = MysqlManager.getMysqlManager(parameter).getConnection
        conn.setAutoCommit(false)
        try {
          var i = 0
          partitionRecords.foreach(
            r => {
              val record = r.asInstanceOf[TopResultSchema]
              buffer += record
              sql.append("(").append("\"").append(record.appId).append("\"").append(",")
                .append("\"").append(record.content).append("\"").append(",")
                .append(record.count).append(",")
                .append("\"").append(record.lastTime).append("\"").append(",")
                .append("\"").append(record.mTime).append("\"").append(")").append(",")
              i += 1
              if (i == batchSize) {
                sql.deleteCharAt(sql.length - 1)
                try {
                  val statement = conn.prepareStatement(sql.toString())
                  statement.execute()
                  statement.close()
                } catch {
                  case ex: Exception =>
                    singleSave(conn, sql.toString)
                }
                sql = new StringBuffer();
                sql.append("insert into ").append(parameter.tableName).append(" (`appId`, `content`, `count`, `lastTime`, `mTime`) values ")
                i = 0
              }
            }
          )
          if (i != 0) {
            sql.deleteCharAt(sql.length - 1)
            try {
              val statement = conn.prepareStatement(sql.toString())
              statement.execute()
              statement.close()
            } catch {
              case ex: Exception =>
                singleSave(conn, sql.toString)
            }
          }
        } catch {
          case e: Exception => {
            conn.rollback()
            throw new Exception(e)
          }
        } finally {
          conn.setAutoCommit(true)
          conn.close()
        }

      })
    }
  }
}
