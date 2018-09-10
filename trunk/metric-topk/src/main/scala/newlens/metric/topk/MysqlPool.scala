package newlens.metric.topk

import java.sql.{Connection, DriverManager}
import java.util.Properties

import org.logicalcobwebs.proxool.ProxoolFacade

/**
  * Created by wushang on 2017/4/27.
  */
class MysqlPool(parameter: Parameter) extends Serializable {

  private[this] val info = new Properties();
  Class.forName("org.logicalcobwebs.proxool.ProxoolDriver")
  info.setProperty("proxool.maximum-connection-count", "70");
  info.setProperty("proxool.minimum-connection-count", "20");
  info.setProperty("proxool.maximum-connection-lifetime", "18000000");
  info.setProperty("proxool.house-keeping-test-sql", "select CURRENT_DATE");
  info.setProperty("user", parameter.dbUser);
  info.setProperty("password", parameter.dbPassword);
  val alias = "newlens";
  val driverClass = "com.mysql.jdbc.Driver";
  //  val driverUrl = "jdbc:mysql://127.0.0.1:3306/mytest?useUnicode=true&amp;characterEncoding=utf8&amp;useSSL=false";
  val driverUrl = parameter.jdbcUrl;
  val url = "proxool." + alias + ":" + driverClass + ":" + driverUrl;
  ProxoolFacade.registerConnectionPool(url, info)

  def getConnection: Connection = {
    try {
      DriverManager.getConnection("proxool.newlens");
    } catch {
      case ex: Exception =>
        throw new Exception(ex)
    }
  }
}

object MysqlManager {
  @volatile var mysqlManager: MysqlPool = _

  def getMysqlManager(parameter: Parameter): MysqlPool = {
    if (mysqlManager == null) {
      synchronized {
        if (mysqlManager == null) {
          mysqlManager = new MysqlPool(parameter)
        }
      }
    }
    mysqlManager
  }
}