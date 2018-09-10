package newlens.metric.topk

/**
  * Created by wushang on 2017/3/29.
  */
case class TopQueue(topN: Int) {

  import scala.collection.mutable

  // Record结果的具体比较规则：如果是同一级别（mark）则比较count， 否则，根据mark确定大小
  implicit def order(r: Record) = new Ordered[Record] {
    def compare(other: Record) = {
      other.mark - r.mark match {
        case 0 => other.count - r.count
        case _ => other.mark - r.mark
      }
    }
  }

  val queue = new mutable.PriorityQueue[Record]()

  def add(record: Record) = {
    if (queue.size < topN) {
      queue.enqueue(record)
    } else {
      val o = queue.head
      (o.mark, record.mark) match { //mark是上次结果标识： 1，是上次结果；0，不是上次结果。 如果是1，直接放入队列；如果是0，需要比较count值
        case (1, 0) => {}
        case (0, 1) => {
          queue.dequeue()
          queue.enqueue(record)
        }
        case _ => {
          if (o.count < record.count) {
            queue.dequeue()
            queue.enqueue(record)
          }
        }
      }
    }
    this
  }

  def size() = queue.size

  def addAll(q: TopQueue) {
    while (!q.queue.isEmpty) {
      this.add(q.queue.dequeue())
    }
  }

  def toList() = {
    queue.toList
  }
}


case class Record(val content: String, val count: Int, val lastTimestamp: Int, val mark: Int)

case class TopResultSchema(appId: String, content: String, count: Int, lastTime: String, mTime: String)

case class Parameter(jdbcUrl: String, dbUser: String, dbPassword: String, tableName: String, batch:Int)

