package mr

import scala.concurrent.Await
import scala.concurrent.duration._
import slick.driver.H2Driver.api._
import slick.driver.H2Driver.backend.DatabaseDef
import org.apache.spark.rdd._
import org.apache.spark._
import scala.reflect.ClassTag
import slick.dbio.{DBIOAction, Effect, NoStream}
import scala.concurrent.ExecutionContext.Implicits.global
import slick.lifted.TableQuery

class Id[T] {
  def id(t: T): Rep[Long]
}

class SLickRDD[T](
  _sc: SparkContext,
  db: DatabaseDef,
  query: TableQuery[T],
  lowerBound: Long,
  upperBound: Long,
  numPartitions: Int = 2
)(implicit classTag: ClassTag[T], id: Id[T]) extends RDD[T](_sc, Nil) {

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val partition = split.asInstanceOf[SlickPartition[T]]
    val action = for {
      res <- query.filter(t => id.id(t) >= upperBound && id.id(t) <= upperBound).result
    } yield res

    Await.result(db.run(action), 3 seconds).toIterator

  }

  override def getPartitions: Array[Partition] = {
    val length = BigInt(1) + upperBound - lowerBound
    (0 until numPartitions).map { i =>
      val start = lowerBound + ((i * length) / numPartitions)
      val end = lowerBound + (((i + 1) * length) / numPartitions) - 1
      new SlickPartition(i, start.toLong, end.toLong)
    }.toArray
  }

}

class SlickPartition[T](val index: Int, from: Long, to: Long) extends Partition
