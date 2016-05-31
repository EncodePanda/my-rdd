package mr

import org.apache.spark.rdd._
import org.apache.spark._

import scala.reflect.ClassTag
import scala.util.Random._
import scala.reflect.runtime.universe._

trait NextRandom[T] {
  def next: T
}

object NextRandom {

  implicit object NextRandomInt extends NextRandom[Int] with Serializable {
    def next: Int = nextInt()
  }

  implicit object NextRandomString extends NextRandom[String] with Serializable {
    def next: String = nextString(100)
  }
}

object RandomRDD {
  implicit class SparkContextOps(sc: SparkContext) {
    def random[T](maxSize: Int = 100, numOfPartitions: Int = 2)(implicit next: NextRandom[T], classTag: ClassTag[T]): RandomRDD[T] =
      new RandomRDD[T](sc, maxSize, numOfPartitions)
  }
}

class RandomRDD[T](_sc: SparkContext, maxSize: Int = 100, numOfPartitions: Int = 2)(implicit next: NextRandom[T], classTag: ClassTag[T]) extends RDD[T](_sc, Nil) {

  override def compute(split: Partition, context: TaskContext): Iterator[T] =
    (1 to nextInt(maxSize / numOfPartitions)).map(n => next.next).toList.toIterator

  override protected def getPartitions: Array[Partition] = {
    val array = new Array[Partition](numOfPartitions)
    for (i <- 0 until numOfPartitions) {
      array(i) = new RandomPartition(i)
    }
    array
  }
}

class RandomPartition(val index: Int) extends Partition
