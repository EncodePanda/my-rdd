package mr

import org.apache.spark.rdd._
import org.apache.spark._

import scala.reflect.ClassTag
import scala.util.Random

// trait Next[U] {
  // def next: U
// }

class RandomRDD(_sc: SparkContext, numberOfPartitions: Int) extends RDD[Int](_sc, Nil) {

  override def compute(split: Partition, context: TaskContext): Iterator[Int] =
    (1 to Random.nextInt(100)).toList.toIterator

  override protected def getPartitions: Array[Partition] = {
    val array = new Array[Partition](numberOfPartitions)
    for (i <- 0 until numberOfPartitions) {
      array(i) = new RandomPartition(i)
    }
    array
  }
}


class RandomPartition(val index: Int) extends Partition
