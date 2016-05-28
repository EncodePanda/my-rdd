package mr

import org.apache.spark.rdd._
import org.apache.spark._

import scala.reflect.ClassTag
import scala.util.Random

class RandomRDD(_sc: SparkContext, maxSize: Int = 100, numOfPartitions: Int = 2) extends RDD[Int](_sc, Nil) {

  override def compute(split: Partition, context: TaskContext): Iterator[Int] =
    (1 to Random.nextInt(maxSize / numOfPartitions)).toList.toIterator

  override protected def getPartitions: Array[Partition] = {
    val array = new Array[Partition](numOfPartitions)
    for (i <- 0 until numOfPartitions) {
      array(i) = new RandomPartition(i)
    }
    array
  }
}


class RandomPartition(val index: Int) extends Partition
