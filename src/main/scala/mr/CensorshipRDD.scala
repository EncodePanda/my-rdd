package mr

import org.apache.spark.rdd._
import org.apache.spark._

import scala.reflect.ClassTag
import scala.util.Random._
import scala.reflect.runtime.universe._

object CensorshipRDD {
  implicit class StringRDDOps(rdd: RDD[String]) {
    def censor() = new CensorshipRDD(rdd)
  }
}

class CensorshipRDD(prev: RDD[String]) extends RDD[String](prev) {

  override def compute(split: Partition, context: TaskContext): Iterator[String] =
    firstParent[String].compute(split, context).map(str => str.replace("Hadoop", "******"))

  override protected def getPartitions: Array[Partition] = firstParent[String].partitions
}

