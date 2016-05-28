package mr

import org.apache.spark._

object Main extends App {

  import RandomRDD._
  import NextRandom._

  val sparkConf = new SparkConf()
    .setAppName(this.getClass.getName)
    .setMaster("local[*]")
  val sc = new SparkContext(sparkConf)

  sc.random[Int]()
    .map(_ + 1)
    .filter(_ > 50)
    .take(5)
    .foreach(println)

  sc.stop()
}
