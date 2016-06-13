package mr

import org.apache.spark._

object Main extends App {

  import RandomRDD._
  import NextRandom._

  val sparkConf = new SparkConf()
    .setAppName(this.getClass.getName)
    .setMaster("local[*]")
  val sc = new SparkContext(sparkConf)

  // random ints
  sc.random[Int]()
    .map(_ + 1)
    .filter(_ > 50)
    .take(5)
    // .foreach(println)

  // random strings
  sc.random[String]()
    .map(_ + "!")
    .filter(_.size > 70)
    .take(5)
    // .foreach(println)

  import CensorshipRDD._

  sc.parallelize(List("something", "Hadoop", "else"))
    .censor().collect().foreach(println)

  sc.stop()
}
