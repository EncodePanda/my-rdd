package mr

import org.apache.spark._

object Main extends App {

  val sparkConf = new SparkConf()
    .setAppName(this.getClass.getName)
    .setMaster("local[*]")
  val sc = new SparkContext(sparkConf)

  new RandomRDD(sc)
    .map(_ + 1)
    .filter(_ > 50)
    .collect
    .foreach(println)

  sc.stop()
}
