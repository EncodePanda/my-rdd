package mr

import org.apache.spark._

object Main extends App {

  val sparkConf = new SparkConf()
    .setAppName(this.getClass.getName)
    .setMaster("local[*]")
  val sc = new SparkContext(sparkConf)

  new RandomRDD(sc, 10).collect.foreach(println)


  sc.stop()
}
