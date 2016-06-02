package mr

import org.apache.spark._
import scala.concurrent.Await
import slick.driver.H2Driver.api._
import slick.lifted.Tag
import scala.concurrent.duration._

object SchemaDef {
  case class Users(tag: Tag) extends Table[(Long, String, Int)](tag, "users") {

    def id = column[Long]("id", O.PrimaryKey)
    def name = column[String]("name")
    def age = column[Int]("age")

    def * = (id, name, age)

  }

  val users = TableQuery[Users]
}

object MainSlick extends App {
  import SchemaDef._

  implicit val usersId = new Id[Users] {
    def within(user: Users): Rep[Long] = user.id
   }

  val db = Database.forConfig("h2mem1")

  import scala.concurrent.ExecutionContext.Implicits.global

  val populate = DBIO.seq(
    users += ((1, "jimmy", 28)),
    users += ((2, "johnny", 19)),
    users += ((3, "laura", 19)),
    users += ((4, "tom", 27)),
    users += ((5, "kate", 31)),
    users += ((6, "monica", 31)),
    users += ((7, "adam", 31))
  )

  users.schema

  val setup = for {
    _ <- users.schema.create
    _ <- populate

  } yield ()

  Await.result(db.run(setup), 3 seconds)

  val sparkConf = new SparkConf()
    .setAppName(this.getClass.getName)
    .setMaster("local[*]")
  val sc = new SparkContext(sparkConf)

  new SLickRDD(sc, db, users, 2, 6, 2).collect().foreach(print)

  // val result = Await.result(db.run(action), 3 seconds)

  // result.foreach(println)

  db.close
  sc.stop()

}
