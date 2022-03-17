
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.{SaveMode, SparkSession}

object WordCountLocal {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    import spark.implicits._

//    spark.read.textFile("flink-spark/data/test.txt")
//      .rdd
//      .flatMap(_.split(" "))
//      .map((_, 1))
//      .reduceByKey(_ + _)
//      .saveAsTextFile("flink-spark/data/test2.txt")
    spark.read.textFile("flink-spark/data/test.txt")
      .flatMap(_.split(" "))
      .map((_, 1))
      .toDF("word","count")
      .select("word")
      .write
      .mode(SaveMode.Overwrite)
      .save("flink-spark/data/test2")
  }
}
