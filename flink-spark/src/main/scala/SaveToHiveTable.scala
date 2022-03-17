import org.apache.spark.sql.SparkSession

/**
 * 读取本地文件写入Hive表中
 */
object SaveToHiveTable {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      // 开启Hive支持
//      .enableHiveSupport()
      // 配置Hivewarehouse地址
//      .config("spark.sql.warehouse.dir","hdfs:///user/hive/warehouse")
      .getOrCreate()
    // 读取文件
    val rawRdd = spark.sparkContext.textFile("file:///home/hadoop/data/spark/course.txt")
    import spark.implicits._
    val frame = rawRdd.map(line => {
      val splits = line.split(",")
      (splits(0), splits(1), splits(2))
    }).toDF("id", "cour", "score")
    // 创建临时表
    frame.createOrReplaceTempView("score")
    // 采用Insert into的方式可以避免发生FileFormart的错误
    spark.sql("insert into spark.course select id,cour,score from score")
  }
}
