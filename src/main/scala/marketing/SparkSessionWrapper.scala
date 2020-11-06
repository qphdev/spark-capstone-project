package marketing

import org.apache.spark.sql.SparkSession


trait SparkSessionWrapper extends Serializable {
  lazy val spark: SparkSession = SparkSession.builder()
    .appName("marketing")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", 4)
    .getOrCreate()
}
