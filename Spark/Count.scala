import org.apache.spark.sql.SQLContext

val sqlContext = new SQLContext(sc)
val df = sqlContext.read.
format("com.databricks.spark.csv").
option("header", "true"). 
option("inferSchema", "true").
option("delimiter", "\t").
load("/user/part2_input/user_artists.dat")
val count=df.groupBy("artistID").sum("weight")
val sort=count.sort($"sum(weight)".desc)
sort.show()

sort.write.
format("com.databricks.spark.csv").
option("header", "true").
option("delimiter", "\t").
save("mini2.output")

sys.exit
