
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object TextCountApp {
def main(args: Array[String]) {

val logFile = "/home/spark/spark-1.6.0-bin-hadoop2.6/README.md"
val conf = new SparkConf().setAppName("TextCount Application").setMaster("local")
val sc = new SparkContext(conf)
val logData = sc.textFile(logFile, 2).cache()
val numAs = logData.filter(line => line.contains("a")).count()
val numBs = logData.filter(line => line.contains("b")).count()
val numSparks = logData.filter(line => line.contains("Spark")).count()
sc.stop
println("Lines with a: %s, Lines with b: %s, Lines with Spark: %s".format(numAs, numBs, numSparks))


}
}
