// Question 1 - Part A: Write a function to load the file customer.txt and create an RDD
import spark.implicits._
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, LongType}
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD

val readFile = (loc: String) => { val rdd = sc.textFile(loc)
     | rdd
     | }

val rdd = readFile("customers.txt")
rdd.toDF.show()

// Question 1 - Part B: Programmatically define the schema of the data from a list (use map API)
def dfSchema(columnNames: List[String]): StructType =
  StructType(
    Seq(
      StructField(name = columnNames(0), dataType = IntegerType, nullable = false),
      StructField(name = columnNames(1), dataType = StringType, nullable = false),
      StructField(name = columnNames(2), dataType = StringType, nullable = false),
      StructField(name = columnNames(3), dataType = StringType, nullable = false),
      StructField(name = columnNames(4), dataType = IntegerType, nullable = false)
    )
  )
val schema = dfSchema(List("ID", "Name", "City", "State", "Pin"))

// Question 1 - Part C: Apply the schema to the RDD to create a DataFrame
val rowRDD = rdd.map(_.split(", ")).map(p => Row(p(0).toInt, p(1), p(2),p(3),p(4).toInt))
val df = spark.createDataFrame(rowRDD, schema)

// Question 1 - Part D: Display the names of the customer in the sqlcontext
df.select("Name").show()

// Question 1 - Part E: Add in a new column “Like” to the dataframe with a default value 
val df2 = df.withColumn("Like", lit("Default_Val"))
df2.show()


//---------------------------------------------------------

// Question 2 - Part A: Write a function to load the file log.txt in an RDD, use a case class to map the file fields
case class Logs(cat: String, datetime: String, id: String, stage: String, info: String);
def createLogs(loc: String): RDD[Logs] = {
	val rdd = sc.textFile(loc)
	val logs = rdd.map(line => line.replaceAll(" -- ", ", ")).map(line => line.replaceAll(".rb: ", ", ")).
	map(line => line.replaceAll(", ghtorrent-", ","))
	val logsrdd = logs.map(_.split(",", 5)).flatMap{ row => 
    if (row.length > 2) {
        Some(Logs(row(0), row(1), row(2), row(3), row(4)))
    } else {
        None
    }
}
logsrdd
}

// Question 2 - Part B: Transform RDD to dataframe
val logsrdd = createLogs("logs.txt") 
val logsdf = logsrdd.toDF

// Question 2 - Part C: How many lines does the df contain?
val count = logsdf.count

// Question 2 - Part D: Count the number of WARNing messages
val warn = logsrdd.filter(r => r.cat == "WARN").count

// Question 2 - Part E: Which client did most HTTP requests?
val http = logsrdd.filter(_.stage == " api_client").keyBy(_.id).
    mapValues(k => 1).reduceByKey(_+_).
    sortBy(_._2, false).
    take(1)

// Question 2 - Part F: Which client did most FAILED HTTP requests?
val failedhttp = logsrdd.filter(_.stage == " api_client").filter(_.info.contains("Failed")).
    keyBy(_.id).mapValues(k => 1).reduceByKey(_+_).
    sortBy(_._2, false).
    take(1)

// Question 2 - Part G: What is the most active hour of day?
val hour = logsrdd.map(_.datetime.substring(12, 14)).toDF("Hours")
val activeh = hour.groupBy("Hours").count().sort(col("count").desc).take(1)

// Question 2 - Part H: What is the most active repository?
val repos = logsrdd.filter(_.stage == " api_client").map(_.info.split("/").slice(4,6).mkString("/").takeWhile(_ != '?'))
val activer = repos.filter(_.nonEmpty).map(l => (l,1)).reduceByKey(_+_).sortBy(x => x._2, false).take(1)

// Question 2 - Part I: Which access keys are failing most often?
val fail_keys = logsrdd.filter(_.info.contains("Failed")).
    filter(_.info.contains("Access: ")).map(_.info.split("Access: ", 2)(1).split(",", 2)(0)).
    map(x => (x, 1)).reduceByKey(_+_).
    sortBy(_._2, false).
    take(1)

// -----------------------------------------------------------------

// Question 3 - Part A: Read in the CSV file to an RDD (called interesting). How many records are there?
case class Repo(id: Integer, url: String, owner_id: Integer, 
name: String, language: String, created_at: String, forked_from: String, deleted: Integer, updated_at: String)
val rddtemp = sc.textFile("important-repos.csv")
val header = rddtemp.first()
val rdd = rddtemp.filter(row => row!=header)
val interesting = rdd.map(_.split(",")).flatMap{ row => 
        Some(Repo(row(0).toInt, row(1), row(2).toInt, row(3), row(4), row(5), row(6), row(7).toInt, row(8)))
}
val records = interesting.count()

// Question 3 - Part B: Read in the interesting repos file using Spark’s CSV parser as dataframe
val interestingdf = spark.read.option(“header”,“true”).csv("important-repos.csv")

// Question 3 - Part C: Transform log RDD in Question 1 to dataframe by defining schema  programmatically
def dfSchema(cols: List[String]): StructType =
  StructType(
    Seq(
      StructField(name = cols(0), dataType = IntegerType, nullable = true),
      StructField(name = cols(1), dataType = StringType,  nullable = true),
      StructField(name = cols(2), dataType = IntegerType, nullable = true),
      StructField(name = cols(3), dataType = StringType,  nullable = true),
      StructField(name = cols(4), dataType = StringType,  nullable = true),
      StructField(name = cols(5), dataType = StringType,  nullable = true),
      StructField(name = cols(6), dataType = StringType,  nullable = true),
      StructField(name = cols(7), dataType = IntegerType, nullable = true),
      StructField(name = cols(8), dataType = StringType,  nullable = true),
    )
  )
val schema = dfSchema(header.split(",").map(_.trim).toList)
val rowRDD = rdd.map(_.split(",")).map(row => Row(row(0).toInt, row(1), row(2).toInt, 
	row(3), row(4), row(5), row(6), row(7).toInt, row(8)))
val df = spark.createDataFrame(rowRDD, schema)
df.show()


// Question 3 - Part D: How many records in the log file refer to entries in the interesting file?
val interestingR = interesting.keyBy(_.name)
val logsR = logsrdd.keyBy(_.info).map(r => r.copy(_1 = r._1.split("/").slice(4,6).mkString("/")
	.takeWhile(_ != '?').split("/", 2).last)).filter(_._1.nonEmpty)
val joinedRDD = interestingR.join(logsR)
val num_entries = joinedRDD.count

// Question 3 - Part E: Which of the interesting repositories has the most failed API calls?
val failedAPI = joinedRDD.filter(r => r._2._2.info.contains("Failed")).
map(r => (r._1, 1)).reduceByKey(_+_).sortBy(_._2, false).take(1)


