# Databricks notebook source
# MAGIC %md ## Chapter 5: Spark SQL and DataFrames: Interacting with External Data Sources
# MAGIC This notebook contains for code samples for *Chapter 5: Spark SQL and DataFrames: Interacting with External Data Sources*.

# COMMAND ----------

# MAGIC %md ### User Defined Functions
# MAGIC While Apache Spark has a plethora of functions, the flexibility of Spark allows for data engineers and data scientists to define their own functions (i.e., user-defined functions or UDFs).  

# COMMAND ----------

# MAGIC %scala
# MAGIC // Create cubed function
# MAGIC val cubed = (s: Long) => {
# MAGIC   s * s * s
# MAGIC }
# MAGIC 
# MAGIC // Register UDF
# MAGIC spark.udf.register("cubed", cubed)
# MAGIC 
# MAGIC // Create temporary view
# MAGIC spark.range(1, 9).createOrReplaceTempView("udf_test")

# COMMAND ----------

# MAGIC %python
# MAGIC from pyspark.sql.types import LongType
# MAGIC 
# MAGIC # Create cubed function
# MAGIC def cubed(s):
# MAGIC   return s * s * s
# MAGIC 
# MAGIC # Register UDF
# MAGIC spark.udf.register("cubed", cubed, LongType())
# MAGIC 
# MAGIC # Generate temporary view
# MAGIC spark.range(1, 9).createOrReplaceTempView("udf_test")

# COMMAND ----------

spark.sql("SELECT id, cubed(id) AS id_cubed FROM udf_test").show()

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.sql("SELECT id, cubed(id) AS id_cubed FROM udf_test").show()

# COMMAND ----------

# MAGIC %md ## Speeding up and Distributing PySpark UDFs with Pandas UDFs
# MAGIC One of the previous prevailing issues with using PySpark UDFs was that it had slower performance than Scala UDFs.  This was because the PySpark UDFs required data movement between the JVM and Python working which was quite expensive.   To resolve this problem, pandas UDFs (also known as vectorized UDFs) were introduced as part of Apache Spark 2.3. It is a UDF that uses Apache Arrow to transfer data and utilizes pandas to work with the data. You simplify define a pandas UDF using the keyword pandas_udf as the decorator or to wrap the function itself.   Once the data is in Apache Arrow format, there is no longer the need to serialize/pickle the data as it is already in a format consumable by the Python process.  Instead of operating on individual inputs row-by-row, you are operating on a pandas series or dataframe (i.e. vectorized execution).

# COMMAND ----------

# ALL_NOTEBOOKS
import pandas as pd
# Import various pyspark SQL functions including pandas_udf
from pyspark.sql.functions import col, pandas_udf
from pyspark.sql.types import LongType

# Declare the cubed function 
def cubed(a: pd.Series) -> pd.Series:
    return a * a * a

# Create the pandas UDF for the cubed function 
cubed_udf = pandas_udf(cubed, returnType=LongType())

# COMMAND ----------

# MAGIC %md ### Using pandas dataframe

# COMMAND ----------

# ALL_NOTEBOOKS
# Create a Pandas series
x = pd.Series([1, 2, 3])

# The function for a pandas_udf executed with local Pandas data
print(cubed(x))

# COMMAND ----------

# MAGIC %md ### Using Spark DataFrame

# COMMAND ----------

# ALL_NOTEBOOKS
# Create a Spark DataFrame
df = spark.range(1, 4)

# Execute function as a Spark vectorized UDF
df.select("id", cubed_udf(col("id"))).show()

# COMMAND ----------

# MAGIC %md ## Higher Order Functions in DataFrames and Spark SQL
# MAGIC 
# MAGIC Because complex data types are an amalgamation of simple data types, it is tempting to manipulate complex data types directly. As noted in the post *Introducing New Built-in and Higher-Order Functions for Complex Data Types in Apache Spark 2.4* there are typically two solutions for the manipulation of complex data types.
# MAGIC 1. Exploding the nested structure into individual rows, applying some function, and then re-creating the nested structure as noted in the code snippet below (see Option 1)  
# MAGIC 1. Building a User Defined Function (UDF)

# COMMAND ----------

# MAGIC %scala
# MAGIC // Create an array dataset
# MAGIC val arrayData = Seq(
# MAGIC   Row(1, List(1, 2, 3)),
# MAGIC   Row(2, List(2, 3, 4)),
# MAGIC   Row(3, List(3, 4, 5))
# MAGIC )
# MAGIC 
# MAGIC // Create schema
# MAGIC import org.apache.spark.sql._
# MAGIC import org.apache.spark.sql.types._
# MAGIC 
# MAGIC val arraySchema = new StructType()
# MAGIC   .add("id", IntegerType)
# MAGIC   .add("values", ArrayType(IntegerType))
# MAGIC 
# MAGIC // Create DataFrame
# MAGIC val df = spark.createDataFrame(spark.sparkContext.parallelize(arrayData), arraySchema)
# MAGIC df.createOrReplaceTempView("table")
# MAGIC df.printSchema()
# MAGIC df.show()

# COMMAND ----------

# MAGIC %python
# MAGIC # Create an array dataset
# MAGIC arrayData = [[1, (1, 2, 3)], [2, (2, 3, 4)], [3, (3, 4, 5)]]
# MAGIC 
# MAGIC # Create schema
# MAGIC from pyspark.sql.types import *
# MAGIC arraySchema = (StructType([
# MAGIC       StructField("id", IntegerType(), True), 
# MAGIC       StructField("values", ArrayType(IntegerType()), True)
# MAGIC       ]))
# MAGIC 
# MAGIC # Create DataFrame
# MAGIC df = spark.createDataFrame(spark.sparkContext.parallelize(arrayData), arraySchema)
# MAGIC df.createOrReplaceTempView("table")
# MAGIC df.printSchema()
# MAGIC df.show()

# COMMAND ----------

# MAGIC %md #### Option 1: Explode and Collect
# MAGIC In this nested SQL statement, we first `explode(values)` which creates a new row (with the id) for each element (`value`) within values.  

# COMMAND ----------

spark.sql("""
SELECT id, collect_list(value + 1) AS newValues
  FROM  (SELECT id, explode(values) AS value
        FROM table) x
 GROUP BY id
""").show()

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.sql("""
# MAGIC SELECT id, collect_list(value + 1) AS newValues
# MAGIC   FROM  (SELECT id, explode(values) AS value
# MAGIC         FROM table) x
# MAGIC  GROUP BY id
# MAGIC """).show()

# COMMAND ----------

# MAGIC %md #### Option 2: User Defined Function
# MAGIC To perform the same task (adding a value of 1 to each element in `values`), we can also create a user defined function (UDF) that uses map to iterate through each element (`value`) to perform the addition operation.

# COMMAND ----------

# MAGIC %scala
# MAGIC // Create UDF
# MAGIC def addOne(values: Seq[Int]): Seq[Int] = {
# MAGIC     values.map(value => value + 1)
# MAGIC }
# MAGIC 
# MAGIC // Register UDF
# MAGIC val plusOneInt = spark.udf.register("plusOneInt", addOne(_: Seq[Int]): Seq[Int])
# MAGIC 
# MAGIC // Query data
# MAGIC spark.sql("SELECT id, plusOneInt(values) AS values FROM table").show()

# COMMAND ----------

# MAGIC %python
# MAGIC from pyspark.sql.types import IntegerType
# MAGIC from pyspark.sql.types import ArrayType
# MAGIC 
# MAGIC # Create UDF
# MAGIC def addOne(values):
# MAGIC   return [value + 1 for value in values]
# MAGIC 
# MAGIC # Register UDF
# MAGIC spark.udf.register("plusOneIntPy", addOne, ArrayType(IntegerType()))  
# MAGIC 
# MAGIC # Query data
# MAGIC spark.sql("SELECT id, plusOneIntPy(values) AS values FROM table").show()

# COMMAND ----------

# MAGIC %md ### Higher-Order Functions
# MAGIC In addition to the previously noted built-in functions, there are high-order functions that take anonymous lambda functions as arguments. 

# COMMAND ----------

# MAGIC %scala
# MAGIC // In Scala
# MAGIC // Create DataFrame with two rows of two arrays (tempc1, tempc2)
# MAGIC val t1 = Array(35, 36, 32, 30, 40, 42, 38)
# MAGIC val t2 = Array(31, 32, 34, 55, 56)
# MAGIC val tC = Seq(t1, t2).toDF("celsius")
# MAGIC tC.createOrReplaceTempView("tC")
# MAGIC 
# MAGIC // Show the DataFrame
# MAGIC tC.show()

# COMMAND ----------

# MAGIC %python
# MAGIC # In Python
# MAGIC from pyspark.sql.types import *
# MAGIC schema = StructType([StructField("celsius", ArrayType(IntegerType()))])
# MAGIC 
# MAGIC t_list = [[35, 36, 32, 30, 40, 42, 38]], [[31, 32, 34, 55, 56]]
# MAGIC t_c = spark.createDataFrame(t_list, schema)
# MAGIC t_c.createOrReplaceTempView("tC")
# MAGIC 
# MAGIC # Show the DataFrame
# MAGIC t_c.show()

# COMMAND ----------

# MAGIC %md #### Transform
# MAGIC 
# MAGIC `transform(array<T>, function<T, U>): array<U>`
# MAGIC 
# MAGIC The transform function produces an array by applying a function to each element of an input array (similar to a map function).

# COMMAND ----------

# Calculate Fahrenheit from Celsius for an array of temperatures
spark.sql("""SELECT celsius, transform(celsius, t -> ((t * 9) div 5) + 32) as fahrenheit FROM tC""").show()

# COMMAND ----------

# MAGIC %scala
# MAGIC // Calculate Fahrenheit from Celsius for an array of temperatures
# MAGIC spark.sql("""SELECT celsius, transform(celsius, t -> ((t * 9) div 5) + 32) as fahrenheit FROM tC""").show()

# COMMAND ----------

# MAGIC %md #### Filter
# MAGIC 
# MAGIC `filter(array<T>, function<T, Boolean>): array<T>`
# MAGIC 
# MAGIC The filter function produces an array where the boolean function is true.

# COMMAND ----------

# MAGIC %scala
# MAGIC // Filter temperatures > 38C for array of temperatures
# MAGIC spark.sql("""SELECT celsius, filter(celsius, t -> t > 38) as high FROM tC""").show()

# COMMAND ----------

# Filter temperatures > 38C for array of temperatures
spark.sql("""SELECT celsius, filter(celsius, t -> t > 38) as high FROM tC""").show()

# COMMAND ----------

# MAGIC %md #### Exists
# MAGIC 
# MAGIC `exists(array<T>, function<T, V, Boolean>): Boolean`
# MAGIC 
# MAGIC The exists function returns true if the boolean function holds for any element in the input array.

# COMMAND ----------

# MAGIC %scala
# MAGIC // Is there a temperature of 38C in the array of temperatures
# MAGIC spark.sql("""
# MAGIC SELECT celsius, exists(celsius, t -> t = 38) as threshold
# MAGIC FROM tC
# MAGIC """).show()

# COMMAND ----------

# Is there a temperature of 38C in the array of temperatures
spark.sql("""
SELECT celsius, exists(celsius, t -> t = 38) as threshold
FROM tC
""").show()

# COMMAND ----------

# MAGIC %md #### Reduce
# MAGIC 
# MAGIC `reduce(array<T>, B, function<B, T, B>, function<B, R>)`
# MAGIC 
# MAGIC The reduce function reduces the elements of the array to a single value  by merging the elements into a buffer B using function<B, T, B> and by applying a finishing function<B, R> on the final buffer.

# COMMAND ----------

# MAGIC %scala
# MAGIC // Calculate average temperature and convert to F
# MAGIC spark.sql("""
# MAGIC SELECT celsius, 
# MAGIC        reduce(
# MAGIC           celsius, 
# MAGIC           0, 
# MAGIC           (t, acc) -> t + acc, 
# MAGIC           acc -> (acc div size(celsius) * 9 div 5) + 32
# MAGIC         ) as avgFahrenheit 
# MAGIC   FROM tC
# MAGIC """).show()

# COMMAND ----------

# Calculate average temperature and convert to F
spark.sql("""
SELECT celsius, 
       reduce(
          celsius, 
          0, 
          (t, acc) -> t + acc, 
          acc -> (acc div size(celsius) * 9 div 5) + 32
        ) as avgFahrenheit 
  FROM tC
""").show()

# COMMAND ----------

# MAGIC %md ## DataFrames and Spark SQL Common Relational Operators
# MAGIC 
# MAGIC The power of Spark SQL is that it contains many DataFrame Operations (also known as Untyped Dataset Operations). 
# MAGIC 
# MAGIC For the full list, refer to [Spark SQL, Built-in Functions](https://spark.apache.org/docs/latest/api/sql/index.html).
# MAGIC 
# MAGIC In the next section, we will focus on the following common relational operators:
# MAGIC * Unions and Joins
# MAGIC * Windowing
# MAGIC * Modifications

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.functions._
# MAGIC 
# MAGIC // Set File Paths
# MAGIC val delaysPath = "/databricks-datasets/learning-spark-v2/flights/departuredelays.csv"
# MAGIC val airportsPath = "/databricks-datasets/learning-spark-v2/flights/airport-codes-na.txt"
# MAGIC 
# MAGIC // Obtain airports dataset
# MAGIC val airports = spark
# MAGIC   .read
# MAGIC   .options(
# MAGIC     Map(
# MAGIC       "header" -> "true", 
# MAGIC       "inferSchema" ->  "true", 
# MAGIC       "sep" -> "\t"))
# MAGIC   .csv(airportsPath)
# MAGIC 
# MAGIC airports.createOrReplaceTempView("airports_na")
# MAGIC 
# MAGIC // Obtain departure Delays data
# MAGIC val delays = spark
# MAGIC   .read
# MAGIC   .option("header","true")
# MAGIC   .csv(delaysPath)
# MAGIC   .withColumn("delay", expr("CAST(delay as INT) as delay"))
# MAGIC   .withColumn("distance", expr("CAST(distance as INT) as distance"))
# MAGIC 
# MAGIC delays.createOrReplaceTempView("departureDelays")
# MAGIC 
# MAGIC // Create temporary small table
# MAGIC val foo = delays
# MAGIC   .filter(
# MAGIC     expr("""
# MAGIC          origin == 'SEA' AND 
# MAGIC          destination == 'SFO' AND 
# MAGIC          date like '01010%' AND delay > 0
# MAGIC          """))
# MAGIC 
# MAGIC foo.createOrReplaceTempView("foo")

# COMMAND ----------

# MAGIC %python
# MAGIC from pyspark.sql.functions import expr
# MAGIC 
# MAGIC # Set File Paths
# MAGIC delays_path = "/databricks-datasets/learning-spark-v2/flights/departuredelays.csv"
# MAGIC airports_path = "/databricks-datasets/learning-spark-v2/flights/airport-codes-na.txt"
# MAGIC 
# MAGIC # Obtain airports dataset
# MAGIC airports = spark.read.options(header='true', inferSchema='true', sep='\t').csv(airports_path)
# MAGIC airports.createOrReplaceTempView("airports_na")
# MAGIC 
# MAGIC # Obtain departure Delays data
# MAGIC delays = spark.read.options(header='true').csv(delays_path)
# MAGIC delays = (delays
# MAGIC           .withColumn("delay", expr("CAST(delay as INT) as delay"))
# MAGIC           .withColumn("distance", expr("CAST(distance as INT) as distance")))
# MAGIC 
# MAGIC delays.createOrReplaceTempView("departureDelays")
# MAGIC 
# MAGIC # Create temporary small table
# MAGIC foo = delays.filter(expr("""
# MAGIC             origin == 'SEA' AND 
# MAGIC             destination == 'SFO' AND 
# MAGIC             date like '01010%' AND 
# MAGIC             delay > 0"""))
# MAGIC 
# MAGIC foo.createOrReplaceTempView("foo")

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.sql("SELECT * FROM airports_na LIMIT 10").show()

# COMMAND ----------

spark.sql("SELECT * FROM airports_na LIMIT 10").show()

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.sql("SELECT * FROM departureDelays LIMIT 10").show()

# COMMAND ----------

spark.sql("SELECT * FROM departureDelays LIMIT 10").show()

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.sql("SELECT * FROM foo LIMIT 10").show()

# COMMAND ----------

spark.sql("SELECT * FROM foo LIMIT 10").show()

# COMMAND ----------

# MAGIC %md ## Unions

# COMMAND ----------

# MAGIC %python
# MAGIC # Union two tables
# MAGIC bar = delays.union(foo)
# MAGIC bar.createOrReplaceTempView("bar")
# MAGIC bar.filter(expr("origin == 'SEA' AND destination == 'SFO' AND date LIKE '01010%' AND delay > 0")).show()

# COMMAND ----------

# MAGIC %scala
# MAGIC // Union two tables
# MAGIC val bar = delays.union(foo)
# MAGIC bar.createOrReplaceTempView("bar")
# MAGIC bar.filter(expr("origin == 'SEA' AND destination == 'SFO' AND date LIKE '01010%' AND delay > 0")).show()

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.sql("""
# MAGIC SELECT * 
# MAGIC FROM bar 
# MAGIC WHERE origin = 'SEA' 
# MAGIC    AND destination = 'SFO' 
# MAGIC    AND date LIKE '01010%' 
# MAGIC    AND delay > 0
# MAGIC """).show()

# COMMAND ----------

spark.sql("""
SELECT * 
FROM bar 
WHERE origin = 'SEA' 
   AND destination = 'SFO' 
   AND date LIKE '01010%' 
   AND delay > 0
""").show()

# COMMAND ----------

# MAGIC %md ## Joins
# MAGIC By default, it is an `inner join`.  There are also the options: `inner, cross, outer, full, full_outer, left, left_outer, right, right_outer, left_semi, and left_anti`.
# MAGIC 
# MAGIC More info available at:
# MAGIC * [PySpark Join](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=join)

# COMMAND ----------

# Join Departure Delays data (foo) with flight info
foo.join(
  airports, 
  airports.IATA == foo.origin
).select("City", "State", "date", "delay", "distance", "destination").show()

# COMMAND ----------

# MAGIC %scala
# MAGIC // Join Departure Delays data (foo) with flight info
# MAGIC foo.join(
# MAGIC   airports.as('air), 
# MAGIC   $"air.IATA" === $"origin"
# MAGIC ).select("City", "State", "date", "delay", "distance", "destination").show()

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.sql("""
# MAGIC SELECT a.City, a.State, f.date, f.delay, f.distance, f.destination 
# MAGIC   FROM foo f
# MAGIC   JOIN airports_na a
# MAGIC     ON a.IATA = f.origin
# MAGIC """).show()

# COMMAND ----------

spark.sql("""
SELECT a.City, a.State, f.date, f.delay, f.distance, f.destination 
  FROM foo f
  JOIN airports_na a
    ON a.IATA = f.origin
""").show()

# COMMAND ----------

# MAGIC %md ## Windowing Functions
# MAGIC 
# MAGIC Great reference: [Introduction Windowing Functions in Spark SQL](https://databricks.com/blog/2015/07/15/introducing-window-functions-in-spark-sql.html)
# MAGIC 
# MAGIC > At its core, a window function calculates a return value for every input row of a table based on a group of rows, called the Frame. Every input row can have a unique frame associated with it. This characteristic of window functions makes them more powerful than other functions and allows users to express various data processing tasks that are hard (if not impossible) to be expressed without window functions in a concise way.

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.sql("DROP TABLE IF EXISTS departureDelaysWindow")
# MAGIC spark.sql("""
# MAGIC CREATE TABLE departureDelaysWindow AS
# MAGIC SELECT origin, destination, sum(delay) as TotalDelays 
# MAGIC   FROM departureDelays 
# MAGIC  WHERE origin IN ('SEA', 'SFO', 'JFK') 
# MAGIC    AND destination IN ('SEA', 'SFO', 'JFK', 'DEN', 'ORD', 'LAX', 'ATL') 
# MAGIC  GROUP BY origin, destination
# MAGIC """)
# MAGIC 
# MAGIC spark.sql("""SELECT * FROM departureDelaysWindow""").show()

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS departureDelaysWindow")
spark.sql("""
CREATE TABLE departureDelaysWindow AS
SELECT origin, destination, sum(delay) as TotalDelays 
  FROM departureDelays 
 WHERE origin IN ('SEA', 'SFO', 'JFK') 
   AND destination IN ('SEA', 'SFO', 'JFK', 'DEN', 'ORD', 'LAX', 'ATL') 
 GROUP BY origin, destination
""")

spark.sql("""SELECT * FROM departureDelaysWindow""").show()

# COMMAND ----------

# MAGIC %md What are the top three total delays destinations by origin city of SEA, SFO, and JFK?

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.sql("""
# MAGIC SELECT origin, destination, sum(TotalDelays) as TotalDelays
# MAGIC  FROM departureDelaysWindow
# MAGIC WHERE origin = 'SEA'
# MAGIC GROUP BY origin, destination
# MAGIC ORDER BY sum(TotalDelays) DESC
# MAGIC LIMIT 3
# MAGIC """).show()

# COMMAND ----------

spark.sql("""
SELECT origin, destination, sum(TotalDelays) as TotalDelays
 FROM departureDelaysWindow
WHERE origin = 'SEA'
GROUP BY origin, destination
ORDER BY sum(TotalDelays) DESC
LIMIT 3
""").show()

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.sql("""
# MAGIC SELECT origin, destination, TotalDelays, rank 
# MAGIC   FROM ( 
# MAGIC      SELECT origin, destination, TotalDelays, dense_rank() 
# MAGIC        OVER (PARTITION BY origin ORDER BY TotalDelays DESC) as rank 
# MAGIC        FROM departureDelaysWindow
# MAGIC   ) t 
# MAGIC  WHERE rank <= 3
# MAGIC """).show()

# COMMAND ----------

spark.sql("""
SELECT origin, destination, TotalDelays, rank 
  FROM ( 
     SELECT origin, destination, TotalDelays, dense_rank() 
       OVER (PARTITION BY origin ORDER BY TotalDelays DESC) as rank 
       FROM departureDelaysWindow
  ) t 
 WHERE rank <= 3
""").show()

# COMMAND ----------

# MAGIC %md ## Modifications
# MAGIC 
# MAGIC Another common DataFrame operation is to perform modifications to the DataFrame. Recall that the underlying RDDs are immutable (i.e. they do not change) to ensure there is data lineage for Spark operations. Hence while DataFrames themselves are immutable, you can modify them through operations that create a new, different DataFrame with different columns, for example.  

# COMMAND ----------

# MAGIC %md ### Adding New Columns

# COMMAND ----------

# MAGIC %python
# MAGIC foo2 = foo.withColumn("status", expr("CASE WHEN delay <= 10 THEN 'On-time' ELSE 'Delayed' END"))
# MAGIC foo2.show()

# COMMAND ----------

# MAGIC %scala
# MAGIC val foo2 = foo.withColumn("status", expr("CASE WHEN delay <= 10 THEN 'On-time' ELSE 'Delayed' END"))
# MAGIC foo2.show()

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.sql("""SELECT *, CASE WHEN delay <= 10 THEN 'On-time' ELSE 'Delayed' END AS status FROM foo""").show()

# COMMAND ----------

spark.sql("""SELECT *, CASE WHEN delay <= 10 THEN 'On-time' ELSE 'Delayed' END AS status FROM foo""").show()

# COMMAND ----------

# MAGIC %md ### Dropping Columns

# COMMAND ----------

# MAGIC %python
# MAGIC foo3 = foo2.drop("delay")
# MAGIC foo3.show()

# COMMAND ----------

# MAGIC %scala
# MAGIC val foo3 = foo2.drop("delay")
# MAGIC foo3.show()

# COMMAND ----------

# MAGIC %md ### Renaming Columns

# COMMAND ----------

foo4 = foo3.withColumnRenamed("status", "flight_status")
foo4.show()

# COMMAND ----------

# MAGIC %scala
# MAGIC val foo4 = foo3.withColumnRenamed("status", "flight_status")
# MAGIC foo4.show()

# COMMAND ----------

# MAGIC %md ### Pivoting
# MAGIC Great reference [SQL Pivot: Converting Rows to Columns](https://databricks.com/blog/2018/11/01/sql-pivot-converting-rows-to-columns.html)

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.sql("""SELECT destination, CAST(SUBSTRING(date, 0, 2) AS int) AS month, delay FROM departureDelays WHERE origin = 'SEA'""").show(10)

# COMMAND ----------

spark.sql("""SELECT destination, CAST(SUBSTRING(date, 0, 2) AS int) AS month, delay FROM departureDelays WHERE origin = 'SEA'""").show(10)

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.sql("""
# MAGIC SELECT * FROM (
# MAGIC SELECT destination, CAST(SUBSTRING(date, 0, 2) AS int) AS month, delay 
# MAGIC   FROM departureDelays WHERE origin = 'SEA' 
# MAGIC ) 
# MAGIC PIVOT (
# MAGIC   CAST(AVG(delay) AS DECIMAL(4, 2)) as AvgDelay, MAX(delay) as MaxDelay
# MAGIC   FOR month IN (1 JAN, 2 FEB, 3 MAR)
# MAGIC )
# MAGIC ORDER BY destination
# MAGIC """).show()

# COMMAND ----------

spark.sql("""
SELECT * FROM (
SELECT destination, CAST(SUBSTRING(date, 0, 2) AS int) AS month, delay 
  FROM departureDelays WHERE origin = 'SEA' 
) 
PIVOT (
  CAST(AVG(delay) AS DECIMAL(4, 2)) as AvgDelay, MAX(delay) as MaxDelay
  FOR month IN (1 JAN, 2 FEB, 3 MAR)
)
ORDER BY destination
""").show()

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.sql("""
# MAGIC SELECT * FROM (
# MAGIC SELECT destination, CAST(SUBSTRING(date, 0, 2) AS int) AS month, delay 
# MAGIC   FROM departureDelays WHERE origin = 'SEA' 
# MAGIC ) 
# MAGIC PIVOT (
# MAGIC   CAST(AVG(delay) AS DECIMAL(4, 2)) as AvgDelay, MAX(delay) as MaxDelay
# MAGIC   FOR month IN (1 JAN, 2 FEB)
# MAGIC )
# MAGIC ORDER BY destination
# MAGIC """).show()

# COMMAND ----------

spark.sql("""
SELECT * FROM (
SELECT destination, CAST(SUBSTRING(date, 0, 2) AS int) AS month, delay 
  FROM departureDelays WHERE origin = 'SEA' 
) 
PIVOT (
  CAST(AVG(delay) AS DECIMAL(4, 2)) as AvgDelay, MAX(delay) as MaxDelay
  FOR month IN (1 JAN, 2 FEB)
)
ORDER BY destination
""").show()

# COMMAND ----------

# MAGIC %md ### Rollup
# MAGIC Refer to [What is the difference between cube, rollup and groupBy operators?](https://stackoverflow.com/questions/37975227/what-is-the-difference-between-cube-rollup-and-groupby-operators)