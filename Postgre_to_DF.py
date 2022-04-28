from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.jars", "postgresql-42.2.12.jar") \
    .getOrCreate()

df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/demo2") \
    .option("dbtable", "(SELECT * FROM details) as temp") \
    .option("user", "postgres") \
    .option("password", "root") \
    .option("driver", "org.postgresql.Driver") \
    .load()


df.show()


#save to postgresql

# studentDf.select("id","name","marks").write.format("jdbc") \
#     .option("url", "jdbc:postgresql://localhost:5432/dezyre_new") \
#     .option("driver", "org.postgresql.Driver").option("dbtable", "students") \
#     .option("user", "hduser").option("password", "bigdata").save()


