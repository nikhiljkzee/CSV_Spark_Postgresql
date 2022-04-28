
#Reading a csv file using spark and save to postgresql
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import psycopg2


spark = SparkSession.builder.appName("Intelli_J").master("local").getOrCreate()
df = spark.read.csv('Sample.csv')

Data_list = ["N1","N2","N3","N4","N5","N6","N7","N8","N9","N10"]
new_df = df.toDF(*Data_list)

new_df = new_df.withColumn('PhoneCode', lit(91)) \
    .withColumn('PhoneNumber', lit('9481******'))
new_df.show(3)
new_df.printSchema()

#saving the df to a csv file
#inside intellij
# new_df.write.option("header",True).csv('customer_contacts.csv')
# new_file = spark.read.csv('C:/Users/nikhil.jk/IdeaProjects/Spark_Project1/Spark1/customer_contacts.csv/details.csv')



# new_df.write.option("header",True).csv('C:/Users/nikhil.jk/Downloads/BigData/customer_contacts.csv')
new_file = spark.read.csv('C:/Users/nikhil.jk/Downloads/BigData/customer_contacts.csv/details1.csv')

new_file.printSchema()


print("connecting to Database")

hostname = 'localhost'
database = 'demo2'
username = 'postgres'
pwd = 'root'
port_id = 5432

conn=None
cur=None

try:
    conn=psycopg2.connect(
        host=hostname,
        dbname=database,
        user=username,
        password=pwd,
        port=port_id)

    cur =conn.cursor()
    create_script = ''' CREATE TABLE IF NOT EXISTS details (
                                    N1      varchar(100) PRIMARY KEY,
                                    N2    varchar(100),
                                    N3    varchar(100),
                                    N4    varchar(100),
                                    N5    varchar(100),
                                    N6    varchar(100),
                                    N7    varchar(100),
                                    N8    varchar(100),
                                    N9    varchar(100),
                                    N10    varchar(100),
                                    PhoneCode int NOT NULL,
                                    PhoneNumber varchar(100) NOT NULL)'''
    cur.execute(create_script)

    print('nikhil ')
    SQL_STATEMENT='''
    COPY details(N1,N2,N3,N4,N5,N6,N7,N8,N9,N10,PhoneCode,PhoneNumber)
     FROM 'C:/Users/nikhil.jk/Downloads/BigData/customer_contacts.csv/details1.csv' WITH
        CSV
        HEADER
        DELIMITER As ',';
    '''
    cur.copy_expert(sql=SQL_STATEMENT,file=new_file)
    print('file copied to db')
    conn.commit()

except Exception as error:
    print(error)
finally:
    if cur is not None:
        cur.close()
    if conn is not None:
        conn.close()


