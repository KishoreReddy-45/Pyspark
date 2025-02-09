# Importing PySpark and the SparkSession
# DataType functionality
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *

# Creating a spark session
spark_session = SparkSession.builder.appName('Spark_Session').getOrCreate()

# Creating an empty RDD to make a 
# DataFrame with no data
emp_RDD = spark_session.sparkContext.emptyRDD()

# Defining the schema of the DataFrame
columns1 = StructType([
                      StructField('Name', StringType(), False),
					            StructField('Salary', IntegerType(), False)
                      ])

# Creating an empty DataFrame
first_df = spark_session.createDataFrame(data=emp_RDD,schema=columns1)

# Printing the DataFrame with no data
first_df.show()

# Hardcoded data for the second DataFrame
rows = [['Kishore', 56000], ['Reddy', 89000],['Kittu', 76000], ['GuruRaj', 98000]]
columns = ['Name', 'Salary']

# Creating the DataFrame
second_df = spark_session.createDataFrame(rows, columns)

# Printing the non-empty DataFrame
second_df.show()

# Storing the union of first_df and 
# second_df in first_df
first_df = first_df.union(second_df)

# Our first DataFrame that was empty,
# now has data
first_df.show()
