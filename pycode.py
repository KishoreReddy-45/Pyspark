# Databricks notebook source
# MAGIC %md
# MAGIC ### Data Reading 

# COMMAND ----------

# DBTITLE 1,Json read
df = spark.read.format('json').option('inferschema',True)\
                              .option('header',True)\
                              .option('multiline',False)\
                              .load('/FileStore/tables/drivers.json')

df.show()

df.printSchema()

df.display()

# COMMAND ----------

# DBTITLE 1,file_path
dbutils.fs.ls('/FileStore/tables/')

# COMMAND ----------

# DBTITLE 1,CSV Data Read
df = spark.read.format('csv').option('inferschema',True).option('header',True).load('/FileStore/tables/BigMart_Sales.csv')

df.show()

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Struct Type

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession

# COMMAND ----------

# DBTITLE 1,Struct Schema
my_strct_schema = StructType([

                              StructField('Item_Identifier',StringType(),True),
                              StructField('Item_Weight',StringType(),True),
                              StructField('Item_Fat_Content',StringType(),True),
                              StructField('Item_Visibility',StringType(),True),
                              StructField('Item_MRP',StringType(),True),
                              StructField('Outlet_Identifier',StringType(),True),
                              StructField('Outlet_Establishment_Year',StringType(),True),
                              StructField('Outlet_Size',StringType(),True),
                              StructField('Outlet_Location_Type',StringType(),True),
                              StructField('Outlet_Type',StringType(),True),
                              StructField('Item_Outlet_Sales',StringType(),True)
])

# COMMAND ----------

# DBTITLE 1,Schema Read
df = spark.read.format('csv')\
               .schema(my_strct_schema)\
               .option('header',True).load('/FileStore/tables/BigMart_Sales.csv')

df.display()

df.printSchema()

# COMMAND ----------

# DBTITLE 1,Filters
#df_fil = df.filter(col("Item_Fat_Content")== 'Regular').display()

#slice the data with soft drinks and weight less than 10

df_fil2 = df.filter((col("Item_Weight") < 10) & (col("Item_Type")=='Soft Drinks')).display()

#Fetch column outlet size is null and location type is tier1 and tier2

df_fil3 = df.filter((col("Outlet_Size").isNull()) & (col("Outlet_Location_Type").isin("Tier 1" , "Tier 2"))).display()


# COMMAND ----------

# DBTITLE 1,WithColumn
df_col = df.withColumn("Prod_Col",col("Item_Weight")*col("Item_MRP")).display()

# COMMAND ----------

# DBTITLE 1,Union and Union BY Name
data1 = [('1','kad'),
         ('2','sid')]
schema1 = 'id STRING, name STRING'

df1 = spark.createDataFrame(data1,schema1)

df1.show()
"""
data2 = [('3','rahul'),
         ('4','jas')]
schema2 = 'id STRING, name STRING'
"""
data2 = [('rahul','3'),
         ('jas','4')]
schema2 = 'name STRING','id STRING' 


df2 = spark.createDataFrame(data2,schema2)

df2.show()

# COMMAND ----------

# DBTITLE 1,Union
df = df1.union(df2).display()

# COMMAND ----------

# DBTITLE 1,Union BYName
df = df2.unionByName(df1).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### STRING FUNCTIONS

# COMMAND ----------

# DBTITLE 1,INITCAP
df.select(initcap("Item_Fat_Content")).display()

# COMMAND ----------

# DBTITLE 1,UPPERCASE
df.select(upper("Item_Fat_Content").alias("Upper_Fat_Content")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Date Func

# COMMAND ----------

# DBTITLE 1,current_date
df = df.withColumn("current_date",current_date()).display()

#df = df.withColumn("week_after",date_add('current_date',7)).display()


# COMMAND ----------

# DBTITLE 1,Date_Add
df = df.withColumn("week_after",date_add('current_date',7))

df.display()

# COMMAND ----------

# DBTITLE 1,Handling Null Values
print("remove null in all column")
df1 = df.dropna('all').display()

print("remove null in any column")
df2 = df.dropna('any').display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Collect_list

# COMMAND ----------

data = [('user1','book1'),
('user1','book2'),
('user2','book2'),
('user2','book4'),
('user3','book1')]

schema = 'user string, book string'

df_book = spark.createDataFrame(data,schema)

df_book.display()


# COMMAND ----------

df_collect = df_book.groupBy('user').agg(collect_list('book')).alias("books").display()

df_explode = df_collect.WithColumn("explode_list",explode("books")).display()

# COMMAND ----------

data = [
  ("Dairy", "Medium", 249.8092),
  ("Soft Drinks", "Medium", 48.2692),
  ("Meat", "Medium", 141.618),
  ("Fruits and Vegetables", None, 182.095),
  ("Household", "High", 53.8614),
  ("Baking Goods", "Medium", 51.4008),
  ("Snack Foods", "High", 57.6588),
  ("Snack Foods", "Medium", 107.7622)
]

columns = ["Item","Size","Amount"]

df_data = spark.createDataFrame(data,columns)

df_data.display()

#pivot
df_pivot = df_data.groupBy("Item").pivot("size").agg(avg("Amount"))


df_pivot.display()


# COMMAND ----------

# DBTITLE 1,When Clasue
df_when = df_data.withColumn("Check",when(col("Size") =="Medium","Half").when(col("Size")=="High","Full").otherwise("low")).display()

# COMMAND ----------

data = [
  (1, "m", "1,2", 20),
  (2, "f", "1,2,3", 20),
  (3, "T", "1", 20)
]

schema = 'Id Integer,Gender String,Category string,Age Integer'

df = spark.createDataFrame(data,schema)

df.display()



# COMMAND ----------

df_split = df.withColumn("Categories",explode(split("Category",","))).display()


# COMMAND ----------

df_match = df_split.withColumn("Genders",(when(col("Gender") == "m","Male")).when(col("Gender") == "f","Female")).otherwise("low").display()
