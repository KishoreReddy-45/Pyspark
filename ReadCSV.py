df = spark.read.format('csv').option('inferschema',True).option('header',True).load('/FileStore/tables/BigMart_Sales.csv')

df.show()

df.display()