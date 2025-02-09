df = spark.read.format('json').option('inferschema',True)\
                              .option('header',True)\
                              .option('multiline',False)\
                              .load('/FileStore/tables/drivers.json')

df.show()

df.printSchema()

df.display()