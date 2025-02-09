#df_fil = df.filter(col("Item_Fat_Content")== 'Regular').display()

#slice the data with soft drinks and weight less than 10

df_fil2 = df.filter((col("Item_Weight") < 10) & (col("Item_Type")=='Soft Drinks')).display()

#Fetch column outlet size is null and location type is tier1 and tier2

df_fil3 = df.filter((col("Outlet_Size").isNull()) & (col("Outlet_Location_Type").isin("Tier 1" , "Tier 2"))).display()
