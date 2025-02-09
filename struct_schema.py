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