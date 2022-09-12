import findspark
findspark.init()

import pyspark
from pyspark.sql.functions import col,udf,asc,desc,when
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.sql('''select 'pyspark is ready' AS message''')
# df.show()

#input the data into here
inputDF = spark.read.parquet("take_home_task1.snappy.parquet")
# inputDF.show()

#Print Schema of the parquet file
# inputDF.printSchema()

#Print top 5 row of the data
# inputDF.show(5)

"""
Task 2
"""
#Create avg_usge column
avg_usgeDF = inputDF.withColumn("avg_usge",col("data_byte_usge_qty")/col("durn_scds_usge_tm"))
# avg_usgeDF.show()

#Create udf function to categories the 
def match_category(usage):
    if usage > 1000:
        return "A"
    elif usage < 1000 and usage > 1:
        return "B"
    else:
        return "C"

category_func = udf(match_category)
finalDF = avg_usgeDF.withColumn("category",category_func(avg_usgeDF['avg_usge']))

# finalDF.show(5)

#Count the number of category and show in ascending order
category_counter=finalDF.groupBy("category").count()
# category_counter.orderBy("category").show(truncate=False)

"""
Task 3
"""
alte_finalDF = avg_usgeDF.withColumn("category_alt",when(avg_usgeDF.avg_usge > 1000, "A").when(avg_usgeDF.avg_usge > 1, "B").otherwise("C"))
print(alte_finalDF.select("msisdn_no","data_byte_usge_qty","durn_scds_usge_tm","avg_usge","category_alt").show(5))

#Count the number of category and show in ascending order
alte_category_counter=alte_finalDF.groupBy("category_alt").count()
print("Category Counter Table:")
print(alte_category_counter.orderBy("category_alt").show(truncate=False))

