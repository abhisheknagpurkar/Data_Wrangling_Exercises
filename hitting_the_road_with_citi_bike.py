# question: how many rides each day for subscribers versus customers?
#
# answer: choose a single day of rides to examine
# the dataset used for this analysis is downloaded from https://s3.amazonaws.com/tripdata/index.html
# filename: JC-202208-citibike-tripdata.csv
#
# program outline:
# 1. Read in the data file JC-202208-citibike-tripdata.csv
# 2. Create variables for count of subscribers, customers and other
# 3. For each User Type, use count() function to retrieve number of rides and assign to respective variables
# 4. Print results
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate()
sep2020_rides_df = (spark.read
                    .option('inferSchema', 'true')
                    .option('header', 'true')
                    .csv('/Users/abhisheknagpurkar/DE/Data_Wrangling/data/202009-citibike-tripdata.csv')
                    )
subscribers = sep2020_rides_df.where(col('usertype') == 'Subscriber').distinct().count()
customers = sep2020_rides_df.where(col('usertype') == 'Customer').distinct().count()

print(sep2020_rides_df.columns)

print(f"Number of Subscribers:\n{subscribers}")
print(f"Number of Customers: \n{customers}")