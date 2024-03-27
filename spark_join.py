from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, lit, format_string, date_format, concat, lpad
from pyspark.sql.types import TimestampType
from datetime import datetime
from pyspark.sql.types import StringType
# Initialize SparkSession
spark = SparkSession.builder.appName("PrecipFlightJoinSafeParse").getOrCreate()

# UDF with try catch to read and format precipitation date
def safe_strptime(date_str):
    try:
        date_object = datetime.strptime(date_str, '%Y%m%d %H:%M')
        # Format the datetime object into the desired string format
        return date_object.strftime('%Y-%m-%d %H')
    except ValueError:  # Catches the error if the date_str does not match the format
        return None


# UDF with try catch to read and format flights date
def safe_catime(date_str, fmt_in, fmt_out):
    try:
        dt = datetime.strptime(date_str, fmt_in)
        return dt.strftime(fmt_out)
    except ValueError:  # Catches the error if the date_str does not match the format
        return None

# Another way of calling UDFs
safe_catime_udf = udf(lambda date_str: safe_catime(date_str, '%m/%d/%Y%H', '%Y-%m-%d %H'), StringType())

# We want all departure times e.g. 715 hrs, 2315 hrs to be reduced to their HH components for consistency: 715 to 07 & 2315 to 23
def format_dep_time(dep_time_col):
    # Pad with zeros if necessary and extract the hour part
    return lpad(dep_time_col.cast("string"), 4, '0').substr(1, 2)


safe_strptime_udf = udf(safe_strptime, StringType())

# Working with precipitation data
precip_df = spark.read.csv("flights_data/3635813.csv", header=True, inferSchema=True)
precip_df_with_parsed_dates = precip_df.withColumn('parsed_date', safe_strptime_udf(precip_df['DATE']))

# Working with flight data
flight_df = spark.read.csv("flights_data/lax_to_jfk.csv", header=True, inferSchema=True)
flight_df = flight_df.withColumn("CRSDepTimeformatted", format_dep_time(col("CRSDepTime")))
flight_df_with_parsed_dates = flight_df.withColumn('flight_date', safe_catime_udf(concat(flight_df['FlightDate'], flight_df['CRSDepTimeformatted']))).select("flight_date", "DepDelayMinutes", "FlightDate", "CRSDepTime", "CRSDepTimeformatted")

# join and output
#flight_df_with_parsed_dates.show()
#precip_df_with_parsed_dates.show()
joined_df = flight_df_with_parsed_dates.join(precip_df_with_parsed_dates, flight_df_with_parsed_dates.flight_date == precip_df_with_parsed_dates.parsed_date).select("flight_date", "DepDelayMinutes","HPCP")
joined_df.show()
