import sys
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, explode, lit, current_timestamp, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType, TimestampType

# Define expected schema
def get_expected_schema():
    video_schema = StructType([
        StructField("video_id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("description", StringType(), True),
        StructField("published_at", StringType(), True),  # Keep as StringType initially
        StructField("view_count", StringType(), True),    # Keep as StringType initially
        StructField("like_count", StringType(), True),    # Keep as StringType initially
        StructField("comment_count", StringType(), True), # Keep as StringType initially
        StructField("duration", StringType(), True)       # Add duration field
    ])

    channel_schema = StructType([
        StructField("channel_id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("description", StringType(), True),
        StructField("published_at", StringType(), True),  # Add published_at field
        StructField("view_count", StringType(), True),
        StructField("subscriber_count", StringType(), True),
        StructField("video_count", StringType(), True),
        StructField("playlist_id", StringType(), True)    # Add playlist_id field
    ])

    return StructType([
        StructField("videos", ArrayType(video_schema), True),
        StructField("channel_info", channel_schema, True)
    ])

def validate_schema(df, expected_schema):
    """
    Validate the DataFrame schema against expected schema
    """
    try:
        # Get the field names from both schemas
        df_fields = set(df.schema.fieldNames())
        expected_fields = set(expected_schema.fieldNames())
        
        missing_fields = expected_fields - df_fields
        extra_fields = df_fields - expected_fields
        
        if missing_fields or extra_fields:
            print("Schema validation warning:")
            if missing_fields:
                print(f"Missing fields: {missing_fields}")
            if extra_fields:
                print(f"Extra fields: {extra_fields}")
            
        print("Schema validation completed")
        return True
    except Exception as e:
        print(f"Schema validation error: {str(e)}")
        raise

def get_date_paths():
    """
    Get current date-based paths for input and output
    """
    current_time = datetime.now()
    year = current_time.year
    month = str(current_time.month).zfill(2)
    day = str(current_time.day).zfill(2)
    
    date_path = f"year={year}/month={month}/day={day}"
    return {
        'year': year,
        'month': int(month),
        'day': int(day),
        'date_path': date_path
    }

def transform_data(glueContext, input_path, output_path):
    """
    Transform raw JSON data into the desired format using PySpark.
    """
    try:
        print(f"Reading JSON data from: {input_path}")
        
        spark = glueContext.spark_session
        
        # Read the raw JSON data with schema
        expected_schema = get_expected_schema()
        raw_df = spark.read.schema(expected_schema).json(input_path)
        
        # Validate initial schema
        validate_schema(raw_df, expected_schema)
        
        # Convert and transform the data
        exploded_df = raw_df.select(
            col("channel_info"),
            explode(col("videos")).alias("video")
        )

        # Extract channel info with explicit column selection and type casting
        channel_df = exploded_df.select(
            col("channel_info.channel_id"),
            col("channel_info.title").alias("channel_title"),
            col("channel_info.description").alias("channel_description"),
            to_timestamp(col("channel_info.published_at")).alias("channel_published_at"),
            col("channel_info.subscriber_count").cast("long"),
            col("channel_info.video_count").cast("long"),
            col("channel_info.view_count").cast("long"),
            col("channel_info.playlist_id")
        )
        
        # Transform videos with proper timestamp and type conversion
        videos_df = exploded_df.select(
            col("channel_info.channel_id").alias("vid_channel_id"),
            col("video.video_id"),
            col("video.title").alias("video_title"),
            col("video.description").alias("video_description"),
            to_timestamp(col("video.published_at")).alias("published_at"),
            col("video.view_count").cast("long"),
            col("video.like_count").cast("long"),
            col("video.comment_count").cast("long"),
            col("video.duration")
        )

        # Join channel info with videos
        transformed_df = videos_df.join(channel_df, videos_df.vid_channel_id == channel_df.channel_id, "inner")

        transformed_df = transformed_df.drop('vid_channel_id')

        # Get current date information
        date_info = get_date_paths()
        
        # Add partition columns based on current date
        transformed_df = transformed_df \
            .withColumn("year", lit(date_info['year'])) \
            .withColumn("month", lit(date_info['month'])) \
            .withColumn("day", lit(date_info['day'])) \
            .withColumn("processed_timestamp", current_timestamp())

        print("\nTransformed Data Schema:")
        transformed_df.printSchema()
        
        print("\nSample of transformed data:")
        transformed_df.show(5, truncate=False)

        # Display some statistics
        print("\nTransformation Statistics:")
        video_count = transformed_df.count()
        channel_count = transformed_df.select("channel_id").distinct().count()
        print(f"Total videos processed: {video_count}")
        print(f"Total unique channels: {channel_count}")
        

        # Write the transformed data to S3 in Parquet format
        print(f"\nWriting transformed data to: {output_path}")
        transformed_df.write \
            .partitionBy("year", "month", "day") \
            .mode("overwrite") \
            .parquet(output_path.rstrip('/'))
            
        print("Data transformation completed successfully!")

    except Exception as e:
        print(f"Error during transformation: {str(e)}")
        raise

def main():
    """
    Main function to run the Glue job
    """
    try:
        # Get job arguments
        args = getResolvedOptions(sys.argv, ['JOB_NAME'])
        
        # Initialize Glue context
        sc = SparkContext()
        glueContext = GlueContext(sc)
        spark = glueContext.spark_session
        job = Job(glueContext)
        job.init(args['JOB_NAME'], args)

        # Get date-based paths
        date_info = get_date_paths()
        
        # Construct input and output paths
        input_path = f"s3://iit-matrix-test/raw/{date_info['date_path']}/"
        output_path = "s3://iit-matrix-test/transform/"

        # Transform the data
        transform_data(glueContext, input_path, output_path)
        
        # Commit the job
        job.commit()

    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        raise

if __name__ == "__main__":
    main()