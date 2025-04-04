from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, input_file_name
import os

def load_pageviews(spark, file_path, date_str):
    return spark.read.option("delimiter", " ").csv(file_path, inferSchema=True) \
        .toDF("domain", "title", "views", "size") \
        .withColumn("date", lit(date_str)) \
        .withColumn("file_path", input_file_name())

def run(data_path: str):
    # SparkSession 생성 (마스터 URL 지정 X, spark-submit에서 설정됨)
    spark = SparkSession.builder.appName("SamplingJob").getOrCreate()

    df = load_pageviews(spark, data_path, "2024-01-01")
    df.show()

    # Spark 세션 종료
    spark.stop()
    
if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.join(base_dir, "../data/wiki/2024/2024-01/dt=20240101/")
    run(data_path)
