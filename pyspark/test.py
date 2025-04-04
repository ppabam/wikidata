from pyspark.sql import SparkSession

def main():
    # SparkSession 생성 (마스터 URL 지정 X, spark-submit에서 설정됨)
    spark = SparkSession.builder.appName("TestJob").getOrCreate()

    # 간단한 DataFrame 생성 및 출력
    data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
    df = spark.createDataFrame(data, ["Name", "Age"])
    df.show()

    # Spark 세션 종료
    spark.stop()
    
if __name__ == "__main__":
    main()
