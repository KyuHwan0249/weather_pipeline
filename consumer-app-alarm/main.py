from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max as spark_max, min as spark_min
import random
import time

# ✅ SparkSession 생성
spark = SparkSession.builder \
    .appName("SparkClusterHeavyTest") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

print("✅ SparkSession created successfully!")
print("Spark App Name:", spark.sparkContext.appName)
print("Spark Master:", spark.sparkContext.master)

# ✅ 큰 데이터셋 생성 (100만 rows)
data = [(i, random.randint(-20, 40), random.choice(["Seoul", "Busan", "Incheon", "Daegu", "Gwangju"])) for i in range(1_000_000)]
df = spark.createDataFrame(data, ["id", "temp", "city"])
print("✅ DataFrame created with", df.count(), "rows")

# ✅ 계산 1: 평균, 최댓값, 최솟값
agg_df = df.groupBy("city").agg(
    avg("temp").alias("avg_temp"),
    spark_max("temp").alias("max_temp"),
    spark_min("temp").alias("min_temp")
)
agg_df.show()

# ✅ 계산 2: 온도 범위 필터링
filtered_df = df.filter((col("temp") > 25) & (col("temp") < 35))
print("✅ Filtered Data count:", filtered_df.count())

# ✅ 계산 3: 윈도우 연산 비슷한 map-like 변환 (의미 없는 무거운 연산)
def complex_func(x):
    # 의도적으로 연산량을 늘리기 위한 수학 연산
    v = x["temp"]
    res = 0
    for i in range(1000):
        res += (v * i) % 7
    return (x["id"], x["city"], res)

mapped_rdd = df.rdd.map(complex_func)
print("✅ Running heavy computation on each row...")
print("Sample result:", mapped_rdd.take(5))

# ✅ 계산 4: 데이터 합산
sum_result = df.agg({"temp": "sum"}).collect()[0][0]
print("✅ Total temperature sum:", sum_result)

# ✅ 약간의 대기 (UI 확인용)
print("⏳ Sleeping for 10 seconds so you can check Spark UI at http://localhost:8080 ...")
time.sleep(10)

spark.stop()
print("✅ Spark job finished successfully!")
