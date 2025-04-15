from pathvalidate import sanitize_filename
from pyspark.sql import SparkSession
import os

def main():
    spark = SparkSession.builder \
        .appName('data preparation') \
        .master("local") \
        .config("spark.sql.parquet.enableVectorizedReader", "true") \
        .getOrCreate()

    print("Getting parquet")

    # Read parquet file
    df = spark.read.parquet("/a.parquet")

    print("Retrieved parquet", df)

    n = 1000
    df = df.select(['id', 'title', 'text']).sample(fraction=100 * n / df.count(), seed=0).limit(n)


    if os.path.exists("data"):
        print("Data folder exists")
    else:
        os.makedirs("data")
        print("Data folder created")

    def create_doc(row):
        filename = "data/" + sanitize_filename(f"{row['id']}_{row['title']}").replace(" ", "_") + ".txt"
        with open(filename, "w", encoding='utf-8') as f:
            f.write(row['text'])
            print(filename + " saved!")

    df.foreach(create_doc)

if __name__ == "__main__":
    main()
    