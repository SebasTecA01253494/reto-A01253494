from pyspark.sql import SparkSession
import json

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("people")\
        .getOrCreate()

    print("read dataset.csv ... ")
    path_cars="Automobile.csv"
    db_cars = spark.read.csv(path_cars,header=True,inferSchema=True)
    db_cars = db_cars.withColumnRenamed("origin", "country")
    db_cars = db_cars.withColumnRenamed("model_year", "model")
    db_cars.createOrReplaceTempView("cars")
    query='DESCRIBE cars'
    spark.sql(query).show(20)

    query="""SELECT name, model, country FROM cars WHERE country=="usa" ORDER BY `model`"""
    db_american_cars = spark.sql(query)
    db_american_cars.show(20)
    results = db_american_cars.toJSON().collect()
    db_american_cars.write.mode("overwrite").json("results")
    with open('results/data.json', 'w') as file:
        json.dump(results, file)

    spark.stop()