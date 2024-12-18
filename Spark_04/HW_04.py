# Используйте источник rate, напишите код, который создаст дополнительный столбец, 
# который будет выводить сумму только нечётных чисел

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum 

# import warnings
# warnings.simplefilter(action='ignore', category=FutureWarning)

# Создаем сессию
spark = SparkSession.builder.appName("RateSourceExample").getOrCreate()
# Создаем поток данных
streamingDF = spark.readStream.format("rate").load()
# Фильтрация нечетных чисел
odd_numbers_df = streamingDF.filter((col("value") % 2) != 0)
# Сумма нечетных чисел
sum_odd_numbers_df = odd_numbers_df.agg(sum("value"))

# Вывод в консоль
query = sum_odd_numbers_df.writeStream.outputMode("update").format("console").start()
query.awaitTermination()
spark.stop()