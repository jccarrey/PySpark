import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

# Crear una sesión de Spark
spark = SparkSession.builder.appName("Ejemplo PySpark").getOrCreate()

# Ruta del archivo CSV
csv_file_path = "./AB.csv"

# Crear un DataFrame desde un archivo CSV
df = spark.read.csv(csv_file_path, header=True, inferSchema=True)

# Renombrar la columna "Close" a "Closing_Price"
df_renamed = df.withColumnRenamed("Close", "Closing_Price")
df_renamed.show()

# Añadir una nueva columna con un valor constante
df_new_column = df.withColumn("New_Column", lit(100))
df_new_column.show()

# Eliminar la columna "Adj Close"
df_dropped = df.drop("Adj Close")
df_dropped.show()

# Ordenar el DataFrame por la columna "Close" en orden descendente
df_sorted = df.orderBy(df["Close"].desc())
df_sorted.show()

# Describir el DataFrame
df.describe().show()
