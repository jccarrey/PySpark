import findspark
findspark.init()

from pyspark.sql import SparkSession

# Crear una sesión de Spark
spark = SparkSession.builder.appName("Ejemplo PySpark").getOrCreate()

# Ruta del archivo CSV
csv_file_path = "./AB.csv"

# Crear un DataFrame desde un archivo CSV
df = spark.read.csv(csv_file_path, header=True, inferSchema=True)

# Mostrar las primeras filas del DataFrame
df.show()

# Seleccionar y mostrar solo las columnas "Date" y "Close"
df.select("Date", "Close").show()

# Filtrar y mostrar los registros donde el valor de "Close" es mayor que 2.6
df.filter(df.Close > 2.6).show()

# Realizar una operación de agrupación y agregación (promedio de "Close" por "Date")
df.groupBy("Date").avg("Close").show()

df.printSchema()

# Detener la sesión de Spark
spark.stop()
