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

# Crear una nueva columna "Null Column" con valores null (de tipo entero)
df = df.withColumn("Null Column", lit(None).cast("integer"))
df.show()

# Rellenar valores nulos en la columna "Volume" con 0
df = df.fillna({'Null Column': 0})
df.show()

# Obtener las primeras 5 filas del DataFrame usando head
head_rows = df.head(5)

# Mostrar las primeras 5 filas
for row in head_rows:
    print(row)

# Detener la sesión de Spark
spark.stop()