from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import StreamingResponse
from tempfile import NamedTemporaryFile
import findspark 
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
import io
import csv
import emoji
import regex
import json

app = FastAPI()

spark = SparkSession.builder.appName('firstSession')\
   .config('spark.master','local[4]')\
   .config('spark.executor.memory','1g')\
   .config('spark.driver.memory','1g')\
   .config('spark.sql.shuffle.partitions','1').getOrCreate()

def read_and_process(file_path):

   
    # Crear un DataFrame a partir del archivo de texto
     # Crear un DataFrame a partir del archivo de texto

    df = spark.read.text(file_path)
    
    # Agregar un índice a las filas para que no se tenga que filtrar por fecha y hora ya que tenemos la hora en formato am y pm lo cual lo hace tedioso ordenar 
    df = df.withColumn("index", F.monotonically_increasing_id())
   
    df = df.withColumn("fechahora", F.split(F.col("value"),' - ').getItem(0))
    
    
    df = df.withColumn("nombremensaje", F.split(F.col("value"),' - ').getItem(1))
    df = df.select("index","fechahora", "nombremensaje")
    #si el mensaje no contiene : se elimina ya que son mensajes predeterminados de la app 
    df = df.filter(F.col("nombremensaje").contains(": "))
    
    # Dividir la columna 'value' en 'fecha', 'hora', 'nombre', 'mensaje' utilizando funciones de F
    
    df = df.withColumn("Fecha", F.split(F.col("fechahora"),', ').getItem(0))
    df = df.withColumn("Hora", F.split(F.col("fechahora"),', ').getItem(1))
    df = df.withColumn("Nombres", F.split(F.col("nombremensaje"),': ').getItem(0))
    #df = df.withColumn("Mensaje", F.split(F.col("nombremensaje"),': ').getItem(1))

    # Dividir el mensaje por ':' y comprobar su longitud para obtener el mensaje adecuado
    df = df.withColumn("Mensaje", 
                    F.when(F.size(F.split(F.col("nombremensaje"), ': ')) > 3,
                            F.concat_ws(':', F.split(F.col("nombremensaje"), ': ').getItem(1),
                                    F.split(F.col("nombremensaje"), ': ').getItem(2),
                                    F.split(F.col("nombremensaje"), ': ').getItem(3)))
                    .when(F.size(F.split(F.col("nombremensaje"), ': ')) > 2,
                            F.concat_ws(':', F.split(F.col("nombremensaje"), ': ').getItem(1),
                                    F.split(F.col("nombremensaje"), ': ').getItem(2)))
                    .otherwise(F.split(F.col("nombremensaje"), ': ').getItem(1))
                    )
    
    # Seleccionar las columnas deseadas
    df = df.select("Fecha", "Hora", "Nombres", "Mensaje")
   
    
    def obtener_emojis(mensaje):
        emoji_lista = []
        data = regex.findall(r'\X', mensaje)
        for caracter in data:
            if any(c in emoji.UNICODE_EMOJI['es'] for c in caracter):
                emoji_lista.append(caracter)
        return emoji_lista

    # Crear una UDF a partir de la función obtener_emojis
    obtener_emojis_udf = F.udf(obtener_emojis, ArrayType(StringType()))
    
    # ... Lógica de procesamiento de datos con PySpark (tu lógica actual de la función `read` y `getMembersStats`)
    df = df.select("Nombres", "Mensaje")
    df = df.withColumn("Multimedia", F.when(F.col("Mensaje") == "<Multimedia omitido>", 1).otherwise(0))
    df = df.withColumn("Links", F.when(F.col("Mensaje").contains("https"), 1).otherwise(0))
    #df = df.filter((F.col("Mensaje").contains("http") == True))
    #df.show(truncate=False)
    # Aplicar la UDF a tu DataFrame para obtener una columna con los emojis encontrados en cada mensaje
    df = df.withColumn("Emojis", obtener_emojis_udf(F.col("Mensaje")))
    #cambiando la columna de emojis al tamaño de la lista de emojis
    df = df.withColumn("Emojis", F.size(F.col("Emojis")))
    #df = df.filter((F.col("Emojis") != 0)&(F.col("Nombres") == "Cristhian"))
   
    # Agrupar por nombres y sumar las columnas Multimedia, Links y Emojis
    df = df.groupBy("Nombres").agg(
        F.sum("Multimedia").alias("Multimedia"),
        F.sum("Links").alias("Links"),
        F.sum("Emojis").alias("Emojis"),  # Utilizar la nueva columna generada por la UDF
        F.count("*").alias("Total de mensajes enviados")
    )
    
    df = df.orderBy("Total de mensajes enviados", ascending=False)
    df.show(truncate=False)
    # Reemplaza esta parte con tu lógica de procesamiento

    json_content = df.toJSON().collect()

    return json_content

@app.post("/procesar_archivo")
async def upload_file(file: UploadFile = File(...)):
    # Verificar si se subió un archivo
    if not file:
        raise HTTPException(status_code=400, detail="No se ha proporcionado un archivo")

    # Guardar el archivo temporalmente
    with NamedTemporaryFile(delete=False) as temp_file:
        temp_file.write(await file.read())
        temp_file_name = temp_file.name

    # Procesar el archivo y obtener el contenido del DataFrame procesado como un archivo CSV
    json_content = read_and_process(temp_file_name)

    # Analizar las cadenas JSON y convertirlas en objetos Python
    parsed_content = [json.loads(item) for item in json_content]

    # Devolver el contenido JSON como respuesta
    return parsed_content
