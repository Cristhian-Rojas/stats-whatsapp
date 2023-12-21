import findspark 
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
import emoji
import regex

spark = SparkSession.builder.appName('firstSession')\
    .config('spark.master','local[4]')\
    .config('spark.executor.memory','1g')\
    .config('spark.driver.memory','1g')\
    .config('spark.sql.shuffle.partitions','1').getOrCreate()
    
#1era config es para que se ejecute en local con 4 nucleos 
#2da config es para que se ejecute con 1gb de memoria
#3era config es para que se ejecute con 1gb de memoria
#4ta config es para que se ejecute con 1 particion
#5ta config es para que se ejecute con la version de spark 3.0 para poder ordenar fechas con formato dd/mm/yyyy

def read(path):
    # Crear un DataFrame a partir del archivo de texto

    df = spark.read.text(path)
    
    # Agregar un índice a las filas para que no se tenga que filtrar por fecha y hora ya que tenemos la hora en formato am y pm lo cual lo hace tedioso ordenar 
    df = df.withColumn("index", F.monotonically_increasing_id())
    
    # # Determinar si la línea tiene patrón de fecha y hora si la linea no tiene fecha y hora se esta obviando pero se debe de concatenar con la linea anterior
    # # df = df.withColumn("is_datetime", F.regexp_extract(df["value"], r"^\d{1,2}/\d{1,2}/\d{4}, \d{1,2}:\d{2} [ap]\. m\.", 0))
    
    # # df = df.filter(F.col("is_datetime") == "")
    # # df.show(truncate=False)
    
    # Dividir la columna 'value' en 'fechahora' y 'nombremensaje'
    df = df.withColumn("fechahora", F.split(F.col("value"),' - ').getItem(0))
    
    # df = df.filter(F.col("fechahora") == "26/5/2023, 4:24 p. m.")
    # df.show(truncate=False)
    
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
    #null son mensajes eliminados o para ver solo una vez
    #df.na.drop()
    
    # df = df.filter(F.col("Fecha") == '10/1/2022')
    # df = df.filter(F.col("Hora") >= '4:56 p. m.')
    # df.show(truncate=False)
    #print('df sin editar',df.count())

    return df

def obtener_emojis(mensaje):
    emoji_lista = []
    data = regex.findall(r'\X', mensaje)
    for caracter in data:
        if any(c in emoji.UNICODE_EMOJI['es'] for c in caracter):
            emoji_lista.append(caracter)
    return emoji_lista

# Crear una UDF a partir de la función obtener_emojis
obtener_emojis_udf = F.udf(obtener_emojis, ArrayType(StringType()))

def getMembersStats(df):
    # Obtener los nombres de los miembros del grupo y su total de mensajes enviados
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
    return df

def read2(path):
    # Crear un DataFrame a partir del archivo de texto

    df = spark.read.text(path)
    
    # Agregar un índice a las filas para que no se tenga que filtrar por fecha y hora ya que tenemos la hora en formato am y pm lo cual lo hace tedioso ordenar 
    df = df.withColumn("index", F.monotonically_increasing_id()).select("index","value")
    df = df.filter(F.col("index")>=21)
    df.show(truncate=False)
    # Filtrar las filas que no comienzan con fecha hora las cuales debemos concatenar con las anteriores ya que son saltos de linea
    #df con indices a eliminar saltos de linea
    df_index = df.filter(~(F.col("value").rlike(r"^\d{1,2}/\d{1,2}/\d{4}"))).select("index")
    #df_index = df_index.withColumn("index", F.expr("index - 1"))
    
    # Convertir el DataFrame a una lista de filas
    rows = df_index.collect()

    # Obtener solo los valores de las filas y almacenarlos en una lista
    indices_to_replace = [row.asDict().values() for row in rows]
    # Convertir a valores enteros (si son números)
    # Convertir a una lista plana
    indices_to_replace = [item for sublist in indices_to_replace for item in sublist]
    print(indices_to_replace)
    # df_filtered = df.select("index", "value")
    
    # Reemplazar saltos de línea solo para las filas con los índices específicos
    # df = df.withColumn("value", 
    #                F.regexp_replace("value", "https", "")
    #               ).where(~(F.col("index").isin(indices_to_replace)))
    
    df = df.select("value").withColumn("value", F.regexp_replace("value", "\n|\r|\r\n", " "))
    print('df con saltos de linea eliminados')
    df.show(truncate=False)
    print('df con indices a eliminar')
    df_index.show(truncate=False) 
    
    
    # Agrupar los mensajes por fecha en una sola fila
    #df_aggregated = df_filtered.groupBy("index").agg(F.concat_ws("\n", F.collect_list(F.col("value"))).alias("Mensajes"))
    #df_aggregated = df_aggregated.filter(F.col("Mensajes").like("7/7/2023, 3:34%"))
    
    # Muestra el resultado final
    #df_aggregated.show(truncate=False)

    return df_index

def main():
    #variables a usar 
    pathtxt = r'C:\Users\PC\Desktop\tuarchivo.txt'
    
    df = read(pathtxt)
    # Mostrar el DataFrame resultante
    #df.show(truncate=False)
    #print(df.count())
    
    #obtener miembros del chat o grupo 
    dfMembers = getMembersStats(df)
    dfMembers.show(truncate=False)
    # Ruta local donde quieres guardar el archivo CSV
    #output_path = r'C:\Users\PC\Desktop\reportes'
    
    # Guardar el DataFrame como archivo CSV en el directorio local
    #dfMembers.coalesce(1).write.mode('overwrite').option("header", True).option("delimiter", "|").csv(output_path)
    
    
    
if __name__ == "__main__":
    main()
    spark.stop()
