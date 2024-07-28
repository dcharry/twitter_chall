import pandas as pd
import time
from typing import List, Tuple
import orjson
import re

# Expresión regular para encontrar menciones
mention_pattern = re.compile(r'@(\w+)')

def extract_mentions(text):
    return mention_pattern.findall(text)

def load_json_in_chunks(file_path: str, chunk_size=10000):
    """
    Carga un archivo JSON en chunks para optimizar el uso de tiempo y de memoria.
    
    :param file_path: Ruta del archivo JSON.
    :param chunk_size: Tamaño del chunk en número de líneas.
    :return: Generador que produce chunks del archivo JSON.
    """
    with open(file_path, 'r') as f:
        chunk = []
        for line in f:
            chunk.append(orjson.loads(line))
            if len(chunk) == chunk_size:
                yield chunk
                chunk = []
        if chunk:
            yield chunk

def process_chunk(chunk):
    """
    Procesa un chunk de datos extrayendo solo las columnas 'content' y 'user' y las menciones.
    
    :param chunk: Lista de registros JSON.
    :return: DataFrame procesado con una columna de menciones.
    """
    contents = [(record['content'], record['user']['username']) for record in chunk]
    df_chunk = pd.DataFrame(contents, columns=['content', 'username'])
    
    # Extraer menciones del contenido
    df_chunk['mentions'] = df_chunk['content'].apply(extract_mentions)
    
    return df_chunk

def q3_time(file_path: str) -> List[Tuple[str, int]]:
    """
    Obtiene los 10 usuarios más influyentes en función del conteo de menciones, optimizando el tiempo de ejecución.
    
    :param file_path: Ruta del archivo JSON.
    :return: Lista de tuplas con los usuarios y su conteo de menciones.
    """
    # Procesar los chunks y concatenar
    chunks = []
    for chunk in load_json_in_chunks(file_path):
        df_chunk = process_chunk(chunk)
        chunks.append(df_chunk)
    df = pd.concat(chunks, ignore_index=True)
    
    # Crear un contador para menciones hechas por cada usuario
    user_count = df.explode('mentions').groupby('username')['mentions'].count()
    top_users = user_count.nlargest(10)
    
    return list(top_users.items())

