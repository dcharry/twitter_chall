import pandas as pd
from typing import List, Tuple
import orjson
import re

# Expresión regular para encontrar menciones
mention_pattern = re.compile(r'@(\w+)')

def extract_mentions(text: str) -> List[str]:
    """
    Extrae todas las menciones (@usuario) de un texto dado.
    
    :param text: Texto de donde se extraerán las menciones.
    :return: Lista de menciones encontradas en el texto.
    """
    return mention_pattern.findall(text)

def load_json_in_chunks(file_path: str, chunk_size: int = 10000) -> List[dict]:
    """
    Carga un archivo JSON en chunks para optimizar el uso de memoria.
    
    :param file_path: Ruta del archivo JSON.
    :param chunk_size: Tamaño del chunk en número de líneas.
    :return: Generador que produce chunks del archivo JSON.
    """
    with open(file_path, 'r') as file:
        chunk = []
        for line in file:
            chunk.append(orjson.loads(line))
            if len(chunk) == chunk_size:
                yield chunk
                chunk = []
        if chunk:
            yield chunk

def process_chunk(chunk: List[dict]) -> pd.DataFrame:
    """
    Procesa un chunk de datos extrayendo solo las columnas 'content' y 'user' y las menciones.
    
    :param chunk: Lista de registros JSON.
    :return: DataFrame procesado con una columna de menciones.
    """
    # Extraer contenido y nombre de usuario
    contents = [(record['content'], record['user']['username']) for record in chunk]
    df_chunk = pd.DataFrame(contents, columns=['content', 'username'])
    
    # Extraer menciones del contenido
    df_chunk['mentions'] = df_chunk['content'].apply(extract_mentions)
    
    # Convertir la columna 'username' a tipo 'category' para ahorrar memoria
    df_chunk['username'] = df_chunk['username'].astype('category')
    
    # Descartar la columna 'content' para reducir el uso de memoria
    df_chunk.drop(columns=['content'], inplace=True)
    
    return df_chunk

def q3_memory(file_path: str) -> List[Tuple[str, int]]:
    """
    Obtiene los 10 usuarios más influyentes en función del conteo de menciones, optimizando el uso de memoria.
    
    :param file_path: Ruta del archivo JSON.
    :return: Lista de tuplas con los usuarios y su conteo de menciones.
    """
    # Procesar los chunks y concatenar solo las columnas necesarias
    chunks = []
    for chunk in load_json_in_chunks(file_path):
        df_chunk = process_chunk(chunk)
        chunks.append(df_chunk)
    df = pd.concat(chunks, ignore_index=True)
    
    # Explode y contar menciones
    all_mentions = df['mentions'].explode()
    user_count = all_mentions.groupby(df['username']).count()
    top_users = user_count.nlargest(10)
    
    return list(top_users.items())

