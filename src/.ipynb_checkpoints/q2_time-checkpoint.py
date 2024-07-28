from typing import List, Tuple
import pandas as pd
import emoji
import orjson

def extract_emojis(text):
    return [char for char in text if emoji.is_emoji(char)]

def load_json_in_chunks(file_path: str, chunk_size=10000):
    """
    Carga un archivo JSON en chunks para optimizar el tiempo y uso de memoria.
    
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
    Procesa un chunk de datos convirtiéndolo en un DataFrame y extrayendo emojis.
    
    :param chunk: Lista de registros JSON.
    :return: DataFrame procesado con una columna de emojis.
    """
    df_chunk = pd.DataFrame(chunk)
    
    # Extraer emojis del contenido
    df_chunk['emojis'] = df_chunk['content'].apply(extract_emojis)
    
    return df_chunk

def q2_time(file_path: str) -> List[Tuple[str, int]]:
    """
    Obtiene los 10 emojis más usados con su respectivo conteo, optimizando el tiempo de ejecución.
    
    :param file_path: Ruta del archivo JSON.
    :return: Lista de tuplas con los emojis y su conteo.
    """
    # Procesar los chunks y concatenar
    chunks = []
    for chunk in load_json_in_chunks(file_path):
        df_chunk = process_chunk(chunk)
        chunks.append(df_chunk)
    df = pd.concat(chunks, ignore_index=True)
    
    # Explode y contar emojis
    all_emojis = df['emojis'].explode()
    top_emojis = all_emojis.value_counts().head(10)
    
    return list(top_emojis.items())