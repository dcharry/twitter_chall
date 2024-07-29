from typing import List, Tuple
from datetime import datetime
import pandas as pd
import orjson

def load_json_in_chunks(file_path: str, chunk_size: int = 10000) -> List[dict]:
    """
    Carga un archivo JSON en chunks para optimizar el tiempo y uso de memoria.
    
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

def q1_time(file_path: str) -> List[Tuple[datetime.date, str]]:
    """
    Obtiene las 10 fechas con más tweets y el usuario con más publicaciones en cada una de esas fechas.
    
    :param file_path: Ruta del archivo JSON.
    :return: Lista de tuplas con la fecha y el usuario con más publicaciones en esa fecha.
    """
    # Procesar los chunks y concatenar
    chunks = []
    for chunk in load_json_in_chunks(file_path):
        df_chunk = pd.DataFrame(chunk)
        chunks.append(df_chunk)
    df = pd.concat(chunks, ignore_index=True)
    
    # Convertir la columna 'date' a datetime y luego a solo fecha
    df['date'] = pd.to_datetime(df['date']).dt.date
    
    # Obtener las 10 fechas con más tweets
    top_dates = df['date'].value_counts().head(10).index
    
    result = []
    for date in top_dates:
        # Filtrar DataFrame por la fecha específica
        sub_df = df[df['date'] == date]
        
        # Encontrar el usuario con más tweets en esa fecha
        top_user = sub_df['user'].apply(lambda x: x['username']).value_counts().idxmax()
        
        result.append((date, top_user))
    
    return result

