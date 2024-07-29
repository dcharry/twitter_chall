from typing import List, Tuple
from datetime import datetime
import orjson
import pandas as pd

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

def process_chunk(chunk: List[dict]) -> pd.DataFrame:
    """
    Procesa un chunk de datos convirtiéndolo en un DataFrame y ajustando la columna de fecha.
    Convierte las columnas a tipos de datos más eficientes.
    
    :param chunk: Lista de registros JSON.
    :return: DataFrame procesado.
    """
    df_chunk = pd.DataFrame(chunk)
    
    # Convertir la columna 'date' a datetime y luego a solo fecha
    df_chunk['date'] = pd.to_datetime(df_chunk['date']).dt.date
    
    # Extraer nombre de usuario
    user_df = pd.json_normalize(df_chunk['user'])
    
    # Solo mantener las columnas necesarias
    df_chunk = df_chunk[['date']]
    df_chunk['user'] = user_df['username'].astype('category')
    
    return df_chunk

def q1_memory(file_path: str) -> List[Tuple[datetime.date, str]]:
    """
    Obtiene las 10 fechas con más tweets y el usuario con más publicaciones en cada una de esas fechas,
    optimizando el uso de memoria.
    
    :param file_path: Ruta del archivo JSON.
    :return: Lista de tuplas con la fecha y el usuario con más publicaciones en esa fecha.
    """
    # Procesar los chunks y concatenar
    chunks = []
    for chunk in load_json_in_chunks(file_path):
        df_chunk = process_chunk(chunk)
        chunks.append(df_chunk)
    df = pd.concat(chunks, ignore_index=True)
    
    # Convertir tipos de datos después de concatenar
    df['user'] = df['user'].astype('category')
    
    # Obtener las 10 fechas con más tweets
    top_dates = df['date'].value_counts().head(10).index
    
    result = []
    for date in top_dates:
        # Filtrar DataFrame por la fecha específica
        sub_df = df[df['date'] == date]
        
        # Encontrar el usuario con más tweets en esa fecha
        top_user = sub_df['user'].value_counts().idxmax()
        
        result.append((date, top_user))
    
    return result
