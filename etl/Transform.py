import pandas as pd
import numpy as np
from datetime import date

def limpiar_columnas(df: pd.DataFrame) -> pd.DataFrame:
    columnas_a_eliminar = ["coordinador_id", "tipo_cliente_id", "hora_num", "url_foto", "token_Firebase", "hora_num", "lider", "foto",
                           "foto_binary", "nombre_solicitante", "nombre_recibe", "telefono_recibe", "descripcion_pago", "descripcion_cancelado"]
    df = df.drop(columns=[col for col in columnas_a_eliminar if col in df.columns])
    df = df.dropna(axis=1, how="all")
    return df

def transform_generico(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in df.columns:
        if df[col].dtype in [float, object, "datetime64[ns]"]:
            df[col] = df[col].fillna("NO DISPONIBLE")
    df["saved"] = date.today()
    return limpiar_columnas(df)

def transform_usuario(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.fillna("NO DISPONIBLE", inplace=True)
    df["saved"] = date.today()
    return df

def transform_mensajero(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if 'nombre_mensajero' in df.columns:
        df.rename(columns={'nombre_mensajero': 'nombre'}, inplace=True)
    df = df.fillna("NO DISPONIBLE")
    df["saved"] = date.today()
    return limpiar_columnas(df)

def transform_fecha(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df.dropna(subset=['date'], inplace=True)
    df["key_dim_fecha"] = df["date"].rank(method="dense").astype("int")
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month
    df["day"] = df["date"].dt.day
    df["weekday"] = df["date"].dt.weekday
    df["quarter"] = df["date"].dt.quarter
    df["day_of_year"] = df["date"].dt.day_of_year
    df["month_str"] = df["date"].dt.month_name()
    df["day_str"] = df["date"].dt.day_name()
    df["saved"] = date.today()
    return df

def transform_hora(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df['hora'] = pd.to_datetime(df['hora_solicitud'], format='%H:%M:%S', errors='coerce').dt.time
    df = df.dropna(subset=['hora']).drop(columns=['hora_solicitud']).drop_duplicates()
    df['hora_str'] = df['hora'].astype(str)
    df["key_dim_hora"] = df["hora_str"].rank(method="dense").astype("int")
    df["hora_num"] = df['hora'].apply(lambda x: x.hour + x.minute / 60)
    df["rango_hora"] = pd.cut(df["hora_num"],
                              bins=[0, 6, 12, 18, 24],
                              labels=["madrugada", "mañana", "tarde", "noche"],
                              right=False)
    df["saved"] = date.today()
    return df

def transform_hecho_servicios(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "es_prueba" in df.columns:
        df = df[df["es_prueba"] == False].copy()

    if 'fecha_solicitud' in df.columns:
        df['fecha_solicitud'] = pd.to_datetime(df['fecha_solicitud'], errors='coerce')
    if 'fecha_entrega' in df.columns:
        df['fecha_entrega'] = pd.to_datetime(df['fecha_entrega'], errors='coerce')
        df["tiempo_total_minutos"] = (
            (df['fecha_entrega'] - df['fecha_solicitud']).dt.total_seconds() / 60
        ).fillna(0).astype(int)
    else:
        df["tiempo_total_minutos"] = 0
    if 'hora_solicitud' in df.columns:
        df['hora_solicitud'] = pd.to_datetime(df['hora_solicitud'], errors='coerce').dt.time
    if 'hora_entrega' in df.columns:
        df['hora_entrega'] = pd.to_datetime(df['hora_entrega'], errors='coerce').dt.time
    df["saved"] = date.today()
    return df

def transform_hecho_novedades(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "es_prueba" in df.columns:
        df = df[df["es_prueba"] == False].copy()

    if 'fecha_registro' in df.columns:
        df['fecha_registro'] = pd.to_datetime(df['fecha_registro'], errors='coerce')
    if 'descripcion' in df.columns:
        df['descripcion'] = df['descripcion'].fillna('NO DISPONIBLE')
    df["saved"] = date.today()
    return df

def transform_hecho_estados(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "es_prueba" in df.columns:
        df = df[df["es_prueba"] == False].copy()

    if 'fecha_estado' in df.columns:
        df['fecha_estado'] = pd.to_datetime(df['fecha_estado'], errors='coerce')
    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].fillna("NO DISPONIBLE")
    df["saved"] = date.today()
    return df
