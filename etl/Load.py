import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy import inspect, text

def load(df: pd.DataFrame, connection: Engine, table_name: str, replace: bool = False) -> None:
    """
    Carga un DataFrame a una tabla en PostgreSQL.

    :param df: DataFrame transformado a cargar.
    :param connection: Conexión a la base de datos destino (SQLAlchemy Engine).
    :param table_name: Nombre de la tabla destino en la base de datos.
    :param replace: Si es True, elimina datos existentes antes de cargar (sin borrar la tabla).
    """
    if df.empty:
        return  # Para no cargar DataFrames vacíos

    # Eliminar columnas sin datos por limpieza
    df = df.dropna(axis=1, how='all')

    inspector = inspect(connection)
    tables_existentes = inspector.get_table_names()

    if replace:
        if table_name in tables_existentes:
            with connection.connect() as conn:
                conn.execute(text(f"DELETE FROM {table_name}"))
        # Si la tabla no existe con algún nombre se creará
        if_exists = 'append'
    else:
        if_exists = 'append'

    df.to_sql(name=table_name, con=connection, if_exists=if_exists, index=False)
