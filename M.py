import pandas as pd
from sqlalchemy import create_engine, inspect, text
from datetime import date
import yaml
from etl import E as extract, T as transform, L as load

def hay_datos_nuevos(conne):
    try:
        inspector = inspect(conne)
        if 'hecho_servicios' not in inspector.get_table_names():
            return True

        query = text('SELECT MAX(saved) FROM hecho_servicios;')
        with conne.connect() as con:
            result = con.execute(query).scalar()
            if result is None or result < date.today():
                return True
            return False
    except Exception:
        return True

def main():
    # Leer config
    with open('config.yml', 'r') as f:
        config = yaml.safe_load(f)
        config_co = config['CO_SA']
        config_etl = config['ETL_PRO']

    # Crear motores
    url_co = f"{config_co['drivername']}://{config_co['user']}:{config_co['password']}@{config_co['host']}:{config_co['port']}/{config_co['dbname']}"
    url_etl = f"{config_etl['drivername']}://{config_etl['user']}:{config_etl['password']}@{config_etl['host']}:{config_etl['port']}/{config_etl['dbname']}"

    co_sa = create_engine(url_co)
    etl_conn = create_engine(url_etl)

    dimensiones = {}
    hechos = {}

    if hay_datos_nuevos(etl_conn):

        nombres_dim = [
            "ciudad", "departamento", "cliente", "sede", "usuario",
            "mensajero", "vehiculo", "pago", "estado", "tiposervicio",
            "tiponovedad", "area", "cliente_usuario", "servicio",
            "fecha", "hora"
        ]

        # Diccionario para transformaciones especiales
        transformaciones_especiales = {
            "mensajero": transform.transform_mensajero,
            "fecha": transform.transform_fecha,
            "hora": transform.transform_hora,
            "usuario": transform.transform_usuario
        }

        for nombre in nombres_dim:
            df = extract.extract(nombre, co_sa)
            func = transformaciones_especiales.get(nombre, transform.transform_generico)
            dimensiones[nombre] = func(df)
            load.load(dimensiones[nombre], etl_conn, f"dim_{nombre}", replace=True)

        hechos["servicios"] = transform.transform_hecho_servicios(
            extract.extract("hecho_servicios", co_sa)
        )
        hechos["novedades"] = transform.transform_hecho_novedades(
            extract.extract("hecho_novedades", co_sa)
        )
        hechos["estados"] = transform.transform_hecho_estados(
            extract.extract("hecho_estados", co_sa)
        )

        for nombre, df in hechos.items():
            load.load(df, etl_conn, f"hecho_{nombre}", replace=True)

if __name__ == "__main__":
    main()
