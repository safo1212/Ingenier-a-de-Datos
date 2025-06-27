import pandas as pd
from sqlalchemy.engine import Engine

def extract(target: str, con: Engine) -> pd.DataFrame:
    if target == "ciudad":
        return pd.read_sql_table('ciudad', con)

    elif target == "departamento":
        return pd.read_sql_table('departamento', con)

    elif target == "cliente":
        return pd.read_sql_table('cliente', con)

    elif target == "sede":
        return pd.read_sql_table('sede', con)

    elif target == "usuario":
        return pd.read_sql_table('clientes_usuarioaquitoy', con)

    elif target == "mensajero":
        # Unión con auth_user para obtener el nombre del mensajero
        query = """
            SELECT m.*, u.username AS nombre_mensajero
            FROM clientes_mensajeroaquitoy m
            JOIN auth_user u ON m.user_id = u.id
        """
        return pd.read_sql_query(query, con)

    elif target == "vehiculo":
        return pd.read_sql_table('mensajeria_tipovehiculo', con)

    elif target == "pago":
        return pd.read_sql_table('mensajeria_tipopago', con)

    elif target == "estado":
        return pd.read_sql_table('mensajeria_estado', con)

    elif target == "tiposervicio":
        return pd.read_sql_table('mensajeria_tiposervicio', con)

    elif target == "tiponovedad":
        return pd.read_sql_table('mensajeria_tiponovedad', con)

    elif target == "area":
        return pd.read_sql_table('areas_cliente', con)

    elif target == "cliente_usuario":
        return pd.read_sql_table('clientes_mensajeroaquitoy_clientes', con)

    elif target == "servicio":
        return pd.read_sql_table('mensajeria_servicio', con)[['id']]  # degenerada

    elif target == "fecha":
        return pd.read_sql_query("SELECT DISTINCT fecha_solicitud AS date FROM mensajeria_servicio", con)

    elif target == "hora":
        return pd.read_sql_query("SELECT DISTINCT hora_solicitud FROM mensajeria_servicio", con)

    elif target == "hecho_servicios":
        return pd.read_sql_table('mensajeria_servicio', con)

    elif target == "hecho_novedades":
        return pd.read_sql_table('mensajeria_novedadesservicio', con)

    elif target == "hecho_estados":
        return pd.read_sql_table('mensajeria_estadosservicio', con)

    else:
        raise ValueError(f"No se reconoce el extracto llamado '{target}'")
