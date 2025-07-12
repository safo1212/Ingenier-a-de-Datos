# Sistema de Análisis de Datos para Fast and Safe

**Repositorio del proceso ETL para la construcción de la bodega de datos de la empresa de mensajería “Fast and Safe”.**

## Descripción del proyecto

Este proyecto desarrolla un proceso ETL (Extracción, Transformación y Carga) que alimenta una **bodega de datos** diseñada para consolidar y estructurar la información generada por clientes, mensajeros y servicios, a partir del sistema de información operacional existente.

---

**Desarrollado por:**

- 2400452 - Jennifer Benavides Castillo  
- 2400479 - Cristhian David Cruz Millán  
- 2400794 - Sergio Alejandro Fierro Ospitia  
- 2400478 - Edwin Andrés Lasso Rosero

```
 

#win
python -m venv my_env

#cmd.exe
C:\> <venv>\Scripts\activate.bat

#PowerShell
.\my_env\Scripts\Activate.ps1
```
your terminal should look like
```
(my_env) $
```
here you can install the packages by doing 
```
pip install -r requirements.txt
```

here you can install a missing package 
```
pip install psycopg2
```
structure of config.yml 
```
nombre_conexion:
  drivername: postgresql  
  user: postgres # su username
  password : valor_privado
  port: 5432 # pordefecto 
  host: localhost # la direccion a la base de datos
  dbname: colombia_saludable #nombre de la base de datos
```