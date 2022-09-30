# Proyecto #1 Flujos de ejecución
## Descripción
Client: Ministerio de Educación de la Nación
Situación inicial
📍
Somos un equipo de desarrollo y data analytics, que trabajamos para la consultora “MyData”
y nuestro líder técnico nos comparte un pedido comercial directamente del Consejo Nacional
de Calidad de la Educación (por sus siglas, CNCE).

El CNCE es un grupo deliberante que pertenece al Ministerio de Educación de la Nación
Argentina. 

Este se encuentra analizando opciones universitarias disponibles en los últimos 10
años para comparar datos extraídos de universidades de todo el país, públicas y privadas,
con el fin de tener una muestra representativa que facilite el análisis.
Para esto, compartieron a “MyData” información disponible de más de 15 universidades y
centros educativos con gran volumen de datos sensibles sobre las inscripciones de alumnos.
El CNCE requiere que preparemos el set de datos para que puedan analizar la información
relevante y tomar directrices en cuanto a qué carreras universitarias requieren programa de
becas, qué planes de estudios tienen adhesión, entre otros.

### Tu objetivo

📋 Como parte de un equipo de desarrollo y data analytics de “MyData”, deberás analizar y
preparar flujos de ejecución del set de datos recibido para obtener las comparaciones y
mediciones requeridas por el CNCE.

#### Requerimientos 🔧

● El Ministerio necesita que ordenemos los datos para obtener un archivo con sólo la
información necesaria de cierto periodo de tiempo y de determinados lugares
geográficos de una base de datos SQL (las especificaciones serán vistas en la primera
reunión de equipo). Será necesario generar un diagrama de base de datos para que se
comprenda la estructura.


● Los datos deben ser procesados de manera que se puedan ejecutar consultas a dos
universidades del total disponible para hacer análisis parciales. Para esto será
necesario realizar DAGs con Airflow que permitan procesar datos con Python y
consultas SQL.


● Calcular, evaluar y ajustar formatos de determinados datos como fechas, nombres,
códigos postales según requerimientos normalizados que se especifican para cada
grupo de universidades, utilizando Pandas.

### Assets 🎨


La base de datos con la información que reunió el Ministerio de Educación se encuentra aquí:

● A proveer en el transcurso del proyecto.


El archivo auxiliar de códigos postales se encuentra en la carpeta assets.


## Requerimientos:
# Airflow usando Docker
https://docs.astronomer.io/software/install-cli?tab=windows#install-the-astro-cli

## Modulos utilizados en Python
- pathlib
- logging
- pandas
- datetime
- os
- sqlalchemy

## Enlaces:
- Guia de instalación de Apache Airflow en Ubuntu: https://unixcop.com/how-to-install-apache-airflow-on-ubuntu-20

## Estructura y flujo de ejecución
  Se generarán archivos ".sql" con las consultas correspondientes a cada centro educativo, normalizando las columnas tenidas en cuenta.

  Mediante operadores disponibles en apache airflow (Python operators y postgre operators, se toman las consultas ".sql" para obtener los datos de la base de datos provista. 
  
  Estos datos se transorman mediante la libreria pandas, y se almacenan en forma local como archivos ".txt".

  Finalmete, a traves de las herramientas provistas por AWS (operadores y hooks S3), los datos almacenados como ".txt" son transformados a strings, y almacenados en el servicio S3.

# Creación de una Wiki del proyecto
Se recomienda crear una wiki del proyecto en Github para dejar anotaciones, lecciones aprendidas o convenciones necesarias adicionales.

# **Convención para nombrar carpetas**

OT000-python

   -airflow

      -assets: archivos complementarios necesarios.

      -dags: para dejar los flujos que se vayan creando

      -datasets: para dejar el archivo resultante del proceso de transformación con Pandas
      
      -files: para almacenar la extracción de la base de datos.

      -include: para almacenar los SQL.


# **Convención para nombrar archivos**
### DAG ETL
Se colocará grupo-letra-siglas de la universidad y localidad, seguido por "_dag_elt.py" para diferenciar de otros csv.

EJ: GFUNRioCuarto_dag_etl.py


# **Convencion para el nombre de la base de datos**

### conexion con base de datos
se llamara 'alkemy_db'

### conexion para S3
se llamara 'aws_s3_bucket'

### csv generados
Se colocará grupo-letra-siglas de la universidad y localidad, seguido por "_select.csv" para diferenciar el dag realizado.

EJ: GFUNRioCuarto_select.csv

### txt generados
Se colocará grupo-letra-siglas de la universidad y localidad, seguido por "_process.txt" para diferenciar el dag realizado.

EJ: GFUNRioCuarto_process.txt

# AIRFLOW

https://airflow.apache.org/

# Informacion importante de como comenzar con Airflow
https://www.astronomer.io/guides/airflow-sql-tutorial/

# Curso de Alkemy de Airflow
https://academy.alkemy.org/curso/python/contenidos/clase-1-introduccion-a-flujos-de-trabajo

# Guía definitiva de Airflow
[Guia](https://www.astronomer.io/ebooks/dags-definitive-guide.pdf)

# Airflow Hooks Explained 101
https://hevodata.com/learn/airflow-hooks/

# Instalar providers de amazon
https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/index.html
# Documentacion de amazon s3 en airflow
https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/_api/airflow/providers/amazon/aws/hooks/s3/index.html


# From Local Filesystem to Amazon S3
https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/operators/transfer/local_to_s3.html

## Quickstart AWS SDK for Python.
https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html

## How to use Boto3 to upload files to an S3 Bucket?
https://www.learnaws.org/2022/07/13/boto3-upload-files-s3/


# Airflow Dynamic DAGs: The powerful way with Jinja and YAML
https://www.youtube.com/watch?v=HuMgMTHrkn4&ab_channel=DatawithMarc

# Dynamically Generating DAGs in Airflow
https://www.astronomer.io/guides/dynamically-generating-dags/

# Loggers

# Configuración del archivo de logger.cfg
https://docs.python.org/3/library/logging.config.html#logging-config-fileformat

# Ejemplo de archivo de configuración
https://realpython.com/lessons/logger-config-file/