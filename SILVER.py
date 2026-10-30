# Databricks notebook source
# MAGIC %md
# MAGIC ## TRATAMIENTO DE DATOS

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## TIPIFICACION DE TABLAS

# COMMAND ----------

# MAGIC %sql
# MAGIC USE aerolinea.bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM aeronaves LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW aeronaves_tipificadas AS
# MAGIC SELECT DISTINCT
# MAGIC        UPPER(TRIM(aeronave_id)) AS aeronave_id,
# MAGIC        TRIM(modelo) AS modelo,
# MAGIC        TRIM(fabricante) AS fabricante,
# MAGIC        TRY_CAST(anio_fabricacion AS INT) AS anio_fabricacion,
# MAGIC        archivo_origen
# MAGIC FROM aeronaves

# COMMAND ----------

# MAGIC %md
# MAGIC ## AERONAVES

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM aeronaves_tipificadas limit 5 

# COMMAND ----------

# MAGIC %md
# MAGIC ## AEROPUERTOS

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW aeropuertos_tipificados AS
# MAGIC SELECT DISTINCT
# MAGIC        UPPER(TRIM(aeropuerto_id)) AS aeropuerto_id,
# MAGIC        TRIM(nombre) AS nombre, TRIM(ciudad) AS ciudad, TRIM(pais) AS pais,
# MAGIC        TRY_CAST(lat AS DOUBLE) AS lat,
# MAGIC        TRY_CAST(lon AS DOUBLE) AS lon,
# MAGIC        archivo_origen
# MAGIC FROM aeropuertos

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM aeropuertos_tipificados limit 5 

# COMMAND ----------

# MAGIC %md
# MAGIC ## VUELOS

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW vuelos_tipificados AS
# MAGIC SELECT DISTINCT
# MAGIC        UPPER(TRIM(vuelo_id)) AS vuelo_id,
# MAGIC        TRY_CAST(fecha AS DATE) AS fecha,
# MAGIC        UPPER(TRIM(origen_id)) AS origen_id,
# MAGIC        UPPER(TRIM(destino_id)) AS destino_id,
# MAGIC        UPPER(TRIM(aeronave_id)) AS aeronave_id,
# MAGIC        LOWER(TRIM(estado)) AS estado,
# MAGIC        TRY_CAST(duracion_min AS INT) AS duracion_min,
# MAGIC        UPPER(TRIM(origen_id)) = UPPER(TRIM(destino_id)) AS ruta_por_revisar,
# MAGIC        fecha_ingesta
# MAGIC FROM vuelos

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM vuelos_tipificados limit 5 

# COMMAND ----------

# MAGIC %md
# MAGIC ## MANTENIMIENTOS

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW mantenimientos_tipificados AS
# MAGIC SELECT DISTINCT
# MAGIC        UPPER(TRIM(mantenimiento_id)) AS mantenimiento_id,
# MAGIC        UPPER(TRIM(aeronave_id)) AS aeronave_id,
# MAGIC        TRY_CAST(fecha AS DATE) AS fecha,
# MAGIC        LOWER(TRIM(tipo)) AS tipo,
# MAGIC        TRY_CAST(costo_usd AS DECIMAL(14,2)) AS costo_usd,
# MAGIC        TRY_CAST(duracion_hr AS DECIMAL(10,2)) AS duracion_hr,
# MAGIC        fecha ingesta
# MAGIC FROM mantenimientos

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM mantenimientos_tipificados limit 5 

# COMMAND ----------

# MAGIC %md
# MAGIC ## REGLAS DEL CONTENIDO

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW control_contenido AS
# MAGIC SELECT 'aeronaves' AS conjunto, COUNT(*) AS errores
# MAGIC FROM aeronaves_tipificadas
# MAGIC WHERE aeronave_id IS NULL OR aeronave_id = '' OR modelo IS NULL OR modelo = ''
# MAGIC    OR fabricante IS NULL OR fabricante = '' OR anio_fabricacion IS NULL
# MAGIC    OR anio_fabricacion < 1900 OR anio_fabricacion > YEAR(current_date())
# MAGIC UNION ALL
# MAGIC SELECT 'aeropuertos', COUNT(*) FROM aeropuertos_tipificados
# MAGIC WHERE aeropuerto_id IS NULL OR aeropuerto_id = '' OR nombre IS NULL OR nombre = ''
# MAGIC    OR ciudad IS NULL OR ciudad = '' OR pais IS NULL OR pais = ''
# MAGIC    OR lat IS NULL OR lon IS NULL OR isnan(lat) OR isnan(lon)
# MAGIC    OR lat NOT BETWEEN -90 AND 90 OR lon NOT BETWEEN -180 AND 180
# MAGIC UNION ALL
# MAGIC SELECT 'vuelos', COUNT(*) FROM vuelos_tipificados
# MAGIC WHERE vuelo_id IS NULL OR vuelo_id = '' OR fecha IS NULL
# MAGIC    OR aeronave_id IS NULL OR aeronave_id = ''
# MAGIC    OR origen_id IS NULL OR origen_id = '' OR destino_id IS NULL OR destino_id = ''
# MAGIC    OR estado IS NULL OR estado NOT IN ('a_tiempo','retrasado','cancelado')
# MAGIC    OR duracion_min < 0
# MAGIC    OR (estado <> 'cancelado' AND (duracion_min IS NULL OR duracion_min = 0))
# MAGIC UNION ALL
# MAGIC SELECT 'mantenimientos', COUNT(*) FROM mantenimientos_tipificados
# MAGIC WHERE mantenimiento_id IS NULL OR mantenimiento_id = '' OR fecha IS NULL
# MAGIC    OR aeronave_id IS NULL OR aeronave_id = '' OR tipo IS NULL
# MAGIC    OR tipo NOT IN ('preventivo','correctivo','inspección')
# MAGIC    OR costo_usd IS NULL OR costo_usd < 0 OR duracion_hr IS NULL OR duracion_hr <= 0

# COMMAND ----------

contenido = spark.table("control_contenido")
display(contenido)
assert contenido.filter("errores > 0").count() == 0, "Revisar el contenido antes de publicar Silver."

# COMMAND ----------

# MAGIC %md
# MAGIC ## REVISION DE CONFLICTOS POR LLAVE 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW control_llaves AS
# MAGIC SELECT 'aeronaves' AS conjunto, COUNT(*) AS errores FROM (
# MAGIC   SELECT aeronave_id FROM aeronaves_tipificadas GROUP BY aeronave_id HAVING COUNT(*) > 1
# MAGIC ) a
# MAGIC UNION ALL
# MAGIC SELECT 'aeropuertos', COUNT(*) FROM (
# MAGIC   SELECT aeropuerto_id FROM aeropuertos_tipificados GROUP BY aeropuerto_id HAVING COUNT(*) > 1
# MAGIC ) b
# MAGIC UNION ALL
# MAGIC SELECT 'vuelos', COUNT(*) FROM (
# MAGIC   SELECT vuelo_id FROM vuelos_tipificados GROUP BY vuelo_id HAVING COUNT(*) > 1
# MAGIC ) c
# MAGIC UNION ALL
# MAGIC SELECT 'mantenimientos', COUNT(*) FROM (
# MAGIC   SELECT mantenimiento_id FROM mantenimientos_tipificados GROUP BY mantenimiento_id HAVING COUNT(*) > 1
# MAGIC ) d

# COMMAND ----------

llaves = spark.table("control_llaves")
display(llaves)
assert llaves.filter("errores > 0").count() == 0, "Hay llaves con registros diferentes."

# COMMAND ----------

# MAGIC %md
# MAGIC ## REVISION DE REFERENCIAS Y COHERENCIA TEMPORAL

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW control_referencias AS
# MAGIC SELECT 'vuelo_aeronave' AS regla, COUNT(*) AS errores
# MAGIC FROM vuelos_tipificados v LEFT ANTI JOIN aeronaves_tipificadas a ON v.aeronave_id = a.aeronave_id
# MAGIC UNION ALL
# MAGIC SELECT 'vuelo_origen', COUNT(*)
# MAGIC FROM vuelos_tipificados v LEFT ANTI JOIN aeropuertos_tipificados a ON v.origen_id = a.aeropuerto_id
# MAGIC UNION ALL
# MAGIC SELECT 'vuelo_destino', COUNT(*)
# MAGIC FROM vuelos_tipificados v LEFT ANTI JOIN aeropuertos_tipificados a ON v.destino_id = a.aeropuerto_id
# MAGIC UNION ALL
# MAGIC SELECT 'mantenimiento_aeronave', COUNT(*)
# MAGIC FROM mantenimientos_tipificados m LEFT ANTI JOIN aeronaves_tipificadas a ON m.aeronave_id = a.aeronave_id
# MAGIC UNION ALL
# MAGIC SELECT 'fabricacion_posterior_vuelo', COUNT(*)
# MAGIC FROM vuelos_tipificados v JOIN aeronaves_tipificadas a ON v.aeronave_id = a.aeronave_id
# MAGIC WHERE a.anio_fabricacion > YEAR(v.fecha)
# MAGIC UNION ALL
# MAGIC SELECT 'fabricacion_posterior_mantenimiento', COUNT(*)
# MAGIC FROM mantenimientos_tipificados m JOIN aeronaves_tipificadas a ON m.aeronave_id = a.aeronave_id
# MAGIC WHERE a.anio_fabricacion > YEAR(m.fecha)

# COMMAND ----------

referencias = spark.table("control_referencias")
display(referencias)
assert referencias.filter("errores > 0").count() == 0, "Revisar las referencias o los años de fabricación."

# COMMAND ----------

# MAGIC %md
# MAGIC ## OBSERVACIONES VISIBLES

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS rutas_por_revisar FROM vuelos_tipificados WHERE ruta_por_revisar;
# MAGIC SELECT vuelo_id, fecha, origen_id, destino_id, estado
# MAGIC FROM vuelos_tipificados WHERE ruta_por_revisar ORDER BY vuelo_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## INGESTA A CAPA SILVER

# COMMAND ----------

# MAGIC %sql
# MAGIC use aerolinea.silver

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE aeronaves USING DELTA AS SELECT * FROM aeronaves_tipificadas;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE aeropuertos USING DELTA AS SELECT * FROM aeropuertos_tipificados;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE vuelos USING DELTA AS SELECT * FROM vuelos_tipificados;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE mantenimientos USING DELTA AS SELECT * FROM mantenimientos_tipificados;
# MAGIC

# COMMAND ----------

## COMPROBACION DE CAPAS SILVER

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from aeronaves limit 5

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from aeropuertos limit 5

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vuelos limit 5

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from mantenimientos limit 5