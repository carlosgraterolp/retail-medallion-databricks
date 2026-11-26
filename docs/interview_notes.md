# Guía de estudio: Retail Medallion Pipeline

Preguntas probables sobre este proyecto, con respuestas cortas basadas en lo que construí.
Cifras de la última corrida: bronze 1.067.371 filas → silver 1.027.015 + cuarentena 6.209 +
duplicados 34.147; `fact_sales` 1.027.015; 33/33 checks OK; ~2 min en serverless.

---

## 1. Cuéntame el proyecto en 1 minuto
Pipeline batch en Databricks Free Edition con datos públicos reales (UCI Online Retail II,
~1M líneas de factura de una tienda online del Reino Unido, 2009–2011). Medallion:
bronze guarda el crudo como texto con metadatos; silver tipifica, limpia, deduplica y manda a
cuarentena lo que falla reglas; gold es un esquema estrella Kimball con agregados para un dashboard.
Un notebook de calidad hace 33 checks y falla el job si alguno no pasa. Todo se despliega con un
Declarative Automation Bundle (`databricks bundle deploy` / `bundle run`).

## 2. Medallion vs Kimball: ¿compiten?
No. **Medallion** organiza el *flujo y nivel de refinamiento* (bronze = crudo, silver = limpio y
conformado, gold = listo para consumo). **Kimball** es una técnica de *modelado dimensional* para la
capa de consumo. Encajan así: la capa gold **es** el esquema estrella. Silver queda a nivel
transaccional, normalizado y sin lógica de negocio de reporte.

## 3. ¿Hecho vs dimensión?
- **Hecho**: eventos medibles a un grano definido, con medidas numéricas y claves foráneas.
  Aquí `fact_sales`: cantidad, precio unitario, `line_amount`.
- **Dimensión**: el contexto descriptivo para filtrar y agrupar (quién, qué, cuándo, dónde):
  `dim_product`, `dim_customer`, `dim_country`, `dim_date`.
- `invoice_no` es una **dimensión degenerada**: un atributo en el hecho sin tabla propia.

## 4. ¿Cuál es el grano y por qué?
**Una línea de factura.** Es lo más atómico que da la fuente: de ahí puedo sacar ventas por factura,
día, cliente o país, pero de un agregado no puedo volver al detalle. Primero se declara el grano,
después se eligen dimensiones y medidas (proceso de 4 pasos de Kimball).

## 5. ¿Por qué claves sustitutas (surrogate keys)?
- Desacoplan el modelo de los códigos de origen (si la fuente recicla o cambia un código, no rompe).
- Joins por enteros, compactos.
- Permiten el **miembro desconocido** (`-1`): el 22% de las líneas no tiene cliente, y la FK nunca es NULL.
- Son la base para SCD2 (varias versiones del mismo cliente = varias claves).
- Implementación: columnas `IDENTITY` de Delta + `MERGE`. Verifiqué que una segunda corrida no
  cambia ninguna clave (huella de claves de `dim_product` idéntica antes y después).
- `dim_date` usa clave "inteligente" `yyyymmdd`: es la excepción aceptada para fechas.

## 6. Tipos de SCD y cuál usé
- **Tipo 0**: nunca cambia. **Tipo 1**: sobrescribe, sin historia. **Tipo 2**: nueva fila por
  versión (`valid_from`, `valid_to`, `is_current`), con historia completa. **Tipo 3**: columna "valor anterior".
- **Usé Tipo 1** (`MERGE ... WHEN MATCHED AND cambió THEN UPDATE`). Ejemplo real: 1.232 códigos de
  producto tienen más de una descripción; me quedo con la más reciente. 13 clientes compraron
  desde 2 países; me quedo con el último.
- Para Tipo 2: en el MERGE, cerrar la fila vigente (`valid_to`, `is_current = false`) e insertar
  la nueva versión; el hecho se une por clave natural + rango de fechas.

## 7. Delta Lake: ¿qué aporta?
Formato de tabla sobre Parquet con **transaction log** (`_delta_log`):
- Transacciones **ACID**: un `overwrite` que falla no deja la tabla a medias.
- **Schema enforcement** (y evolución explícita con `overwriteSchema`).
- **Time travel** (`VERSION AS OF`), `MERGE`, `UPDATE`, `DELETE`.
- Data skipping con estadísticas por archivo; **liquid clustering** (`CLUSTER BY date_key` en el hecho).

## 8. Evaluación perezosa (lazy evaluation) y acciones en Spark
Las **transformaciones** (`select`, `filter`, `withColumn`, `join`) solo construyen un plan lógico.
Nada se ejecuta hasta una **acción** (`count`, `collect`, `display`, `write/saveAsTable`). Spark
optimiza el plan completo (Catalyst) antes de ejecutar: por ejemplo empuja filtros y elimina columnas
no usadas. En bronze, el `saveAsTable` es la acción que realmente lee los CSV. Cuidado: cada acción
recalcula el plan si no hay caché; por eso cuento leyendo la tabla ya escrita.

## 9. Particionamiento: ¿particionaste?
No, a propósito. Con ~1M filas, particionar por fecha crearía muchos archivos pequeños (el
"small files problem") y empeoraría el rendimiento. La regla práctica es particionar solo tablas
grandes (cientos de GB o más) y por columnas de baja cardinalidad. En su lugar usé **liquid
clustering** por `date_key`, que agrupa datos para filtrar por fecha sin fijar la estructura de carpetas.
Distinto: las *particiones de Spark* en memoria (paralelismo) vs *particiones de la tabla* en disco.

## 10. ELT vs ETL
- **ETL**: transformas antes de cargar en el destino (motor externo).
- **ELT**: cargas crudo en la plataforma y transformas dentro de ella con su cómputo.
- Este proyecto es **ELT**: el crudo aterriza en bronze tal cual y las transformaciones corren en
  Spark dentro de Databricks. (La conversión xlsx → CSV local no transforma valores; solo cambia
  el formato porque Spark serverless no lee Excel.)

## 11. Calidad de datos: ¿cómo sabes que los números están bien?
- Reconciliación de filas: bronze = silver + cuarentena + duplicados (se valida en cada corrida).
- 33 checks: nulos y duplicados en claves, integridad referencial hecho → dimensiones (anti-join),
  y reconciliación de ingresos silver = hecho = agregados.
- **Falla ruidosamente**: `AssertionError` y el job queda FAILED. Lo probé insertando una fila
  huérfana: fallaron 3 de 33 checks (conteo, integridad referencial, ingresos).
- Resultados guardados en `dq_check_results` como auditoría.

## 12. ¿Por qué cuarentena y no borrar?
Borrar oculta la pérdida. La cuarentena guarda cada fila rechazada con sus `failed_rules`: la pérdida
es medible (6.209 filas, sobre todo precio cero y cantidades negativas sin cancelación) y si una regla
estaba mal, se reprocesa. Las cancelaciones (factura con "C") y las ventas sin cliente **no** se
rechazan: son hechos de negocio y se marcan con flags.

## 13. Infraestructura como código: Bundles vs Terraform
- **Declarative Automation Bundles** (antes Databricks Asset Bundles): YAML (`databricks.yml`) que
  versiona en Git los recursos *del proyecto*: aquí esquemas, volumen y el job con sus 4 tareas.
  `bundle deploy` crea/actualiza, `bundle run` ejecuta. Pensado para el ciclo de desarrollo de
  datos (targets dev/prod, despliegue del código junto a la definición).
- **Terraform** (provider de Databricks): para la *plataforma*: workspaces, metastore, permisos,
  redes, cloud. Gestiona estado y sirve para varias nubes y servicios.
- Regla práctica: Terraform para la infraestructura base, Bundles para los pipelines que corren encima.

## 14. Serverless en Free Edition: ¿qué cambió respecto a clusters?
Free Edition es solo serverless: no hay clusters clásicos ni configuración de nodos. Usa Spark
Connect (sin RDD API), ANSI activado (por eso `try_cast`), DBFS limitado (uso volúmenes de Unity
Catalog) y cuotas diarias de cómputo.

## 15. Unity Catalog
Gobierno centralizado con espacio de nombres de 3 niveles `catálogo.esquema.tabla`
(`workspace.retail_gold.fact_sales`). Los archivos viven en **volúmenes** (`/Volumes/...`) en lugar
de DBFS. Da permisos, linaje y auditoría en un solo lugar.

## 16. ¿Qué mejorarías? (siguientes pasos)
1. Modelos **dbt** sobre gold (tests declarativos, documentación, linaje).
2. **Terraform** para los recursos del workspace.
3. **CI con GitHub Actions**: validar el bundle y correr los checks de calidad en cada PR.
4. Carga incremental (Auto Loader / `MERGE` por fecha) y SCD2 en `dim_customer`.

## Preguntas trampa
- *"¿Por qué hay 22% de ventas sin cliente y no las eliminaste?"*: son ventas reales (compras como
  invitado); eliminarlas subestimaría los ingresos. Van al cliente `-1`.
- *"¿Tasa de cancelación?"*: monto en facturas de cancelación / ventas brutas = 7,15%. No la llamo
  tasa de devolución porque la fuente no distingue devoluciones de otras cancelaciones.
- *"¿Duplicados legítimos?"*: la fuente no trae número de línea; asumí que dos líneas idénticas en la
  misma factura y el mismo segundo son un duplicado. Está documentado como supuesto.
