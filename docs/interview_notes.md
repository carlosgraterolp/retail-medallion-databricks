# Guía de estudio: Retail Medallion Pipeline

Preguntas probables sobre este proyecto, con respuestas cortas basadas en lo que construí.
Cifras de la última corrida: bronze 1.067.371 filas → silver 1.027.015 + cuarentena 6.209 +
duplicados 34.147; `fact_sales` 1.027.015; 33/33 checks OK; job completo (5 tareas, con dbt) ~4 min en serverless.
Capa dbt: 5 modelos y 38 tests (43/43 OK); RFM 5.939 clientes; 4.738 productos; 3.237 filas día × país.

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
  versiona en Git los recursos *del proyecto*: aquí 4 esquemas, el volumen, el job con sus 5 tareas (4 notebooks + dbt) y el dashboard.
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

## 16. ¿Por qué dbt encima de notebooks de Spark? (dbt vs notebooks)
Cada herramienta donde rinde más. **Spark/notebooks**: ingesta de archivos, tipado, deduplicación,
cuarentena, el esquema estrella; trabajo pesado y orientado a archivos. **dbt**: modelado de negocio
en SQL que un analista puede leer y revisar, con tests y documentación al lado de cada modelo, y
linaje automático. dbt solo **lee** gold (declarado como `sources`) y escribe en su propio esquema,
`retail_marts`. En el job, `dbt_marts` es la quinta tarea: un `dbt_task` en serverless contra el SQL
warehouse; Databricks genera el profile e inyecta el token del usuario, no guardo credenciales.

## 17. `ref()` vs `source()`
- `source('gold', 'fact_sales')`: tablas que **no** construye dbt (aquí, las de los notebooks).
  Se declaran en `sources.yml` con descripción, tests y freshness.
- `ref('stg_sales')`: otro modelo **de dbt**. dbt arma el DAG con los `ref`, ejecuta en orden y
  resuelve el nombre real (catálogo/esquema) según el target.
- Ambos alimentan el linaje; hardcodear `workspace.retail_gold.fact_sales` rompería las dos cosas.

## 18. Materializaciones: view, table, incremental (¿cuándo cada una?)
- **view**: barata, siempre actual, se recalcula al consultarla. Para staging ligero
  (`stg_sales`, `stg_customers`).
- **table**: se recrea completa en cada `dbt run`. Para marts que se consultan mucho y cuyo costo de
  recálculo es bajo (`mart_customer_rfm`, `mart_product_performance`). Además RFM *necesita* recalcularse
  entero: `percent_rank` depende de todos los clientes.
- **incremental**: solo procesa lo nuevo o cambiado. Para tablas grandes que crecen con el tiempo,
  con una clave clara (`mart_daily_sales`, por día y país).
- (**ephemeral**: CTE inyectada, no se materializa. No la usé.)

## 19. Estrategias incrementales (dbt-databricks)
- **merge** (la que usé, default en Delta): `MERGE` por `unique_key` = `(sale_date, country)`.
  Actualiza filas existentes e inserta nuevas; idempotente si reproceso un día.
- **append**: solo inserta; rápido pero duplica si reproceso.
- **insert_overwrite** / **replace_where**: reemplaza particiones o un rango (p. ej. "los últimos 3 días").
- **delete+insert** y **microbatch** (por lotes de tiempo).
- **Ventana de reproceso (`lookback_days` = 3)**: cada corrida reprocesa desde (última fecha cargada
  − 3 días) hasta la fecha más nueva, **ambos extremos incluidos**: son **4 días** ya cargados (la última
  fecha y los 3 anteriores) más los días nuevos. Así capto correcciones tardías (una cancelación días después).
- Demo (simulada, la fuente es estática): corrida 1 con `cutoff_date = 2011-11-30` → 3.178 filas;
  corrida 2 → escribe **85 filas** (12 días, 27/11 a 9/12: 4 reprocesados + 8 nuevos); corrida 3 sin
  datos nuevos → **31 filas** (los 4 días reprocesados, 6/12 a 9/12). El total cuadra con `fact_sales` al centavo (£19.002.774,21).
- `is_incremental()` es falso en la primera corrida o con `--full-refresh`.

## 20. Tests genéricos vs singulares
- **Genéricos**: reutilizables y parametrizados en YAML: `unique`, `not_null`, `relationships`,
  `accepted_values`. Tengo 34 (claves, relaciones fact → dims, segmentos válidos).
- **Singulares**: un SELECT en `tests/` que devuelve las filas que violan la regla; pasa si devuelve 0.
  Tengo 4: el mart diario cuadra con `fact_sales`; todo cliente no invitado está en RFM; el segmento
  "No net value" es exactamente monetary <= 0 y sin scores; las participaciones del Pareto suman 100%.
- Me salvaron dos veces: en Spark `least(5, NULL) = 5` (ignora NULLs), y había dado score 5 a clientes
  sin valor; y mi primer test de Pareto suponía mal que el acumulado nunca pasa de 100% (33 productos
  con ingreso neto <= 0 al final del ranking lo hacen subir y bajar).
- Además: **source freshness** sobre `_loaded_at` (avisa a los 7 días, solo warn).

## 21. ¿Por qué RFM y cómo definiste los segmentos?
- RFM (Recency, Frequency, Monetary) es simple, explicable y accionable: dice a quién retener y a quién
  premiar, sin un modelo de ML.
- **R** = días desde la última compra hasta 2011-12-10 (día siguiente a la última factura, no "hoy",
  porque los datos son históricos). **F** = facturas no canceladas. **M** = ingreso neto.
- **Empates**: `score = least(5, 1 + floor(percent_rank * 5))`. `percent_rank` da el mismo rango a
  valores iguales, así que clientes empatados reciben el **mismo** score. Con `NTILE(5)` los 1.593
  clientes de una sola factura habrían caído en buckets distintos de forma arbitraria. Costo: los
  grupos no tienen el mismo tamaño.
- **Monetary <= 0** (solo cancelaciones o compras revertidas, 102 clientes): no se puntúan, segmento
  "No net value". Los mantengo para que todo cliente aparezca (y un test lo exige).
- Segmentos (primera regla que cumple, sobre R y F): Champions (R>=4, F>=4), Loyal (R>=3, F>=3),
  New (R>=4), Potential loyalist (R=3), At risk (R<=2, F>=3), Lost (el resto). M se reporta para priorizar.
- **Hallazgos**: Champions = 23,4% de los clientes identificados y **70,1%** de su ingreso neto
  (£11,41M de £16,29M). "At risk": 712 clientes, 5,4 compras de media pero ~358 días sin comprar,
  £1,33M: el objetivo obvio de retención. Pareto: **21,9%** de los productos (1.038 de 4.738) hacen el 80%.

## 22. ¿Qué mejorarías? (siguientes pasos)
1. ~~Modelos dbt sobre gold~~ (hecho).
2. **Terraform** para los recursos del workspace.
3. **CI con GitHub Actions**: validar el bundle, `dbt build` y los checks de calidad en cada PR.
4. SCD2 en `dim_customer` (snapshots de dbt) e ingesta incremental real en bronze (Auto Loader).

## Preguntas trampa
- *"¿Por qué hay 22% de ventas sin cliente y no las eliminaste?"*: son ventas reales (compras como
  invitado); eliminarlas subestimaría los ingresos. Van al cliente `-1`.
- *"¿Tasa de cancelación?"*: monto en facturas de cancelación / ventas brutas = 7,15%. No la llamo
  tasa de devolución porque la fuente no distingue devoluciones de otras cancelaciones.
- *"¿Duplicados legítimos?"*: la fuente no trae número de línea; asumí que dos líneas idénticas en la
  misma factura y el mismo segundo son un duplicado. Está documentado como supuesto.
