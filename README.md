# Control y Gestión de Costos y Auditoría

25 Top queries para la gestión de costes y auditoria.
Consulta la documentación de [Tablas de Sistema](https://docs.databricks.com/aws/en/admin/system-tables/) para mas detalles.

Se quieres puedes descargar el [Notebook con las queries] (https://github.com/mousastech/monitor/blob/main/System%20Tables%20Queries.ipynb)

**01. ¿Cuánto es el consumo diario de Databricks?**

```sql
SELECT
    DATE(u.usage_date) AS fecha,
    SUM(u.usage_quantity) AS dbu_consumida,
    SUM(u.usage_quantity * lp.pricing.default) AS coste
FROM 
    system.billing.usage AS u
LEFT JOIN 
    system.billing.list_prices AS lp 
ON 
    u.cloud = lp.cloud 
    AND u.sku_name = lp.sku_name 
    AND u.usage_start_time >= lp.price_start_time 
    AND (u.usage_end_time <= lp.price_end_time OR lp.price_end_time IS NULL)
GROUP BY 
    DATE(u.usage_date)
ORDER BY 
    DATE(u.usage_date) ASC;
```

**02. ¿Cuánto es el consumo de Databricks por año/mes?**

```sql
SELECT
 u.workspace_id,
 CASE
     WHEN u.sku_name LIKE '%ALL_PURPOSE%' THEN 'ALL_PURPOSE'
     WHEN u.sku_name LIKE '%JOBS_COMPUTE%' THEN 'JOBS'
     WHEN u.sku_name LIKE '%DLT%' THEN 'DLT'
     WHEN u.sku_name LIKE '%ENTERPRISE_SQL_COMPUTE%' THEN 'SQL'
     WHEN u.sku_name LIKE '%ENTERPRISE_SQL_PRO_COMPUTE%' THEN 'SQL'
     WHEN u.sku_name LIKE '%SERVERLESS_SQL_COMPUTE%' THEN 'SQL_SERVERLESS'
     WHEN u.sku_name LIKE '%INFERENCE%' THEN 'MODEL_INFERENCE'
     ELSE 'OTHER'
 END AS sku,
 u.cloud,
 u.usage_date,
 date_format(u.usage_date, 'yyyy-MM') as YearMonth,
 u.usage_unit,
 u.usage_quantity,
 lp.pricing.default as list_price,
 lp.pricing.default * u.usage_quantity as list_cost,
 u.usage_quantity * lp.pricing.default as actual_cost
FROM
 system.billing.usage u
 LEFT JOIN system.billing.list_prices lp on u.cloud = lp.cloud and
   u.sku_name = lp.sku_name and
   u.usage_start_time >= lp.price_start_time and
   (u.usage_end_time <= lp.price_end_time or lp.price_end_time is null);
```

**03. ¿Cuál es el consumo de cada SKU en el mes actual?**

```sql
SELECT
  CASE
     WHEN u.sku_name LIKE '%ALL_PURPOSE%' THEN 'ALL_PURPOSE'
     WHEN u.sku_name LIKE '%JOBS_COMPUTE%' THEN 'JOBS'
     WHEN u.sku_name LIKE '%DLT%' THEN 'DLT'
     WHEN u.sku_name LIKE '%ENTERPRISE_SQL_COMPUTE%' THEN 'SQL'
     WHEN u.sku_name LIKE '%ENTERPRISE_SQL_PRO_COMPUTE%' THEN 'SQL'
     WHEN u.sku_name LIKE '%SERVERLESS_SQL_COMPUTE%' THEN 'SQL_SERVERLESS'
     WHEN u.sku_name LIKE '%INFERENCE%' THEN 'MODEL_INFERENCE'
     ELSE 'OTHER'
  END AS sku,
  sum(u.usage_quantity) as DBU,
  sum(u.usage_quantity * lp.pricing.default) as dolares
 FROM system.billing.usage u
   LEFT JOIN system.billing.list_prices lp ON u.cloud = lp.cloud AND
     u.sku_name = lp.sku_name AND
     u.usage_start_time >= lp.price_start_time AND
     (u.usage_end_time <= lp.price_end_time or lp.price_end_time is null)
WHERE
 month(u.usage_date) = month(CURRENT_DATE)
GROUP BY sku;
```

**04. ¿Cómo se compara el consumo entre el mes actual y el mes anterior?**
```sql
SELECT
    after.sku_name,
    before.before_dbus,
    after.after_dbus,
    ((after.after_dbus - before.before_dbus) / before.before_dbus * 100) AS growth_rate
FROM
    (
        SELECT
            sku_name,
            SUM(usage_quantity) AS before_dbus
        FROM
            system.billing.usage
        WHERE
            month(usage_date) = month(CURRENT_DATE) - 1
        GROUP BY
            sku_name
    ) AS before
JOIN
    (
        SELECT
            sku_name,
            SUM(usage_quantity) AS after_dbus
        FROM
            system.billing.usage
        WHERE
            month(usage_date) = month(CURRENT_DATE)
        GROUP BY
            sku_name
    ) AS after
ON
    before.sku_name = after.sku_name
ORDER BY
    growth_rate DESC;
```

**05. ¿Cuántos usuarios activos mensuales hay?**

```sql
SELECT 
    year_month,
    active_users,
    LAG(active_users, 1) OVER (ORDER BY year_month) AS active_users_previous_month,
    CASE
        WHEN LAG(active_users, 1) OVER (ORDER BY year_month) IS NULL THEN 0
        ELSE active_users - LAG(active_users, 1) OVER (ORDER BY year_month)
    END AS growth,
    ROUND(
        (
            (active_users - LAG(active_users, 1) OVER (ORDER BY year_month)) * 100
        ) / LAG(active_users, 1) OVER (ORDER BY year_month), 
        2
    ) AS perc_grow
FROM (
    SELECT 
        DATE_FORMAT(event_time, 'MM/yy') AS year_month,
        COUNT(DISTINCT user_identity.email) AS active_users
    FROM 
        system.access.audit
    WHERE 
        YEAR(event_time) = YEAR(CURRENT_DATE())
        AND action_name IS NOT NULL
    GROUP BY 
        year_month
    ORDER BY 
        year_month
) AS subquery;
```

**06. ¿Qué clusters consumen más?**

```sql
WITH clusters AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY cluster_id ORDER BY change_time DESC) AS rn
    FROM
        system.compute.clusters c
)
SELECT
    c.cluster_name,
    SUM(u.usage_quantity) AS `DBUs Consumed`,
    SUM(u.usage_quantity * lp.pricing.default) AS `Total Dollar Cost`
FROM
    system.billing.usage u
    LEFT JOIN system.billing.list_prices lp ON u.cloud = lp.cloud
        AND u.sku_name = lp.sku_name
        AND u.usage_start_time >= lp.price_start_time
        AND (u.usage_end_time <= lp.price_end_time OR lp.price_end_time IS NULL)
    INNER JOIN clusters c ON u.usage_metadata.cluster_id = c.cluster_id
WHERE
    rn = 1
GROUP BY
    c.cluster_name
ORDER BY
    `Total Dollar Cost` DESC;
```

**07. ¿Quiénes son los propietarios de los clusters interactivos?**

```sql
select
  cluster_id,
  owned_by as usuario
from
  system.compute.clusters
group by all;
```


**08. ¿A qué clústeres interactivos están accediendo los usuarios?**

```sql
WITH user_cluster AS (
  SELECT DISTINCT 
    a.user_identity.email AS email,
    element_at(a.request_params, 'cluster_id') AS cluster_id
  FROM 
    system.access.audit AS a
)
SELECT DISTINCT
  c.cluster_source AS cluster_source,
  a.cluster_id AS cluster_id,
  c.cluster_name AS cluster_name,
  a.email AS email
FROM 
  user_cluster AS a
JOIN 
  system.compute.clusters AS c 
ON 
  a.cluster_id = c.cluster_id
WHERE 
  a.cluster_id IS NOT NULL
  AND c.cluster_source NOT IN ('JOB')
ORDER BY 
  cluster_source,
  cluster_id,
  cluster_name
;
```

**09. ¿Qué usuarios tienen el mayor tiempo de ejecución dentro de un clúster interactivo?**

```sql
SELECT
  user_identity.email AS email,
  request_params.commandLanguage AS command_language,
  SUM(request_params.executionTime) / 3600 AS minutes
FROM
  system.access.audit
WHERE
  action_name = 'runCommand'
  AND request_params.status NOT IN ('skipped')
GROUP BY
  user_identity.email,
  request_params.commandLanguage
ORDER BY
  minutes DESC
LIMIT
  10;
```

**10. ¿Cuáles son los comandos que consumen más tiempo dentro de los clústeres interactivos?**

```sql
SELECT
  a.event_date,
  a.user_identity.email,
  a.request_params.notebookId,
  a.request_params.clusterId,
  a.request_params.status,
  a.request_params.executionTime AS seconds,
  a.request_params.executionTime / 60 AS minutes,
  a.request_params.executionTime / 60 / 60 AS hour,
  a.request_params.commandLanguage,
  a.request_params.commandId,
  a.request_params.commandText
FROM
  system.access.audit AS a
WHERE
  1 = 1
  AND a.action_name = 'runCommand'
  AND a.request_params.status NOT IN ('skipped')
  AND TIMESTAMPDIFF(HOUR, a.event_date, CURRENT_TIMESTAMP()) < 24 * 90
ORDER BY
  seconds DESC;
```

**11. ¿Cuál es el consumo diario de Serverless Notebook?**

```sql
SELECT
  u.usage_date,
  u.sku_name,
  u.billing_origin_product,
  u.usage_quantity,
  u.usage_type,
  u.usage_metadata,
  u.custom_tags,
  u.product_features
FROM system.billing.usage u
WHERE u.sku_name LIKE '%SERVERLESS%'
  AND u.product_features.is_serverless
  AND u.billing_origin_product IN ('NOTEBOOKS', 'INTERACTIVE')
ORDER BY u.usage_date DESC;
```

**12. ¿Cuánto consumió cada usuario en Serverless Notebook en los últimos 30 días?**

```sql
SELECT
  u.usage_metadata.notebook_id,
  u.identity_metadata.run_as,
  SUM(u.usage_quantity) AS total_dbu
FROM
  system.billing.usage AS u
WHERE
  u.billing_origin_product IN ('NOTEBOOKS', 'INTERACTIVE')
  AND u.product_features.is_serverless
  AND u.usage_unit = 'DBU'
  AND u.usage_date >= DATEADD(day, -30, current_date)
GROUP BY
  ALL
ORDER BY
  total_dbu DESC;
```

**13. ¿Qué trabajos del flujo de trabajo de Databricks consumen más tiempo durante el período?**

```sql
SELECT 
    u.usage_metadata.job_id AS job_id,
    get_json_object(a.request_params.new_settings, '$.name') AS job_name,
    SUM(u.usage_quantity) AS dbus_consumed,
    SUM(u.usage_quantity * lp.pricing.default) AS cost
FROM 
    system.billing.usage AS u
LEFT JOIN 
    system.billing.list_prices AS lp 
    ON u.cloud = lp.cloud 
    AND u.sku_name = lp.sku_name 
    AND u.usage_start_time >= lp.price_start_time 
    AND (u.usage_end_time <= lp.price_end_time OR lp.price_end_time IS NULL)
LEFT JOIN 
    system.access.audit AS a 
    ON u.usage_metadata.job_id = a.request_params.job_id
WHERE 
    u.usage_metadata.job_id IS NOT NULL
    AND a.request_params.new_settings IS NOT NULL
    AND a.service_name = 'jobs'
GROUP BY 
    u.usage_metadata.job_id,
    a.request_params.new_settings
ORDER BY 
    cost DESC;
```

**14. ¿Cuál es el consumo diario de Job Serverless (últimos 60 días)?**

```sql
SELECT
    us.usage_date,
    SUM(us.usage_quantity) AS dbus,
    dbus * ANY_VALUE(lp.pricing.`default`) AS cost_at_list_price
FROM
    system.billing.usage AS us
    LEFT JOIN system.billing.list_prices AS lp 
        ON lp.sku_name = us.sku_name 
        AND lp.price_end_time IS NULL
WHERE
    us.usage_date >= DATE_SUB(current_date(), 60)
    AND us.sku_name LIKE "%JOBS_SERVERLESS%"
GROUP BY
    ALL;
```

**15. ¿Cuanto cuesta cada proyecto? (verificando por etiqueta "Proyecto")**

```sql
SELECT u.custom_tags.project as `Project`,
      sum(u.usage_quantity) as `DBUs Consumed`,
      sum(u.usage_quantity * lp.pricing.default) as `Total Dollar Cost`
 FROM system.billing.usage u
     LEFT JOIN system.billing.list_prices lp ON u.cloud = lp.cloud AND
       u.sku_name = lp.sku_name AND
       u.usage_start_time >= lp.price_start_time AND
       (u.usage_end_time <= lp.price_end_time or lp.price_end_time is null)
GROUP BY ALL;
```

**16. ¿Cuántos tokens se utilizan en las interacciones con los modelos de base?**

```sql
SELECT
    u.ingestion_date AS data,
    u.usage_quantity AS dbus_consumido,
    u.usage_quantity * lp.pricing.default AS custo
FROM
    system.billing.usage u
LEFT JOIN
    system.billing.list_prices lp 
    ON u.cloud = lp.cloud
    AND u.sku_name = lp.sku_name
    AND u.usage_start_time >= lp.price_start_time
    AND (u.usage_end_time <= lp.price_end_time OR lp.price_end_time IS NULL)
WHERE
    u.usage_type = 'TOKEN';
```

**17. Quais são as tabelas mais populares (mais acessada)?**

```sql
SELECT
  access_table,
  COUNT(access_table) AS qtde_acesso
FROM (
  SELECT
    IFNULL(request_params.full_name_arg, 'Non-specific') AS access_table
  FROM
    system.access.audit
  WHERE
    action_name = 'getTable'
)
WHERE
  access_table NOT LIKE '__databricks%'
GROUP BY
  ALL
ORDER BY
  qtde_acesso DESC;
```

**18. ¿Quién más accede a estas tablas populares?**

```sql
SELECT 
  user_identity.email AS user_email,
  COUNT(*) AS qnt_acessos
FROM 
  system.access.audit
WHERE 
  request_params.table_full_name = '< poner el nombre de la tabla >'
GROUP BY 
  user_identity.email
ORDER BY 
  qnt_acessos DESC;
```

**19. Quais foram as tabelas acessadas por um usuário nas últimas 24 horas?**

```sql
SELECT 
  a.action_name AS `EVENTO`,
  a.event_time AS `CUANDO`,
  IFNULL(a.request_params.full_name_arg, 'Non-specific') AS `TABLA`,
  IFNULL(a.request_params.commandText, 'GET table') AS `QUERY_TEXT`
FROM 
  system.access.audit AS a
WHERE 
  a.action_name IN (
    'createTable', 
    'commandSubmit', 
    'getTable', 
    'deleteTable', 
    'generateTemporaryTableCredential'
  )
  AND datediff(now(), a.event_date) < 1
ORDER BY 
  a.event_date DESC;
```

**20. ¿Cuál es el linaje de una tabla determinada?**

```sql
SELECT DISTINCT 
    target_table_full_name 
FROM 
    system.access.table_lineage 
WHERE 
    source_table_full_name = '< poner el nombre de la tabla >';
```

**21. ¿Quais as entidades que leem de uma determinada tabela?**

```sql
SELECT DISTINCT 
    tl.entity_type AS entity_type,
    tl.entity_id AS entity_id,
    tl.source_table_full_name AS source_table_full_name
FROM 
    system.access.table_lineage AS tl
WHERE 
    tl.source_table_full_name = '< poner el nombre de la tabla >';
```

**22. ¿Qué tablas tienen comentarios vacíos?**

```sql
SELECT
    t.table_catalog AS table_catalog
    , t.table_schema AS table_schema
    , t.table_name AS table_name
    , t.table_catalog || '.' || t.table_schema || '.' || t.table_name AS table_unique_name
    , t.table_owner AS table_owner
    , t.table_type AS table_type
    , t.last_altered_by AS table_last_altered_by
    , t.last_altered AS table_last_altered_at
    , t.data_source_format AS table_data_source_format
    , t.`comment` AS table_comment
    , CASE 
        WHEN t.`comment` IS NULL THEN true 
        ELSE false 
      END AS table_empty_comment
FROM
    system.information_schema.tables t
WHERE
    t.table_catalog != 'system'
    AND t.table_catalog != '__databricks_internal'
    AND t.table_schema != 'information_schema'
    AND t.`comment` IS NULL;
```

**23. ¿Qué columnas tienen comentarios vacíos?**

```sql
SELECT 
  c.table_catalog AS table_catalog,
  c.table_schema AS table_schema,
  c.table_name AS table_name,
  c.column_name AS column_name,
  c.table_catalog || '.' || c.table_schema || '.' || c.table_name || '.' || c.column_name AS column_unique_name,
  c.is_nullable AS column_is_nullable,
  c.full_data_type AS column_full_data_type,
  c.`comment` AS columns_comment
FROM 
  system.information_schema.columns AS c
WHERE 
  TRUE
  AND c.table_catalog != 'system'
  AND c.table_catalog != '__databricks_internal'
  AND c.table_schema != 'information_schema'
  AND c.`comment` IS NULL;
```

**24. ¿Cuáles son las tablas menos populares (a las que no se ha accedido en los últimos 90 días)?**

```sql
WITH used_tables AS (
  SELECT
    source_table_catalog,
    source_table_schema,
    source_table_name,
    COUNT(DISTINCT created_by) AS downstream_users,
    COUNT(*) AS downstream_dependents
  FROM
    system.access.table_lineage
  WHERE
    source_table_full_name IS NOT NULL
    AND event_time >= date_add(now(), -90)
  GROUP BY
    ALL
)

SELECT
  t.table_catalog,
  t.table_schema,
  t.table_name,
  t.table_type,
  t.table_owner,
  t.comment AS table_comment,
  t.created AS table_created_at,
  t.created_by AS table_created_by,
  t.last_altered AS table_last_update_at,
  t.last_altered_by AS table_last_altered_by
FROM
  system.information_schema.tables AS t
LEFT JOIN
  used_tables AS ut ON ut.source_table_catalog = t.table_catalog
  AND ut.source_table_schema = t.table_schema
  AND ut.source_table_name = t.table_name
WHERE
  ut.downstream_dependents IS NULL
  AND t.table_catalog != 'system'
  AND t.table_catalog != '__databricks_internal'
  AND t.table_schema != 'information_schema';
```

**25. ¿Qué comando SQL se ejecutó en una tabla determinada?**

```sql
SELECT
    l.source_table_full_name,
    l.entity_type,
    q.statement_text,
    q.executed_by,
    q.end_time
FROM system.access.table_lineage AS l
JOIN system.query.history AS q
    ON l.entity_run_id = q.statement_id
WHERE l.source_table_full_name = '< poner el nombre de la tabla >';
```

Si te gustó comparte tu opinión.





