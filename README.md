Universidad San Francisco de Quito
Data Mining
Proyecto 02
John Ochoa Abad 345743

#Descripción del proyecto:
En el presente proyecto pude llevar a cabo una ingesta masiva de datos (aprox. 800 millones de filas) correspondientes a
los viajes de taxis de New York de tipo Amarillo y Verde desde el año 2015 hasta Julio del presente año 2025. Dichos datos
fueron extraidos de la página web https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page. La ingesta se llevo a cabo mediante
jobs (programados en Python) desde Mage de Data Loading consumiendo los parquets correspondientes a viajes de taxis amarillos, 
viajes de taxis verdes y zonas de taxis de New York. Posterior a dicha carga de datos los mismos se fueron exportando directamente hacia
un warehouse en Snowflake empleando conexión con credenciales seguras manejadas desde secrets de Mage (datos de bbdd, datos de usuario, datos de warehouse, clave privada RSA)
sobre un perfil con grants mínimos (únicamente permisos de creación y select) para cargar datos raw + metadatos a tablas SQL en Snowflake respetando principios
de idempotencia, manejo de errores y recarga de datos (checkpoint) ante posibles fallos. Una vez completada la carga de datos en Snowflake se siguió 
con la transformación de los mismos (esquema ELT) mediante el uso de DBT desde Mage para obtención de datos refinados en tabla silver en primer lugar
generando unificación de datos de viajes, enriquecimiento, limpieza y estandarización de los datos. Posteriormente, se promovio a un esquema gold generando una 
tabla de hechos (viajes de taxis) y tablas de dimensiones (zonas, fechas, horarios, tipo de pago, tipo de servicio, etc). Todo esto se llevo a cabo con DBT desde Mage
por lo que se empleo directamente SQL para las transformaciones y las mismas fueron validadas con tests de valores y relaciones igual con DBT. Luego se valido la necesidad
de clusterización llevado a cabo mediciones de Query Profiles sobre la tabla de hechos sin clusterización y con clusterización basada en clustering keys de fecha + zonas +
servicio al corresponder las mismas a los principales tipos de consultas. Por último, sobre Gold se efectuaron queries desde un Jupyter Notebook para responder a preguntas
de negocio sobre los viajes de taxis ya que al final del día lo importante es sacar valor de los datos.


#Checklist de aceptación (copiar al README y marcar)
[x] Cargados todos los meses 2015–2025 (Parquet) de Yellow y Green; matriz de cobertura en README. NYC.gov
[x] Mage orquesta backfill mensual con idempotencia y metadatos por lote.
[x] Bronze (raw) refleja fielmente el origen; Silver unifica/escaliza; Gold en estrella con fct_trips y dimensiones clave.
[x] Clustering aplicado a fct_trips con evidencia antes/después (Query Profile, pruning). Snowflake Docs
[x] Secrets y cuenta de servicio con permisos mínimos (evidencias sin exponer valores).
[x] Tests dbt (not_null, unique, accepted_values, relationships) pasan; docs y lineage generados.
[x] Notebook con respuestas a las 5 preguntas de negocio desde gold.

#Layout para .env a manera de ejecutar conexión con Snowflake desde Jupyter Notebook
Para la ejecución de las queries desde un Jupyter Notebook a manera de obtener datos de respuesta en formato de Pandas Data Frames para responder
las preguntas de negocio se empleo un .env donde se colocaron las credenciales para la conexión con Snowflake. Para replicar dicha conexión usando crendenciales
propias el layout que se debe emplear en el .env es:

SNOWFLAKE_USER=[valor]
SNOWFLAKE_PRIVATE_KEY=[valor]
SNOWFLAKE_ACCOUNT=[valor]
SNOWFLAKE_WAREHOUSE=[valor]
SNOWFLAKE_DATABASE=[valor]
SNOWFLAKE_SCHEMA=[valor]

Para mi caso a manera de otorgar mayor seguridad estoy generando la conexión con llave privada (RSA) la cual debe ser colocada en una sola línea con caracteres de escape \n para
saltos de línea de la clave. De igual forma, se puede emplear conexión únicamente con el password del usuario a continuación pero se necesitaría ajustar el método de conexión del 
Jupyter Notebook sustituyendo private_key por password en los argumentos de la conexión. Esto se manejo de esta manera para en el docker compose únicamente tener el servicio de Mage
sin requerir de un servicio adicional de Jupyter Notebook


#Descripción y diagrama de arquitectura (bronze/silver/gold) y orquestación en Mage.

*Descripción

1. Loaders+Exporters for Taxis Data: ingestión en Mage que descarga de forma incremental y controlada los archivos Parquet de viajes de taxis amarillos de Nueva York 
(2015–2025) desde la fuente pública, los procesa por lotes para no saturar memoria, enriquece los datos con metadatos de ejecución (run_id, ventana temporal, lote, mes, año), 
elimina inconsistencias, y finalmente los carga en una tabla en Snowflake con su esquema y restricciones de llave primaria. Además, utiliza un checkpoint en JSON para retomar 
la carga en caso de interrupción, garantizando idempotencia y reanudación segura.
2. Loader for Taxis Zones: data loader en Mage que descarga el archivo público taxi_zone_lookup.csv desde el repositorio oficial de datos de taxis de Nueva York, lo lee directamente 
en un DataFrame de Pandas, imprime información de confirmación (columnas y estado de descarga) y retorna el DataFrame listo para usarse en las siguientes etapas del pipeline.
En caso de error durante la descarga o lectura, captura la excepción y muestra un mensaje de fallo.
3. Exporter for Taxis Zones: data exporter en Mage que toma un DataFrame (en este caso, el de zonas de taxis de Nueva York) y lo carga en Snowflake. Para la conexión, utiliza credenciales 
seguras almacenadas en Mage Secrets (ACCOUNT_ID, SNOWFLAKE_USER_TRANSFORMER, RSA_PRIVATE, etc.), establece la conexión con snowflake.connector, y luego usa write_pandas para escribir
el DataFrame en la tabla NEWYORK_TAXIS_ZONES.
4. Bronze DBT: Este bloque combina Mage + dbt para orquestar la transformación de datos en la capa Bronze. Primero, la función execute_dbt ejecuta los comandos dbt deps y dbt run 
sobre el proyecto NY_TAXI_DBT, configurando las credenciales de Snowflake a partir de secretos de Mage, de modo que se instalan dependencias y se corren los modelos etiquetados 
como bronze. Luego, los modelos SQL de dbt normalizan y estandarizan los datasets: crean tablas en el esquema BRONZE a partir de las fuentes raw, generando una tabla para taxis verdes
 y otra para amarillos (con sus columnas renombradas y un campo service_type que indica el tipo de servicio), y una tabla de zonas de taxis que homogeniza columnas (LocationID, Borough, 
 Zone, service_zone). En conjunto, esto asegura que los datos crudos de ingestión pasen a una capa Bronze estandarizada e idempotente, lista para transformaciones posteriores en Silver y Gold.
 5. Silver DBT: Se construye es la capa Silver del pipeline en dbt, orquestada desde Mage. El modelo SQL toma los viajes de taxis verdes y amarillos ya depurados en Bronze, los une 
 y estandariza en una única tabla de viajes, enriquecida con la información de zonas (pickup y dropoff), conversiones de fechas a UTC y NYC, cálculo de duración, velocidad promedio 
 y categorización de rate_code y payment_type. Además, se generan flags de calidad (duration_flag, speed_flag) y se filtran registros inválidos mediante reglas estrictas (pasajeros > 0,
 montos ≥ 0, tiempos consistentes, etc.). Luego, en el archivo schema.yml, se definen tests automáticos para validar unicidad, no-nulos y expresiones de negocio en las columnas críticas 
 (por ejemplo, fare_amount >= 0, dropoff_datetime_utc > pickup_datetime_utc). Finalmente, el transformer de Mage ejecuta dbt deps, dbt run --select tag:silver y dbt test --select tag:silver,
 asegurando que la capa Silver se genere con integridad y control de calidad antes de pasar a Gold.
6. Gold DBT: se construye la capa Gold del pipeline de NYC Taxi. Aquí, los datos ya estandarizados y validados en Silver se integran con las dimensiones del modelo (fecha, hora, zonas de pickup 
y dropoff, proveedor, tipo de pago, código de tarifa, tipo de servicio y flags de viaje) para crear la tabla de hechos principal (fact_trips), con métricas calculadas como duración, velocidad promedio,
 distancia, montos y porcentaje de propina. Además, se generan las tablas de dimensiones auxiliares (dim_date_pickup, dim_date_dropoff, dim_time_pickup, dim_time_dropoff, dim_zone_pickup, dim_zone_dropoff,
 dim_vendor, dim_rate_code, dim_payment_type, dim_service_type, dim_trip_type) con claves surrogate (SK) y atributos descriptivos, permitiendo relaciones 1:N consistentes para análisis OLAP. El transformer
 de Mage ejecuta dbt deps, dbt run --select tag:gold y dbt test --select tag:gold, asegurando que todas las tablas Gold se generen correctamente y que los tests de calidad (unicidad de claves, integridad referencial,
 valores válidos en métricas críticas) se cumplan antes de disponibilizar la capa para análisis.

*Orquestación en Mage (ver evidencias para pipeline real)

 Loaders - Taxi Trips
        │
        ▼
Loader - Taxi Zones
        │
        ▼
Exporter - Taxi Zones
        │
        ▼
    DBT Bronze
        │
        ▼
    DBT Silver
        │
        ▼
    DBT Gold

#Cobertura de meses 2015–2025 (matriz por servicio) y estado de carga (Parquet).

Se verificó la cobertura completa de datos de taxis amarillos y verdes desde 2015 hasta 2025. Todos los archivos Parquet fueron correctamente ingestados y consolidados en las tablas BRONZE correspondientes 
(RAW_NEWYORK_TAXIS_YELLOW_BRONZE y RAW_NEWYORK_TAXIS_GREEN_BRONZE). La cantidad de registros por mes y por tipo de taxi se contabilizó para confirmar la consistencia de la carga y la integridad de los datos.

| AÑO  | MES   | YELLOW_COUNT | GREEN_COUNT | ESTADO_LOAD |
| ---- | ----- | ------------ | ----------- | ----------- |
| 2015 | 1     | 12741035     | 1508493     | OK          |
| 2015 | 2     | 12442394     | 1574830     | OK          |
| 2015 | 3     | 13342951     | 1722574     | OK          |
| 2015 | 4     | 13063758     | 1664394     | OK          |
| 2015 | 5     | 13157677     | 1786848     | OK          |
| 2015 | 6     | 12324936     | 1638868     | OK          |
| 2015 | 7     | 11559666     | 1541671     | OK          |
| 2015 | 8     | 11123123     | 1532343     | OK          |
| 2015 | 9     | 11218122     | 1494927     | OK          |
| 2015 | 10    | 12307333     | 1630536     | OK          |
| 2015 | 11    | 11305240     | 1529984     | OK          |
| 2015 | 12    | 11452996     | 1608297     | OK          |
| 2016 | 1     | 10905067     | 1445292     | OK          |
| 2016 | 2     | 11375412     | 1510722     | OK          |
| 2016 | 3     | 12203824     | 1576393     | OK          |
| 2016 | 4     | 11927996     | 1543926     | OK          |
| 2016 | 5     | 11832049     | 1536979     | OK          |
| 2016 | 6     | 11131645     | 1404727     | OK          |
| 2016 | 7     | 10294080     | 1332510     | OK          |
| 2016 | 8     | 9942263      | 1247675     | OK          |
| 2016 | 9     | 10116018     | 1162373     | OK          |
| 2016 | 10    | 10854626     | 1252572     | OK          |
| 2016 | 11    | 10102128     | 1148214     | OK          |
| 2016 | 12    | 10446697     | 1224158     | OK          |
| 2017 | 1     | 9710820      | 1069565     | OK          |
| 2017 | 2     | 9169775      | 1022313     | OK          |
| 2017 | 3     | 10295441     | 1157827     | OK          |
| 2017 | 4     | 10047135     | 1080844     | OK          |
| 2017 | 5     | 10102127     | 1059463     | OK          |
| 2017 | 6     | 9656993      | 976467      | OK          |
| 2017 | 7     | 8588486      | 914783      | OK          |
| 2017 | 8     | 8422153      | 867407      | OK          |
| 2017 | 9     | 8945421      | 882464      | OK          |
| 2017 | 10    | 9768672      | 925737      | OK          |
| 2017 | 11    | 9284803      | 874173      | OK          |
| 2017 | 12    | 9508501      | 906016      | OK          |
| 2018 | 1     | 8760687      | 792744      | OK          |
| 2018 | 2     | 8492819      | 769197      | OK          |
| 2018 | 3     | 9431289      | 836246      | OK          |
| 2018 | 4     | 9306216      | 799383      | OK          |
| 2018 | 5     | 9224788      | 796552      | OK          |
| 2018 | 6     | 8714667      | 738546      | OK          |
| 2018 | 7     | 7851143      | 684374      | OK          |
| 2018 | 8     | 7855040      | 675815      | OK          |
| 2018 | 9     | 8049094      | 682032      | OK          |
| 2018 | 10    | 8834520      | 731888      | OK          |
| 2018 | 11    | 8155449      | 673287      | OK          |
| 2018 | 12    | 8195675      | 719654      | OK          |
| 2019 | 1     | 7696617      | 672105      | OK          |
| 2019 | 2     | 7049370      | 615594      | OK          |
| 2019 | 3     | 7866620      | 643063      | OK          |
| 2019 | 4     | 7475949      | 567852      | OK          |
| 2019 | 5     | 7598445      | 545452      | OK          |
| 2019 | 6     | 6971560      | 506238      | OK          |
| 2019 | 7     | 6310419      | 470743      | OK          |
| 2019 | 8     | 6073357      | 449695      | OK          |
| 2019 | 9     | 6567788      | 449063      | OK          |
| 2019 | 10    | 7213891      | 476386      | OK          |
| 2019 | 11    | 6878111      | 449500      | OK          |
| 2019 | 12    | 6896317      | 455294      | OK          |
| 2020 | 1     | 6405008      | 447770      | OK          |
| 2020 | 2     | 6299367      | 398632      | OK          |
| 2020 | 3     | 3007687      | 223496      | OK          |
| 2020 | 4     | 238073       | 35644       | OK          |
| 2020 | 5     | 348415       | 57361       | OK          |
| 2020 | 6     | 549797       | 63110       | OK          |
| 2020 | 7     | 800412       | 72258       | OK          |
| 2020 | 8     | 1007286      | 81063       | OK          |
| 2020 | 9     | 1341017      | 87987       | OK          |
| 2020 | 10    | 1681132      | 95120       | OK          |
| 2020 | 11    | 1509000      | 88605       | OK          |
| 2020 | 12    | 1461898      | 83130       | OK          |
| 2021 | 1     | 1369769      | 76518       | OK          |
| 2021 | 2     | 1371709      | 64572       | OK          |
| 2021 | 3     | 1925152      | 83827       | OK          |
| 2021 | 4     | 2171187      | 86941       | OK          |
| 2021 | 5     | 2507109      | 88180       | OK          |
| 2021 | 6     | 2834264      | 86737       | OK          |
| 2021 | 7     | 2821746      | 83691       | OK          |
| 2021 | 8     | 2788757      | 83499       | OK          |
| 2021 | 9     | 2963793      | 95709       | OK          |
| 2021 | 10    | 3463504      | 110891      | OK          |
| 2021 | 11    | 3472949      | 108229      | OK          |
| 2021 | 12    | 3214369      | 99961       | OK          |
| 2022 | 1     | 2463931      | 62495       | OK          |
| 2022 | 2     | 2979431      | 69399       | OK          |
| 2022 | 3     | 3627882      | 78537       | OK          |
| 2022 | 4     | 3599920      | 76136       | OK          |
| 2022 | 5     | 3588295      | 76891       | OK          |
| 2022 | 6     | 3558124      | 73718       | OK          |
| 2022 | 7     | 3174394      | 64192       | OK          |
| 2022 | 8     | 3152677      | 65929       | OK          |
| 2022 | 9     | 3183767      | 69031       | OK          |
| 2022 | 10    | 3675411      | 69322       | OK          |
| 2022 | 11    | 3252717      | 62313       | OK          |
| 2022 | 12    | 3399549      | 72439       | OK          |
| 2023 | 1     | 3066766      | 68211       | OK          |
| 2023 | 2     | 2913955      | 64809       | OK          |
| 2023 | 3     | 3403766      | 72044       | OK          |
| 2023 | 4     | 3288250      | 65392       | OK          |
| 2023 | 5     | 3513649      | 69174       | OK          |
| 2023 | 6     | 3307234      | 65550       | OK          |
| 2023 | 7     | 2907108      | 61343       | OK          |
| 2023 | 8     | 2824209      | 60649       | OK          |
| 2023 | 9     | 2846722      | 65471       | OK          |
| 2023 | 10    | 3522285      | 66177       | OK          |
| 2023 | 11    | 3339715      | 64025       | OK          |
| 2023 | 12    | 3376567      | 64215       | OK          |
| 2024 | 1     | 2964624      | 56551       | OK          |
| 2024 | 2     | 3007526      | 53577       | OK          |
| 2024 | 3     | 3582628      | 57457       | OK          |
| 2024 | 4     | 3514289      | 56471       | OK          |
| 2024 | 5     | 3723833      | 61003       | OK          |
| 2024 | 6     | 3539193      | 54748       | OK          |
| 2024 | 7     | 3076903      | 51837       | OK          |
| 2024 | 8     | 2979183      | 51771       | OK          |
| 2024 | 9     | 3633030      | 54440       | OK          |
| 2024 | 10    | 3833771      | 56147       | OK          |
| 2024 | 11    | 3646369      | 52222       | OK          |
| 2024 | 12    | 3668371      | 53994       | OK          |
| 2025 | 1     | 3475226      | 48326       | OK          |
| 2025 | 2     | 3577543      | 46621       | OK          |
| 2025 | 3     | 4145257      | 51539       | OK          |
| 2025 | 4     | 3970553      | 52132       | OK          |
| 2025 | 5     | 4591845      | 55399       | OK          |
| 2025 | 6     | 4322960      | 49390       | OK          |
| 2025 | 7     | 3898963      | 48205       | OK          |


#Estrategia de pipeline de backfill mensual e idempotencia

La estrategia de pipeline implementada sigue un enfoque de backfill mensual, lo que permite procesar de manera incremental los datos históricos de viajes de taxis de Nueva York sin reejecutar todo el conjunto de
datos desde cero. Cada mes se generan los lotes correspondientes con metadatos de ejecución (run_id, ventana_temporal, lote_mes, year), lo que asegura que los datos cargados en Snowflake sean consistentes y trazables, 
facilitando auditoría y reproducibilidad.

Además, el pipeline está diseñado para ser idempotente: los loaders y exporters manejan checkpoints y verificaciones (por ejemplo, control de archivos Parquet descargados y registros existentes en Snowflake), de modo 
que si una ejecución falla, puede reanudarse sin duplicar datos ni alterar la integridad de la tabla. Esta combinación de backfill controlado y ejecución segura garantiza que tanto los datos históricos como los nuevos
 se procesen de manera confiable, eficiente y repetible. Al igual se incluyen primary keys en nuestra tabla inicial de carga de datos de modo que los datos no se dupliquen a la hora de ingresarse
 
#Gestión de secretos (nombres y propósito) y cuenta de servicio / rol (permisos mínimos).

En el pipeline se implementa una gestión centralizada de secretos a través de Mage AI, asegurando que credenciales sensibles no estén hardcodeadas en el código y puedan rotarse de forma segura. Los secretos utilizados son:

ACCOUNT_ID: Identificador de la cuenta de Snowflake.

SNOWFLAKE_USER_TRANSFORMER: Usuario de Snowflake con permisos limitados para ejecutar transformaciones y cargas de datos.

RSA_PRIVATE: Clave privada RSA utilizada para la autenticación segura con Snowflake.

WAREHOUSE: Nombre del warehouse de Snowflake para ejecutar consultas.

DATABASE: Base de datos destino en Snowflake.

SCHEMA: Esquema de Snowflake donde se crean tablas de Bronze, Silver y Gold.

ROLE_LOW_PRIVILEDGES: Rol con permisos mínimos necesarios para realizar inserciones y lecturas, evitando privilegios excesivos.

La cuenta de servicio y rol asociado se configuran siguiendo el principio de mínimos privilegios: la cuenta puede ejecutar cargas, transformaciones y tests en dbt sobre las tablas necesarias, pero no tiene permisos de administración 
ni acceso a datos sensibles fuera del scope del pipeline. Esto asegura seguridad, auditoría y control sobre el acceso a los datos.

Para la ejecución de los proceso ELT se genero desde el usuario principal de Snowflake un usuario con privilegios mínimos y un rol asignado al usuario al cual se le asignaron los siguientes
privilegios únicos:

* GRANT USAGE ON DATABASE mi_base TO ROLE mi_rol;
* GRANT USAGE ON SCHEMA mi_base.mi_esquema TO ROLE mi_rol;
* GRANT SELECT ON ALL TABLES IN SCHEMA mi_base.mi_esquema TO ROLE mi_rol;
* GRANT SELECT ON FUTURE TABLES IN SCHEMA mi_base.mi_esquema TO ROLE mi_rol;
* GRANT CREATE TABLE, CREATE VIEW ON SCHEMA mi_base.mi_esquema TO ROLE mi_rol;

Posteriormente, se registraron las credenciales de dicho usuario en el .env local y en los secrets de Mage (ver evidencias) para que los procesos los manejemos
únicamente con el usuario de privilegios mínimos lo cual es lo más seguro

#Diseño de silver (reglas de limpieza/estandarización) y gold (hechos/dimensiones).

*Diseño de Silver:
En la capa Silver se unifican los datasets de taxis verdes y amarillos previamente cargados en Bronze. Se aplican reglas de limpieza estrictas, como eliminar viajes con pasajeros ≤ 0, distancias negativas, montos inválidos o 
tiempos inconsistentes (pickup posterior a dropoff). Además, se estandarizan formatos de fechas y horas, convirtiendo timestamps a UTC y hora local de Nueva York, y se calculan métricas derivadas como duración del viaje y 
velocidad promedio. Se enriquecen los datos con información de zonas de pickup y dropoff, y se generan flags de calidad (duration_flag, speed_flag) para clasificar viajes atípicos. Finalmente, se implementan tests de integridad
 y consistencia mediante dbt, garantizando unicidad de identificadores, no-nulos en columnas críticas y cumplimiento de reglas de negocio.

*Diseño de Gold:
En la capa Gold se construye la tabla de hechos principal (fact_trips) integrando los viajes estandarizados de Silver con las dimensiones asociadas: fecha de pickup y dropoff, tiempo de pickup y dropoff, zonas de origen y 
destino, proveedor, tipo de tarifa, tipo de pago, tipo de servicio y flags de viaje. Se calculan métricas clave como duración, velocidad promedio, distancias recorridas, montos y porcentaje de propina. Cada dimensión se modela
 con claves surrogate (SK) y atributos descriptivos, asegurando relaciones 1:N consistentes para análisis OLAP. Esta capa permite consultas analíticas y reportes precisos, mientras que dbt se encarga de ejecutar los modelos y 
 validar la integridad referencial y consistencia de datos antes de disponibilizar la información para downstream.

#Clustering: llaves elegidas, métricas antes/después, conclusion.

Para el tema del clustering se ejecutaron 6 pruebas para validar las ventajas de clusterizar la tabla de hechos y seleccionar las mejores llaves para los propósitos de optimizar las consultas. A continuación se describen pruebas junto a métricas
Cabe destacar que todos los resultados de las presentes pruebas se encuentran en formato de capturas en la carpeta de evidencias:

1. Select basado unicamente en fechas de pickup: Sin clustering para la presente query se escanearon 2312 particiones todas las existentes. La consulta tomo 1.5s y se escanearon 1.54GB. Posterior a la clusterización teniendo como llave principal al
pickup_date_sk la consulta mejoro en tiempo a 947ms, solo se tuvieron que escanear 9 particiones y 1.91MB lo cual optimizo de sobremanera la presente consulta.

2. Select basado unicamente en zona de pickup: Sin clustering para la presente query se escanearon 2312 particiones todas las existentes. La consulta tomo 1.6s y se escanearon 1.12GB. Posterior a la clusterización teniendo como llave principal al
pickup_date_sk, seguida por pickup_zone_sk la consulta mejoro en tiempo a 1.3s, solo se tuvieron que escanear 1951 particiones y 910.13MB lo cual optimizo de sobremanera la presente consulta.

3. Select basado en fechas de pickup + pickup_zone: Sin clustering para la presente query se escanearon 2312 particiones todas las existentes. La consulta tomo 1.3s y se escanearon 2.19GB. Posterior a la clusterización teniendo como llave principal al
pickup_date_sk + pickup_zone la consulta mejoro en tiempo a 416ms, solo se tuvieron que escanear 3 particiones y 1.3MB lo cual optimizo de sobremanera la presente consulta.

4. Select basado en fechas de pickup + pickup_zone 2: Sin clustering para la presente query se escanearon 2312 particiones todas las existentes. La consulta tomo 866ms y se escanearon 2.18GB. Posterior a la clusterización teniendo como llave principal al
pickup_date_sk + pickup_zone la consulta mejoro en tiempo a 503ms, solo se tuvieron que escanear 92 particiones y 52.43MB lo cual optimizo de sobremanera la presente consulta.

5. Select basado en fechas de dropoff: Para verificar la diferencia de clusterizar con fecha de dropoff y de pickup se valido que al usar clusterizacion basada en pickup una consulta de fechas dropoff se demoro 953ms, se escanearon 119 parciones y 81.16MB.
Por otro lado, al hacer la misma consulta pero con clusterizacion basada en dropoff_date la consulta mejoro a 416ms, 116 particiones y 55.45MB. La cual es una mejora a nivel de tiempo pero no de particiones escaneadas

6. Select basado en fechas de pickup, zonas de pickup y tipo de servicio: Sin clustering para la presente query se escanearon 2312 particiones todas las existentes. La consulta tomo 7s y se escanearon 38.08GB. Posterior a la clusterización teniendo como llave principal al
pickup_date_sk + pickup_zone_sk + service_type_sk la consulta mejoro en tiempo a 1.8ms, solo se tuvieron que escanear 18 particiones y 245.75MB lo cual optimizo de sobremanera la presente consulta.

Tras ejecutar las seis pruebas de rendimiento, se evidenció que la clusterización impacta significativamente la eficiencia de las consultas, reduciendo tanto el tiempo de ejecución como el volumen de datos escaneados. Las pruebas mostraron que clusterizar únicamente 
por pickup_date_sk mejora de manera notable las consultas basadas en fechas de pickup, mientras que incluir pickup_zone_sk optimiza consultas que filtran por zona. Adicionalmente, la combinación de pickup_date_sk + pickup_zone_sk + service_type_sk ofrece mejoras consistentes
en consultas más complejas que involucran fechas, zonas y tipo de servicio, reduciendo drásticamente tanto el número de particiones escaneadas como el tamaño de datos leídos.

#Pruebas (qué validan y cómo interpretar resultados).

*Validación de unicidad de trip_id: asegura que cada viaje tenga un identificador único.

*Chequeo de no-nulos en columnas críticas: garantiza que campos como pickup_datetime, dropoff_datetime, fare_amount y passenger_count no contengan valores vacíos.

*Reglas de negocio: expresiones que confirman condiciones como trip_distance >= 0, fare_amount >= 0, y dropoff_datetime_utc > pickup_datetime_utc.

*Integridad referencial: validación de claves foráneas hacia dimensiones (vendor_sk, zone_sk, payment_type_sk, etc.).

*Interpretación de resultados: tests exitosos indican que los datos cumplen las reglas definidas; fallos apuntan a inconsistencias, datos faltantes o errores de transformación que deben revisarse antes de usar la capa Silver o Gold.

#Troubleshooting (archivos faltantes, fallas de carga, límites, costos).

*Archivos Parquet faltantes en la fuente pública: revisar URL de descarga y permisos de acceso.

*Fallas de carga en Snowflake: validar credenciales, roles y esquema de destino.

*Errores durante ejecución de dbt: verificar logs de dbt run y dbt test, y resolver dependencias faltantes (dbt deps).

*Límites de memoria o CPU durante procesamiento batch: ajustar tamaño de lotes en loaders y exportadores.

*Costos de almacenamiento o computación en Snowflake: revisar tamaño de tablas y uso de warehouse durante transformaciones masivas.

*Problemas de idempotencia: asegurar checkpoint JSON actualizado y consistencia de run_id para reanudar cargas interrumpidas.

*Datos inconsistentes o nulos en columnas críticas: aplicar reglas de limpieza en Silver antes de pasar a Gold.



