import logging
import os
import boto3
import datetime as dt
import sys
import pytz
from io import StringIO
import csv
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from delta.tables import DeltaTable
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, DateType, TimestampType, FloatType

logging.basicConfig(format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger("ingest_bigmagic")
logger.setLevel(os.environ.get("LOGGING", logging.INFO))

args = getResolvedOptions(
    sys.argv,
    [
        "S3_PATH_STG",
        "S3_PATH_ANALYTICS",
        "S3_PATH_EXTERNAL",
        "S3_PATH_ARTIFACTS",
        "S3_PATH_ARTIFACTS_CSV",
        "S3_PATH_ARTIFACTS_CONFIG",
        "TEAM",
        "REGION_NAME",
        "INSTANCIAS",
        "DYNAMODB_LOGS_TABLE",
        "ERROR_TOPIC_ARN",
        "PROJECT_NAME",
        "FLOW_NAME",
        "PROCESS_NAME",
        "ENVIRONMENT",
    ],
)

S3_PATH_STG = args["S3_PATH_STG"]
S3_PATH_ANALYTICS = args["S3_PATH_ANALYTICS"]
S3_PATH_EXTERNAL = args["S3_PATH_EXTERNAL"]
S3_PATH_ARTIFACTS = args["S3_PATH_ARTIFACTS"]
S3_PATH_ARTIFACTS_CSV = args["S3_PATH_ARTIFACTS_CSV"]
S3_PATH_ARTIFACTS_CONFIG = args["S3_PATH_ARTIFACTS_CONFIG"]
TEAM = args["TEAM"]
REGION_NAME = args["REGION_NAME"]
INSTANCIAS = args["INSTANCIAS"]

DYNAMODB_LOGS_TABLE = args["DYNAMODB_LOGS_TABLE"]
ERROR_TOPIC_ARN = args["ERROR_TOPIC_ARN"]
PROJECT_NAME = args["PROJECT_NAME"]
FLOW_NAME = args["FLOW_NAME"]
PROCESS_NAME = args["PROCESS_NAME"]
ENVIRONMENT = args["ENVIRONMENT"]

TZ_LIMA = pytz.timezone("America/Lima")
NOW_LIMA = dt.datetime.now(pytz.utc).astimezone(TZ_LIMA)

logger.info(f"project name: {PROJECT_NAME} | flow name:  {FLOW_NAME} | process name: {PROCESS_NAME}")
logger.info(f"instance: {INSTANCIAS}")

DOMAIN_LAYER = "domain"
ANALYTICS_LAYER = "analytics"
STAGE_LAYER_BIGMAGIC = "bigmagic"
STAGE_LAYER_SALESFORCE = "salesforce"

INSTANCES = [x.upper() for x in INSTANCIAS.split(",")]

s3 = boto3.client("s3")
sns_client = boto3.client("sns")


class data_paths:
    DOMAIN = f"{S3_PATH_ANALYTICS}{DOMAIN_LAYER}/"
    BIGMAGIC = f"{S3_PATH_STG}{STAGE_LAYER_BIGMAGIC}/"
    SALESFORCE = f"{S3_PATH_STG}{STAGE_LAYER_SALESFORCE}/public/"
    EXTERNAL = f"{S3_PATH_EXTERNAL}"
    ARTIFACTS_CSV = f"{S3_PATH_ARTIFACTS_CSV}/"

    def getDataPath(self, layer):
        if layer.upper() == "DOMAIN":
            return self.DOMAIN
        else:
            raise ValueError(f"Layer {layer} not found")


class STATUS:
    IN_PROGRESS = 0
    LANDING_SUCCEEDED = 1
    RAW_SUCCEEDED = 2
    STAGE_SUCCEEDED = 3
    LANDING_FAILED = 4
    RAW_FAILED = 5
    STAGE_FAILED = 6
    WARNING = 2


class SPARK_CONTROLLER:
    def __init__(self) -> None:
        self.spark = (
            SparkSession.builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
            .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
            .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
            .config("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")
            .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
            .getOrCreate()
        )

        self.logger = LOGGING_UTILS()

    def _load_csv_from_s3(self, s3_path):
        """Load CSV file from S3 and return as list of dictionaries"""
        s3_client = boto3.client("s3")
        bucket = s3_path.split("/")[2]
        key = "/".join(s3_path.split("/")[3:])

        response = s3_client.get_object(Bucket=bucket, Key=key)
        content = response["Body"].read().decode("latin-1")

        csv_data = []
        reader = csv.DictReader(StringIO(content), delimiter=";")
        for row in reader:
            csv_data.append(row)

        return csv_data

    def get_cod_pais_list_from_credentials(self):
        # Get cod_pais from schema_path = f"{S3_PATH_ARTIFACTS_CONFIG}{TEAM}/{STAGE_LAYER_BIGMAGIC}/configuration/csv/credentials.csv"
        schema_path = f"{S3_PATH_ARTIFACTS_CONFIG}{TEAM}/{STAGE_LAYER_BIGMAGIC}/configuration/csv/credentials.csv"
        logger.info(f"Obteniendo lista de paises desde: {schema_path}")
        credentials_data = self._load_csv_from_s3(schema_path)
        countries_set = set()
        for item in credentials_data:
            if item.get("INSTANCE", "ZZZZ").upper() in INSTANCES and item.get("ENV", "ZZZZ").upper() == ENVIRONMENT.upper():
                # Separate by ','
                countries = item["COUNTRIES"].split(",")
                for country in countries:
                    # add country to set
                    countries_set.add(country.strip())
        # transform set to list
        return list(countries_set)

    def _create_empty_dataframe_from_csv(self, team, source, table_name):
        """
        Crea un DataFrame vacío usando el esquema definido en un archivo CSV en S3.
        """
        try:
            schema_path = f"{S3_PATH_ARTIFACTS_CONFIG}{team}/{source}/configuration/csv/columns.csv"
            logger.info(f"Cargando esquema desde: {schema_path}")

            columns_data = self._load_csv_from_s3(schema_path)

            filtered_columns = [col for col in columns_data if col.get("TABLE_NAME", "").upper() == table_name.upper()]

            if not filtered_columns:
                raise ValueError(f"No se encontraron columnas para la tabla: {table_name}")

            # Ordenar por COLUMN_ID si existe
            if "COLUMN_ID" in filtered_columns[0]:
                filtered_columns.sort(key=lambda x: int(x.get("COLUMN_ID", 0)))

            # Mapeo de tipos
            type_mapping = {"string": StringType(), "int": IntegerType(), "integer": IntegerType(), "double": DoubleType(), "float": FloatType(), "boolean": BooleanType(), "date": DateType(), "timestamp": TimestampType()}

            fields = []
            for col_spec in filtered_columns:
                column_name = col_spec["COLUMN_NAME"]
                data_type = col_spec.get("NEW_DATA_TYPE", "string").lower()
                spark_type = type_mapping.get(data_type, StringType())
                fields.append(StructField(column_name, spark_type, True))

            schema = StructType(fields)
            logger.info(f"Schema de tabla {table_name}: {[f.name + ':' + f.dataType.simpleString() for f in fields]}")
            return self.spark.createDataFrame([], schema)

        except Exception as e:
            logger.error(f"Error al construir DataFrame vacío desde CSV para {table_name}: {str(e)}")
            raise

    def get_now_lima_datetime(self):
        return NOW_LIMA

    def read_table(self, path, table_name, have_principal=False, schema=False):
        try:
            s3_path = f"{path}{table_name}/"
            if path == data_paths.EXTERNAL or path == data_paths.ARTIFACTS_CSV:
                if schema:
                    df = self.spark.read.format("csv").options(delimiter=",", header=False).schema(schema).load(s3_path)
                else:
                    df = self.spark.read.format("csv").option("sep", ";").option("header", "true").load(s3_path)

            elif path == data_paths.BIGMAGIC:
                schema_path = f"{S3_PATH_ARTIFACTS_CONFIG}{TEAM}/{STAGE_LAYER_BIGMAGIC}/configuration/csv/credentials.csv"
                logger.info(f"Cargando esquema desde: {schema_path}")
                credentials_data = self._load_csv_from_s3(schema_path)
                items = []
                for item in credentials_data:
                    if item.get("INSTANCE").upper() in INSTANCES and item.get("ENV").upper() == ENVIRONMENT.upper():
                        if have_principal:
                            if not bool(item.get("IS_PRINCIPAL", False)):
                                continue
                        items.append(item["ENDPOINT_NAME"])
                logger.info(f"Instancias encontradas: {items}")

                df_list = []
                table_exists_somewhere = False

                for carpeta in items:
                    try:
                        carpeta_path = f"{path}{carpeta}/{table_name}/"
                        print(f"Leyendo archivos desde: {carpeta_path}")
                        # Verificar si existe la tabla usando DeltaTable
                        if DeltaTable.isDeltaTable(self.spark, carpeta_path):
                            df_tmp = self.spark.read.format("delta").load(carpeta_path)
                            df_list.append(df_tmp)
                            table_exists_somewhere = True
                    except Exception as e:
                        logger.warning(f"No se pudo leer la tabla {table_name} desde {carpeta}: {str(e)}")
                        continue

                # Si no se encontró ninguna tabla, crear DataFrame vacío con schema de DynamoDB
                if not table_exists_somewhere:
                    logger.warning(f"Tabla {table_name} no existe en ninguna ubicación. Creando DataFrame vacío con schema de DynamoDB.")
                    df = self._create_empty_dataframe_from_csv(TEAM, STAGE_LAYER_BIGMAGIC, table_name)
                else:
                    # Unir todos los DataFrames en uno solo si hay más de una carpeta
                    df = df_list[0] if len(df_list) == 1 else df_list[0].unionByName(*df_list[1:])

            elif path == data_paths.SALESFORCE:
                max_retries = 3
                df = None

                for attempt in range(max_retries):
                    try:
                        if attempt > 0:
                            self.spark.catalog.clearCache()
                            logger.info(f"Retry {attempt} for Salesforce table {table_name}")

                        if DeltaTable.isDeltaTable(self.spark, s3_path):
                            df = self.spark.read.format("delta").load(s3_path)
                            logger.info(f"Successfully read Salesforce table {table_name} on attempt {attempt + 1}")
                            break
                        else:
                            raise Exception(f"Path {s3_path} is not a valid Delta table")

                    except Exception as e:
                        if attempt == max_retries - 1:
                            logger.error(f"Failed to read Salesforce table {table_name} after {max_retries} attempts: {str(e)}")
                            try:
                                df = self._create_empty_dataframe_from_csv(TEAM, "salesforce", table_name)
                                logger.warning(f"Created empty DataFrame for {table_name}")
                            except Exception as csv_error:
                                logger.error(f"Could not create empty DataFrame: {str(csv_error)}")
                                df = self.spark.createDataFrame([], StructType([]))
                            break
                        else:
                            logger.warning(f"Attempt {attempt + 1} failed for {table_name}: {str(e)}")
                            import time

                            time.sleep(2)

            else:
                df = self.spark.read.format("delta").load(s3_path)

            return df
        except Exception as e:
            logger.error(f"Source table cannot be read {table_name}")
            # self.logger.send_error_message(ERROR_TOPIC_ARN,f"Reading table {table_name}", str(e))
            raise e

    def upsert(self, df, path, table_name, id_columns, partition_by: list = []):
        logger.info(f"Upserting table {table_name}")
        if self.table_exists(path, table_name):
            logger.info(f"table exists")
            self.update_table(df, path, table_name, id_columns)
        else:
            logger.info(f"table not exists")
            self.write_table(df, path, table_name, partition_by)

    def update_table(self, update_records, path, table_name, update_columns_ids):
        table_path = f"{path}{table_name}/"

        expression = ""
        for column in update_columns_ids:
            expression += f"a.{column} = b.{column} and "
        expression = expression[:-4]

        deltaTable = DeltaTable.forPath(self.spark, table_path)
        deltaTable.alias("a").merge(source=update_records.alias("b"), condition=expression).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

        deltaTable.vacuum(100)
        deltaTable.generate("symlink_format_manifest")

    def write_table(self, df, path, table_name, partition_by: list = []):
        try:
            s3_path = f"{path}{table_name}/"
            logger.info(f"Writing table {table_name} to {s3_path} with partition by {partition_by}") # Debugging line
            if len(partition_by) == 0:
                df.write.format("delta").mode("overwrite").save(s3_path)
            else:
                df.write.partitionBy(*partition_by).format("delta").mode("overwrite").option("partitionOverwriteMode", "dynamic").save(s3_path)
            deltaTable = DeltaTable.forPath(self.spark, s3_path)
            deltaTable.vacuum(20)
            deltaTable.generate("symlink_format_manifest")
        except Exception as e:
            logger.error(str(e))
            self.logger.send_error_message(ERROR_TOPIC_ARN, f"Writing table {table_name}", str(e))
            raise e

    ###########################################################################################################

    def read_spark_table(self, path, table_name, update_records, update_columns):
        try:
            update_expr = ""
            for column in update_columns:
                update_expr += f"a.{column} = b.{column} and "
            update_expr = update_expr[:-4]
            s3_path = f"{path}{table_name}/"
            if DeltaTable.isDeltaTable(self.spark, s3_path):
                dt = DeltaTable.forPath(self.spark, s3_path)
                dt.alias("a").merge(source=update_records.alias("b"), condition=update_expr).whenMatchedUpdateAll().execute()
        except Exception as e:
            logger.error(e)

    def insert_into_table(self, df, path, table_name, partition_by: list = []):
        try:
            s3_path = f"{path}{table_name}/"
            if len(partition_by) == 0:
                df.write.format("delta").mode("append").save(s3_path)
            else:
                df.write.partitionBy(*partition_by).format("delta").mode("append").save(s3_path)
        except Exception as e:
            logger.error(str(e))
            self.logger.send_error_message(ERROR_TOPIC_ARN, f"Writing table {table_name}", str(e))
            raise e

    def table_exists(self, path, table_name):
        try:
            return DeltaTable.isDeltaTable(self.spark, f"{path}{table_name}/")
        except Exception as e:
            logger.error(e)
            return False

    def get_previous_period(self, date: str = NOW_LIMA.strftime("%Y%m")):
        year = int(date[:4])
        month = int(date[4:])
        if month == 1:
            year -= 1
            month = 12
        else:
            month -= 1
        return f"{year}{month:02d}"

    def get_periods(self, periods : int, date : dt.datetime = NOW_LIMA):
        periods_list = []
        current_period = date.strftime("%Y%m")

        periods_list.append(current_period)
        extra_periods = periods - 1
        for i in range(extra_periods):
            current_period = self.get_previous_period(current_period)
            periods_list.append(current_period)
        
        return periods_list
    
    def get_dates_filter(self, periods):
        dates_filter = []
        dates_magic = []
        periods_filter = self.get_periods(periods)
        #Get the last day of previous month
        for period in periods_filter:
            year = int(period[:4])
            month = int(period[4:])
            #get next period
            if month == 12:
                month_next = 1
                year_next = year + 1
            else:
                month_next = month + 1
                year_next = year
            #get last day of current period
            last_day = (dt.date(year_next, month_next, 1) - dt.timedelta(days=1)).day
            #Get remaining days of current period e.g. last_day = 2023-01-31 then get dates from 2023-01-01 to 2023-01-31
            for i in range(1, last_day + 1):
                date = dt.date(year, month, i)
                dates_filter.append(date)

            #date = dt.date(year, month, last_day)
            #dates_filter.append(date)

        for date_normal in dates_filter:
            #data type int

            #Receive timestamp column: column_name_deltalake, transform into an integer column: column_name_target
            #integer column is a number of days from a pivot date: 2008-01-01 | days 733042
            #pesudo code from integer to date:
            #pivot_date = datetime(2008,1,1)
            #move_date = date_magic - 733042
            #real_date = pivot_date + timedelta(days=move_date)

            #pseudo code from timestamp to integer:
            #pivot_date = datetime(2008,1,1)
            #real_date = date_magic - pivot_date
            #move_date = real_date.days
            #integer_date = move_date + 733042
            
            date_magic = (date_normal - dt.date(2008, 1, 1)).days + 733042
            dates_magic.append(date_magic)

        return dates_filter, dates_magic, periods_filter

    def get_catalog_jdbc_connection(self, catalog_connection):
        # Get the JDBC connection details from AWS Glue Data Catalog
        glue_client = boto3.client("glue", region_name=REGION_NAME)
        try:
            # Fetch the connection details
            response = glue_client.get_connection(Name=catalog_connection)
            connection_properties = response["Connection"]["ConnectionProperties"]

            # Extract the credentials
            username = connection_properties.get("USERNAME")
            password = connection_properties.get("PASSWORD")
            jdbc_url = connection_properties.get("JDBC_CONNECTION_URL")

            # Determine driver based on JDBC URL
            if "redshift" in jdbc_url.lower():
                driver = "com.amazon.redshift.jdbc42.Driver"
            elif "sqlserver" in jdbc_url.lower():
                driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
            elif "postgresql" in jdbc_url.lower():
                driver = "org.postgresql.Driver"
            elif "mysql" in jdbc_url.lower():
                driver = "com.mysql.cj.jdbc.Driver"
            else:
                # Default to Redshift driver for backward compatibility
                driver = "com.amazon.redshift.jdbc42.Driver"

            jdbc_properties = {
                "user": username,
                "password": password,
                "driver": driver,
            }

            return jdbc_url, jdbc_properties
        except Exception as e:
            print(f"Error fetching JDBC connection details: {e}")
            raise

    def get_catalog_connection_redshift(self, catalog_connection):
        """
        Backward compatibility method - delegates to get_catalog_jdbc_connection
        @deprecated Use get_catalog_jdbc_connection instead
        """
        return self.get_catalog_jdbc_connection(catalog_connection)

    def load_to_redshift(self, df, properties, url, redshift_table_name, pais_filter=[], periodos_filter=[]):
        try:
            # Use preactions to delete the data from the table
            preactions = None
            if len(pais_filter) > 0 and len(periodos_filter) > 0:
                if len(pais_filter) == 1:
                    tuple_pais_filter = f"('{pais_filter[0]}')"
                else:
                    tuple_pais_filter = f"{tuple(pais_filter)}"
                if len(periodos_filter) == 1:
                    tuple_periodos_filter = f"('{periodos_filter[0]}')"
                else:
                    tuple_periodos_filter = f"{tuple(periodos_filter)}"
                preactions = f"DELETE FROM {redshift_table_name} WHERE id_pais IN {tuple_pais_filter} AND id_periodo IN {tuple_periodos_filter};"
            elif len(pais_filter) > 0:
                if len(pais_filter) == 1:
                    tuple_pais_filter = f"('{pais_filter[0]}')"
                else:
                    tuple_pais_filter = f"{tuple(pais_filter)}"
                preactions = f"DELETE FROM {redshift_table_name} WHERE id_pais IN {tuple_pais_filter};"
            # Execute the preactions query manually
            if preactions:
                print(f"Validate existence of table {redshift_table_name}")
                try:
                    # Validate existence of table using Spark DataFrame
                    query = f"(SELECT 1 FROM {redshift_table_name} LIMIT 1)"
                    df_redshift = self.spark.read.format("jdbc").options(url=url, user=properties["user"], password=properties["password"], dbtable=query, driver="com.amazon.redshift.jdbc42.Driver").load()

                except Exception as e:
                    logger.error(f"Table {redshift_table_name} does not exist. Error: {str(e)}")
                    preactions = "select 1;"
                # If the table does not exist, set preactions to a dummy query

                print(f"Executing preactions query: {preactions}")
                glue_context = GlueContext(SparkContext.getOrCreate())
                connection_options = {"url": url, "user": properties["user"], "password": properties["password"], "preactions": preactions, "dbtable": redshift_table_name, "redshiftTmpDir": f"{S3_PATH_ARTIFACTS}temp/"}

                # Write the data to the Redshift table using Glue DynamicFrame
                dynamic_frame = DynamicFrame.fromDF(df, glue_context, "dynamic_frame")
                glue_context.write_dynamic_frame.from_options(frame=dynamic_frame, connection_type="redshift", connection_options=connection_options)
        except Exception as e:
            logger.error(f"Failed to write to Redshift: {str(e)}")
            self.logger.send_error_message(ERROR_TOPIC_ARN, f"Failed to write to Redshift", str(e))

    def load_to_redshift_stage(self, df, properties, url, redshift_table_name, compania_filters=[], compania_column="cod_compania", fechas_filter=[], fecha_column="fecha"):
        try:
            # Use preactions to delete the data from the table
            preactions = None
            if len(compania_filters) > 0 and len(fechas_filter) > 0:
                if len(compania_filters) == 1:
                    tuple_compania_filter = f"('{compania_filters[0]}')"
                else:
                    tuple_compania_filter = f"{tuple(compania_filters)}"
                if len(fechas_filter) == 1:
                    tuple_fechas_filter = f"('{fechas_filter[0]}')"
                else:
                    tuple_fechas_filter = f"{tuple(fechas_filter)}"
                preactions = f"DELETE FROM {redshift_table_name} WHERE {compania_column} IN {tuple_compania_filter} AND {fecha_column} IN {tuple_fechas_filter};"
            elif len(compania_filters) > 0:
                if len(compania_filters) == 1:
                    tuple_compania_filter = f"('{compania_filters[0]}')"
                else:
                    tuple_compania_filter = f"{tuple(compania_filters)}"
                preactions = f"DELETE FROM {redshift_table_name} WHERE {compania_column} IN {tuple_compania_filter};"
            else:
                preactions = f"DELETE FROM {redshift_table_name} WHERE 1=1;"
                # preactions = f"select 1;"
            # Execute the preactions query manually
            if preactions:
                print(f"Validate existence of table {redshift_table_name}")
                try:
                    # Validate existence of table using Spark DataFrame
                    query = f"(SELECT 1 FROM {redshift_table_name} LIMIT 1)"
                    df_redshift = self.spark.read.format("jdbc").options(url=url, user=properties["user"], password=properties["password"], dbtable=query, driver="com.amazon.redshift.jdbc42.Driver").load()

                except Exception as e:
                    logger.error(f"Table {redshift_table_name} does not exist. Error: {str(e)}")
                    preactions = "select 1;"
                # If the table does not exist, set preactions to a dummy query

                print(f"Executing preactions query: {preactions}")
                glue_context = GlueContext(SparkContext.getOrCreate())
                connection_options = {"url": url, "user": properties["user"], "password": properties["password"], "preactions": preactions, "dbtable": redshift_table_name, "redshiftTmpDir": f"{S3_PATH_ARTIFACTS}temp/"}

                # Write the data to the Redshift table using Glue DynamicFrame
                dynamic_frame = DynamicFrame.fromDF(df, glue_context, "dynamic_frame")
                glue_context.write_dynamic_frame.from_options(frame=dynamic_frame, connection_type="redshift", connection_options=connection_options)
        except Exception as e:
            logger.error(f"Failed to write to Redshift: {str(e)}")
            self.logger.send_error_message(ERROR_TOPIC_ARN, f"Failed to write to Redshift", str(e))

    def load_to_odscorp_stage(self, df, properties, url, ods_table_name, compania_filters = [],compania_column = 'cod_compania', fechas_filter=[], fecha_column = 'fecha'):
        try:
            #Use preactions to delete the data from the table
            preactions = None
            if len(compania_filters) > 0 and len(fechas_filter) > 0:
                if len(compania_filters) == 1:
                    tuple_compania_filter = f"('{compania_filters[0]}')"
                else:
                    tuple_compania_filter = f"{tuple(compania_filters)}"
                if len(fechas_filter) == 1:
                    tuple_fechas_filter = f"('{fechas_filter[0]}')"
                else:
                    tuple_fechas_filter = f"{tuple(fechas_filter)}"
                preactions = f"""DELETE FROM {ods_table_name} WHERE {compania_column} IN {tuple_compania_filter} AND {fecha_column} IN {tuple_fechas_filter};"""
            elif len(compania_filters) > 0:
                if len(compania_filters) == 1:
                    tuple_compania_filter = f"('{compania_filters[0]}')"
                else:
                    tuple_compania_filter = f"{tuple(compania_filters)}"
                preactions = f"""DELETE FROM {ods_table_name} WHERE {compania_column} IN {tuple_compania_filter};"""
            else:
                preactions = f"""DELETE FROM {ods_table_name} WHERE 1=1;"""
                #preactions = f"select 1;"
            print(f"Prepared precations {preactions} [wont work in this case, just for reference]")
            # Execute the preactions query manually
            if preactions:
                df.write \
                    .format("jdbc") \
                    .option("url", url) \
                    .option("user", properties["user"]) \
                    .option("password", properties["password"]) \
                    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
                    .option("dbtable", ods_table_name) \
                    .mode("append") \
                    .save()
                #Insert into dummy table the model name and the country code
                #ODSDWAJECORP.dbo.dummy_incremental_update_masters
                target_table_name = ods_table_name.split(".")[-1]
                #remove temp_ from target_table_name if exists
                if target_table_name.startswith("temp_"):
                    target_table_name = target_table_name[5:]
                #remove _{country_code} from target_table_name if exists
                cod_pais = self.get_cod_pais_list_from_credentials()
                if f"_{cod_pais}".lower() in target_table_name:
                    target_table_name = target_table_name.replace(f"_{cod_pais}".lower(), "")
                country_code_list = cod_pais.split(",")
                for country_code in country_code_list:
                    dummy_table_name = f"ODSDWAJECORP.dbo.dummy_incremental_update_masters"
                    if len(compania_filters) > 0 and len(fechas_filter) > 0:
                        df_dummy = self.spark.createDataFrame([(target_table_name, country_code,fecha_column,compania_column)], ["model_name", "country_code","fecha_name","compania_name"])
                    elif len(compania_filters) > 0:
                        df_dummy = self.spark.createDataFrame([(target_table_name, country_code,compania_column)], ["model_name", "country_code","compania_name"])
                    else:
                        df_dummy = self.spark.createDataFrame([(target_table_name, country_code)], ["model_name", "country_code"])
                    df_dummy.printSchema()
                    df_dummy.write \
                        .format("jdbc") \
                        .option("url", url) \
                        .option("user", properties["user"]) \
                        .option("password", properties["password"]) \
                        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
                        .option("dbtable", dummy_table_name) \
                        .mode("append") \
                        .save()
                    print(f"Inserted into dummy table {dummy_table_name} the model name {target_table_name} and country code {country_code}")
                    print(f"This will only work for master tables with compania")
                
                print("Datos cargados correctamente.")
        except Exception as e:
            logger.error(f"Failed to write to Redshift: {str(e)}")
            self.logger.send_error_message(ERROR_TOPIC_ARN,f"Failed to write to Redshift", str(e))   

class LOGGING_UTILS:
    def send_error_redshift_message(self, topic_arn, table_name, error):
        message = f"Failed table: {table_name} \nStep: load to redshift \nLog ERROR \n{error}"

        sns_client.publish(TopicArn=topic_arn, Message=message)

    def send_error_message(self, topic_arn, table_name, error):

        if "no data detected to migrate" in error:
            message = f"RAW WARNING in table: {table_name} \n{error}"
        else:
            message = f"Failed table: {table_name} \nStep: stage job \nLog ERROR \n{error}"
        sns_client.publish(TopicArn=topic_arn, Message=message)

    def update_attribute_value_dynamodb(self, row_key_field_name, row_key, attribute_name, attribute_value, table_name):
        logger.info("update dynamoDb Metadata : {} ,{},{},{},{}".format(row_key_field_name, row_key, attribute_name, attribute_value, table_name))
        dynamo_table = dynamodb_resource.Table(table_name)
        dynamo_table.update_item(Key={row_key_field_name: row_key}, AttributeUpdates={attribute_name: {"Value": attribute_value, "Action": "PUT"}})

    def update_status_dynamo(self, config_table_name, table_name, status: STATUS, message: str = ""):

        if status == STATUS.IN_PROGRESS:
            status_stage = "IN_PROGRESS"
            status_raw = "IN_PROGRESS"
            status_landing = "IN_PROGRESS"
            self.update_attribute_value_dynamodb("TARGET_TABLE_NAME", table_name, "STATUS_STAGE", status_stage, config_table_name)
            self.update_attribute_value_dynamodb("TARGET_TABLE_NAME", table_name, "STATUS_RAW", status_raw, config_table_name)
            self.update_attribute_value_dynamodb("TARGET_TABLE_NAME", table_name, "STATUS_LANDING", status_landing, config_table_name)

        elif status == STATUS.LANDING_SUCCEEDED:
            status_landing = "SUCCEEDED"
            self.update_attribute_value_dynamodb("TARGET_TABLE_NAME", table_name, "STATUS_LANDING", status_landing, config_table_name)

        elif status == STATUS.RAW_SUCCEEDED:
            status_raw = "SUCCEEDED"
            self.update_attribute_value_dynamodb("TARGET_TABLE_NAME", table_name, "STATUS_RAW", status_raw, config_table_name)

        elif status == STATUS.STAGE_SUCCEEDED:
            status_stage = "SUCCEEDED"
            self.update_attribute_value_dynamodb("TARGET_TABLE_NAME", table_name, "STATUS_STAGE", status_stage, config_table_name)

        elif status == STATUS.LANDING_FAILED:
            status_stage = "FAILED"
            status_raw = "FAILED"
            status_landing = "FAILED"
            self.update_attribute_value_dynamodb("TARGET_TABLE_NAME", table_name, "STATUS_STAGE", status_stage, config_table_name)
            self.update_attribute_value_dynamodb("TARGET_TABLE_NAME", table_name, "STATUS_RAW", status_raw, config_table_name)
            self.update_attribute_value_dynamodb("TARGET_TABLE_NAME", table_name, "STATUS_LANDING", status_landing, config_table_name)

        elif status == STATUS.RAW_FAILED:
            status_stage = "FAILED"
            status_raw = "FAILED"
            self.update_attribute_value_dynamodb("TARGET_TABLE_NAME", table_name, "STATUS_STAGE", status_stage, config_table_name)
            self.update_attribute_value_dynamodb("TARGET_TABLE_NAME", table_name, "STATUS_RAW", status_raw, config_table_name)

        elif status == STATUS.STAGE_FAILED:
            status_stage = "FAILED"
            self.update_attribute_value_dynamodb("TARGET_TABLE_NAME", table_name, "STATUS_STAGE", status_stage, config_table_name)

        elif status == STATUS.WARNING:
            status_raw = "WARNING"
            status_landing = "WARNING"
            status_stage = "WARNING"

        self.update_attribute_value_dynamodb("TARGET_TABLE_NAME", table_name, "FAIL REASON", message, config_table_name)

    def upload_log(log_table_name: str, target_table_name: str, type: str, message: str, load_type: str, date_time="", service: str = "GLUE", service_specification: str = "", pipeline_id: str = ""):
        dynamodb = boto3.resource("dynamodb")
        log = {
            "TARGET_TABLE_NAME": target_table_name.upper(),
            "DATETIME": date_time,
            "PROJECT_NAME": os.getenv("PROJECT_NAME"),
            "LAYER": "RAW",
            "TYPE": type,
            "LOAD_TYPE": load_type,
            "MESSAGE": message,
            "SERVICE": service,
            "SERVICE_SPECIFICATION": service_specification,
            "PIPELINE_ID": f"{pipeline_id}_{date_time[:-5]}",
        }
        logger.info("uploading log")
        dynamo_table = dynamodb.Table(log_table_name)
        response = dynamo_table.put_item(Item=log)



