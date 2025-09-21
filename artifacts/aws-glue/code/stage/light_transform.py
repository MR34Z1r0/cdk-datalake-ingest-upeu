import datetime as dt
import logging
import os
import sys
import time
import json
import csv
from io import StringIO
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from abc import ABC, abstractmethod
import re

import boto3
import pytz
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from delta.tables import DeltaTable
from dateutil.relativedelta import relativedelta
from py4j.protocol import Py4JJavaError
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql import Window
from pyspark.sql.types import *
from pyspark.sql.session import SparkSession

# Configuración de logging
logging.basicConfig(format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger("LightTransform")
logger.setLevel(os.environ.get("LOGGING", logging.INFO))

# Constantes
TZ_LIMA = pytz.timezone('America/Lima')
BASE_DATE_MAGIC = "1900-01-01"
MAGIC_OFFSET = 693596

@dataclass
class ColumnMetadata:
    """Estructura de datos para metadatos de columna"""
    name: str
    column_id: int
    data_type: str
    transformation: str
    is_partition: bool = False
    is_id: bool = False
    is_order_by: bool = False
    is_filter_date: bool = False

@dataclass
class TableConfig:
    """Configuración de tabla"""
    stage_table_name: str
    source_table: str
    source_table_type: str
    load_type: str
    num_days: Optional[str] = None
    delay_incremental_ini: str = "-2"

@dataclass
class EndpointConfig:
    """Configuración de endpoint"""
    endpoint_name: str
    environment: str
    src_db_name: str
    src_server_name: str
    src_db_username: str

class TransformationException(Exception):
    """Excepción específica para errores de transformación"""
    def __init__(self, column_name: str, message: str):
        self.column_name = column_name
        self.message = message
        super().__init__(f"Error en columna {column_name}: {message}")

class DataValidationException(Exception):
    """Excepción para errores de validación de datos"""
    pass

class ConfigurationManager:
    """Maneja la carga y validación de configuraciones desde S3"""
    
    def __init__(self, s3_client):
        self.s3_client = s3_client
    
    def load_csv_from_s3(self, s3_path: str) -> List[Dict[str, str]]:
        """Carga archivo CSV desde S3 con validación"""
        try:
            bucket = s3_path.split('/')[2]
            key = '/'.join(s3_path.split('/')[3:])
            
            response = self.s3_client.get_object(Bucket=bucket, Key=key)
            content = response['Body'].read().decode('utf-8')  # Cambio a UTF-8
            
            csv_data = []
            reader = csv.DictReader(StringIO(content), delimiter=';')
            for row in reader:
                # Sanitizar datos
                sanitized_row = self._sanitize_csv_row(row)
                csv_data.append(sanitized_row)
            
            return csv_data
        except Exception as e:
            raise DataValidationException(f"Error cargando CSV desde {s3_path}: {str(e)}")
    
    def _sanitize_csv_row(self, row: Dict[str, str]) -> Dict[str, str]:
        """Sanitiza una fila de CSV removiendo comillas"""
        sanitized = {}
        for key, value in row.items():
            if isinstance(value, str):
                # Remover comillas triples y dobles
                clean_value = value.replace('"""', '"')
                if clean_value.startswith('"') and clean_value.endswith('"'):
                    clean_value = clean_value[1:-1]
                sanitized[key] = clean_value
            else:
                sanitized[key] = value
        return sanitized

class ExpressionParser:
    """Parser robusto para expresiones de transformación"""
    
    def __init__(self):
        self.function_pattern = re.compile(r'(\w+)\((.*)\)$')
    
    def parse_transformation(self, expression: str) -> List[Tuple[str, str]]:
        """
        Parsea expresión de transformación y retorna lista de (función, parámetros)
        """
        if not expression or expression.strip() == '':
            return []
        
        functions_with_params = []
        remaining = expression.strip()
        
        while remaining:
            match = self.function_pattern.match(remaining)
            if not match:
                # No es una función, es una columna simple
                return [('simple_column', remaining)]
            
            function_name = match.group(1)
            params_str = match.group(2)
            
            # Extraer parámetros considerando paréntesis anidados
            params = self._extract_parameters(params_str)
            functions_with_params.append((function_name, params))
            
            # Para funciones anidadas, buscar la siguiente
            # Por ahora, asumimos una función por expresión
            break
        
        return functions_with_params
    
    def _extract_parameters(self, params_str: str) -> str:
        """Extrae parámetros de una función manejando paréntesis anidados"""
        if not params_str:
            return ""
        
        # Para simplicidad, retornamos el string completo
        # En una implementación completa, habría que parsear paréntesis anidados
        return params_str.strip()

class TransformationEngine:
    """Motor de transformaciones optimizado"""
    
    def __init__(self, spark_session):
        self.spark = spark_session
        self.parser = ExpressionParser()
    
    def apply_transformations(self, df, columns_metadata: List[ColumnMetadata]) -> Tuple[Any, List[str]]:
        """
        Aplica todas las transformaciones de manera optimizada
        Retorna (DataFrame transformado, lista de errores)
        """
        errors = []
        transformation_exprs = []
        
        # Ordenar columnas por column_id
        sorted_columns = sorted(columns_metadata, key=lambda x: x.column_id)
        
        for column_meta in sorted_columns:
            try:
                expr = self._build_transformation_expression(column_meta)
                if expr:
                    transformation_exprs.append(expr.alias(column_meta.name))
                else:
                    # Columna simple sin transformación
                    transformation_exprs.append(col(column_meta.transformation).alias(column_meta.name))
            except Exception as e:
                error_msg = f"Error en columna {column_meta.name}: {str(e)}"
                errors.append(error_msg)
                logger.error(error_msg)
                # Agregar columna con valor null en caso de error
                transformation_exprs.append(lit(None).cast(column_meta.data_type).alias(column_meta.name))
        
        # Aplicar todas las transformaciones en una sola operación
        if transformation_exprs:
            transformed_df = df.select(*transformation_exprs)
        else:
            transformed_df = df
        
        return transformed_df, errors
    
    def _build_transformation_expression(self, column_meta: ColumnMetadata):
        """Construye expresión de transformación para una columna"""
        functions_with_params = self.parser.parse_transformation(column_meta.transformation)
        
        if not functions_with_params:
            return None
        
        if len(functions_with_params) == 1 and functions_with_params[0][0] == 'simple_column':
            return None  # Columna simple
        
        # Por ahora, manejar solo una función por columna
        function_name, params = functions_with_params[0]
        return self._create_transformation_expr(function_name, params, column_meta.data_type)
    
    def _create_transformation_expr(self, function_name: str, params: str, data_type: str):
        """Crea expresión de transformación para función específica"""
        param_list = self._split_parameters(params) if params else []
        
        if function_name == 'fn_transform_Concatenate':
            columns_to_concat = [col(p.strip()) for p in param_list]
            return concat_ws("|", *[trim(c) for c in columns_to_concat])
        
        elif function_name == 'fn_transform_Concatenate_ws':
            if len(param_list) < 2:
                raise TransformationException("fn_transform_Concatenate_ws", "Requiere al menos 2 parámetros")
            separator = param_list[-1]
            columns_to_concat = [col(p.strip()) for p in param_list[:-1]]
            return concat_ws(separator, *[trim(c) for c in columns_to_concat])
        
        elif function_name == 'fn_transform_Integer':
            if not param_list:
                raise TransformationException("fn_transform_Integer", "Requiere nombre de columna")
            origin_column = param_list[0]
            return when(
                col(origin_column).isNull() | 
                (col(origin_column).cast(StringType()) == "") |
                (col(origin_column).cast(StringType()).rlike("^\\s*$")),
                lit(None)
            ).otherwise(
                col(origin_column).cast(IntegerType())
            )
        
        elif function_name == 'fn_transform_Numeric':
            if not param_list:
                raise TransformationException("fn_transform_Numeric", "Requiere nombre de columna")
            origin_column = param_list[0]
            
            # Determinar tipo decimal
            decimal_type = self._parse_decimal_type(data_type)
            
            return when(
                col(origin_column).isNull() | 
                (col(origin_column).cast(StringType()) == "") |
                (col(origin_column).cast(StringType()).rlike("^\\s*$")) |
                (col(origin_column).cast(StringType()).rlike("^[Nn][Aa][Nn]$")) |
                (col(origin_column).cast(StringType()).rlike("^[Ii][Nn][Ff]$")),
                lit(None)
            ).otherwise(
                col(origin_column).cast(decimal_type)
            )
        
        elif function_name == 'fn_transform_ClearString':
            origin_column = param_list[0] if param_list else None
            if not origin_column:
                raise TransformationException("fn_transform_ClearString", "Requiere nombre de columna")
            
            if len(param_list) > 1:
                default = param_list[1]
                default_expr = lit(default.replace('$', '')) if '$' in default else col(default)
                return when(col(origin_column).isNotNull(), trim(col(origin_column))).otherwise(default_expr)
            else:
                return trim(col(origin_column))
        
        elif function_name == 'fn_transform_DateMagic':
            if len(param_list) < 3:
                raise TransformationException("fn_transform_DateMagic", "Requiere 3 parámetros")
            
            origin_column = param_list[0]
            date_format_param = param_list[1]
            value_default = param_list[2]
            
            date_pattern = r'^([7-9]\d{5}|[1-2]\d{6}|3[0-5]\d{5})$'
            
            return when(
                regexp_extract(col(origin_column).cast(StringType()), date_pattern, 1) != "",
                to_date(date_add(to_date(lit(BASE_DATE_MAGIC)), 
                       col(origin_column).cast(IntegerType()) - lit(MAGIC_OFFSET)), date_format_param)
            ).otherwise(
                to_date(lit(value_default), date_format_param)
            )
        
        elif function_name == 'fn_transform_Datetime':
            origin_column = param_list[0] if param_list else ''
            if origin_column == '':
                return from_utc_timestamp(current_timestamp(), "America/Lima")
            else:
                return to_timestamp(col(origin_column))
        
        else:
            raise TransformationException(function_name, f"Función no soportada: {function_name}")
    
    def _parse_decimal_type(self, data_type: str) -> DecimalType:
        """Parsea tipo decimal desde string"""
        if isinstance(data_type, str) and "numeric" in data_type.lower():
            match = re.search(r'numeric\((\d+),(\d+)\)', data_type.lower())
            if match:
                precision = int(match.group(1))
                scale = int(match.group(2))
                return DecimalType(precision, scale)
        return DecimalType(38, 12)  # Default
    
    def _split_parameters(self, params_str: str) -> List[str]:
        """Divide parámetros considerando paréntesis anidados"""
        if not params_str:
            return []
        
        params = []
        current_param = ""
        paren_count = 0
        
        for char in params_str + ',':  # Agregar coma al final para procesar último parámetro
            if char == ',' and paren_count == 0:
                if current_param.strip():
                    params.append(current_param.strip())
                current_param = ""
            else:
                if char == '(':
                    paren_count += 1
                elif char == ')':
                    paren_count -= 1
                current_param += char
        
        return params

class DeltaTableManager:
    """Maneja operaciones con tablas Delta"""
    
    def __init__(self, spark_session):
        self.spark = spark_session
    
    def write_delta_table(self, df, s3_path: str, partition_columns: List[str], 
                         mode: str = "overwrite") -> None:
        """Escribe DataFrame a tabla Delta con optimizaciones"""
        writer = df.write.format("delta").mode(mode)
        
        if partition_columns:
            writer = writer.partitionBy(*partition_columns)
        
        # Optimizaciones
        writer = writer.option("delta.autoOptimize.optimizeWrite", "true")
        writer = writer.option("delta.autoOptimize.autoCompact", "true")
        
        writer.save(s3_path)
    
    def merge_delta_table(self, df, s3_path: str, merge_condition: str) -> None:
        """Realiza merge en tabla Delta"""
        delta_table = DeltaTable.forPath(self.spark, s3_path)
        
        delta_table.alias("old").merge(
            df.alias("new"), 
            merge_condition
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    
    def optimize_delta_table(self, s3_path: str) -> None:
        """Optimiza tabla Delta"""
        delta_table = DeltaTable.forPath(self.spark, s3_path)
        delta_table.vacuum(100)
        delta_table.generate("symlink_format_manifest")

class DataProcessor:
    """Procesador principal de datos optimizado"""
    
    def __init__(self, spark_session, config_manager, transformation_engine, delta_manager):
        self.spark = spark_session
        self.config_manager = config_manager
        self.transformation_engine = transformation_engine
        self.delta_manager = delta_manager
        self.now_lima = dt.datetime.now(pytz.utc).astimezone(TZ_LIMA)
    
    def process_table(self, args: Dict[str, str]) -> None:
        """Procesa una tabla completa"""
        try:
            # Cargar configuraciones
            table_config, endpoint_config, columns_metadata = self._load_configurations(args)
            
            # Construir rutas S3
            s3_paths = self._build_s3_paths(args, table_config)
            
            # Leer datos source
            source_df = self._read_source_data(s3_paths['raw'])
            
            if source_df.count() == 0:
                self._handle_empty_data(s3_paths['stage'], columns_metadata)
                return
            
            # Aplicar transformaciones
            transformed_df, transformation_errors = self.transformation_engine.apply_transformations(
                source_df, columns_metadata
            )
            
            # Aplicar post-procesamiento (deduplicación, ordenamiento)
            final_df = self._apply_post_processing(transformed_df, columns_metadata)
            
            # Escribir a stage
            self._write_to_stage(final_df, s3_paths['stage'], table_config, columns_metadata)
            
            # Optimizar tabla Delta
            self.delta_manager.optimize_delta_table(s3_paths['stage'])
            
            # Log de éxito
            self._log_success(args, table_config, endpoint_config, transformation_errors)
            
        except Exception as e:
            self._log_error(args, str(e))
            raise
    
    def _load_configurations(self, args: Dict[str, str]) -> Tuple[TableConfig, EndpointConfig, List[ColumnMetadata]]:
        """Carga todas las configuraciones necesarias"""
        # Cargar datos CSV
        tables_data = self.config_manager.load_csv_from_s3(args['TABLES_CSV_S3'])
        credentials_data = self.config_manager.load_csv_from_s3(args['CREDENTIALS_CSV_S3'])
        columns_data = self.config_manager.load_csv_from_s3(args['COLUMNS_CSV_S3'])
        
        # Encontrar configuración de tabla
        table_config = self._find_table_config(tables_data, args['TABLE_NAME'])
        
        # Encontrar configuración de endpoint
        endpoint_config = self._find_endpoint_config(credentials_data, args['ENDPOINT_NAME'], args['ENVIRONMENT'])
        
        # Procesar metadatos de columnas
        columns_metadata = self._process_columns_metadata(columns_data, args['TABLE_NAME'])
        
        return table_config, endpoint_config, columns_metadata
    
    def _find_table_config(self, tables_data: List[Dict], table_name: str) -> TableConfig:
        """Encuentra configuración de tabla"""
        for row in tables_data:
            if row.get('STAGE_TABLE_NAME', '').upper() == table_name.upper():
                return TableConfig(
                    stage_table_name=row.get('STAGE_TABLE_NAME', ''),
                    source_table=row.get('SOURCE_TABLE', ''),
                    source_table_type=row.get('SOURCE_TABLE_TYPE', 'm'),
                    load_type=row.get('LOAD_TYPE', ''),
                    num_days=row.get('NUM_DAYS'),
                    delay_incremental_ini=row.get('DELAY_INCREMENTAL_INI', '-2')
                )
        raise DataValidationException(f"Configuración de tabla no encontrada: {table_name}")
    
    def _find_endpoint_config(self, credentials_data: List[Dict], endpoint_name: str, environment: str) -> EndpointConfig:
        """Encuentra configuración de endpoint"""
        for row in credentials_data:
            if (row.get('ENDPOINT_NAME', '') == endpoint_name and 
                row.get('ENV', '').upper() == environment.upper()):
                return EndpointConfig(
                    endpoint_name=row.get('ENDPOINT_NAME', ''),
                    environment=row.get('ENV', ''),
                    src_db_name=row.get('SRC_DB_NAME', ''),
                    src_server_name=row.get('SRC_SERVER_NAME', ''),
                    src_db_username=row.get('SRC_DB_USERNAME', '')
                )
        raise DataValidationException(f"Configuración de endpoint no encontrada: {endpoint_name}")
    
    def _process_columns_metadata(self, columns_data: List[Dict], table_name: str) -> List[ColumnMetadata]:
        """Procesa metadatos de columnas"""
        columns_metadata = []
        
        for row in columns_data:
            if row.get('TABLE_NAME', '').upper() == table_name.upper():
                column_meta = ColumnMetadata(
                    name=row.get('COLUMN_NAME', ''),
                    column_id=int(row.get('COLUMN_ID', '0')),
                    data_type=row.get('NEW_DATA_TYPE', 'string'),
                    transformation=row.get('TRANSFORMATION', ''),
                    is_partition=row.get('IS_PARTITION', 'false').lower() in ['true', '1', 'yes', 'y', 't'],
                    is_id=row.get('IS_ID', 'false').lower() in ['true', '1', 'yes', 'y', 't'],
                    is_order_by=row.get('IS_ORDER_BY', 'false').lower() in ['true', '1', 'yes', 'y', 't'],
                    is_filter_date=row.get('IS_FILTER_DATE', 'false').lower() in ['true', '1', 'yes', 'y', 't']
                )
                columns_metadata.append(column_meta)
        
        return columns_metadata
    
    def _build_s3_paths(self, args: Dict[str, str], table_config: TableConfig) -> Dict[str, str]:
        """Construye rutas S3"""
        now_lima = dt.datetime.now(TZ_LIMA)
        year = '2025'#now_lima.strftime('%Y')
        month = '09'#now_lima.strftime('%m')
        day = '18'#now_lima.strftime('%d')
        
        # Extraer nombre limpio de tabla
        source_table_clean = table_config.source_table.split()[0] if ' ' in table_config.source_table else table_config.source_table
        
        day_route = f"{args['TEAM']}/{args['DATA_SOURCE']}/{args['ENDPOINT_NAME']}/{source_table_clean}/year={year}/month={month}/day={day}/"
        
        return {
            'raw': f"s3://{args['S3_RAW_BUCKET']}/{day_route}",
            'stage': f"s3://{args['S3_STAGE_BUCKET']}/{args['TEAM']}/{args['DATA_SOURCE']}/{args['ENDPOINT_NAME']}/{args['TABLE_NAME']}/"
        }
    
    def _read_source_data(self, s3_raw_path: str):
        """Lee datos fuente con cache"""
        try:
            df = self.spark.read.format("parquet").load(s3_raw_path)
            df.cache()  # Cache para optimizar múltiples operaciones
            return df
        except Exception:
            # Retornar DataFrame vacío en caso de error
            return self.spark.createDataFrame([], StructType([]))
    
    def _apply_post_processing(self, df, columns_metadata: List[ColumnMetadata]):
        """Aplica post-procesamiento: deduplicación y ordenamiento"""
        # Identificar columnas especiales
        id_columns = [col.name for col in columns_metadata if col.is_id]
        filter_date_columns = [col.name for col in columns_metadata if col.is_filter_date]
        order_by_columns = [col.name for col in columns_metadata if col.is_order_by]
        
        # Deduplicación si hay columnas de fecha de filtro
        if filter_date_columns and id_columns:
            window_spec = Window.partitionBy(*id_columns).orderBy(*[col(c).desc() for c in filter_date_columns])
            df = df.withColumn("row_number", row_number().over(window_spec))
            df = df.filter(col("row_number") == 1).drop("row_number")
        
        # Ordenamiento
        if order_by_columns:
            df = df.orderBy(*order_by_columns)
        
        return df
    
    def _write_to_stage(self, df, s3_stage_path: str, table_config: TableConfig, columns_metadata: List[ColumnMetadata]):
        """Escribe datos a stage"""
        partition_columns = [col.name for col in columns_metadata if col.is_partition]
        
        if DeltaTable.isDeltaTable(self.spark, s3_stage_path):
            if table_config.load_type in ['incremental', 'between-date']:
                # Merge incremental
                id_columns = [col.name for col in columns_metadata if col.is_id]
                merge_condition = " AND ".join([f"old.{col} = new.{col}" for col in id_columns])
                self.delta_manager.merge_delta_table(df, s3_stage_path, merge_condition)
            else:
                # Overwrite completo
                self.delta_manager.write_delta_table(df, s3_stage_path, partition_columns, "overwrite")
        else:
            # Crear nueva tabla
            self.delta_manager.write_delta_table(df, s3_stage_path, partition_columns, "overwrite")
    
    def _handle_empty_data(self, s3_stage_path: str, columns_metadata: List[ColumnMetadata]):
        """Maneja datos vacíos"""
        if not DeltaTable.isDeltaTable(self.spark, s3_stage_path):
            # Crear DataFrame vacío con esquema
            empty_df = self._create_empty_dataframe(columns_metadata)
            partition_columns = [col.name for col in columns_metadata if col.is_partition]
            self.delta_manager.write_delta_table(empty_df, s3_stage_path, partition_columns)
        
        raise Exception("No data detected to migrate")
    
    def _create_empty_dataframe(self, columns_metadata: List[ColumnMetadata]):
        """Crea DataFrame vacío con esquema"""
        type_mapping = {
            'string': StringType(),
            'int': IntegerType(),
            'integer': IntegerType(),
            'double': DoubleType(),
            'float': DoubleType(),
            'boolean': BooleanType(),
            'timestamp': TimestampType(),
            'date': DateType()
        }
        
        fields = []
        for col_meta in sorted(columns_metadata, key=lambda x: x.column_id):
            data_type = type_mapping.get(col_meta.data_type.lower(), StringType())
            fields.append(StructField(col_meta.name, data_type, True))
        
        schema = StructType(fields)
        return self.spark.createDataFrame([], schema)
    
    def _log_success(self, args: Dict[str, str], table_config: TableConfig, endpoint_config: EndpointConfig, errors: List[str]):
        """Log de éxito"""
        # Implementar logging a DynamoDB
        pass
    
    def _log_error(self, args: Dict[str, str], error_message: str):
        """Log de error"""
        # Implementar logging a DynamoDB y SNS
        pass

def main():
    """Función principal optimizada"""
    # Obtener argumentos
    args = getResolvedOptions(
        sys.argv, 
        ['JOB_NAME', 'S3_RAW_BUCKET', 'S3_STAGE_BUCKET', 'DYNAMO_LOGS_TABLE', 
         'TABLE_NAME', 'ARN_TOPIC_FAILED', 'PROJECT_NAME', 'TEAM', 'DATA_SOURCE', 
         'TABLES_CSV_S3', 'CREDENTIALS_CSV_S3', 'COLUMNS_CSV_S3', 'ENDPOINT_NAME', 'ENVIRONMENT']
    )
    
    # Configurar Spark con optimizaciones
    spark = SparkSession.builder \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()
    
    # Inicializar componentes
    s3_client = boto3.client('s3')
    config_manager = ConfigurationManager(s3_client)
    transformation_engine = TransformationEngine(spark)
    delta_manager = DeltaTableManager(spark)
    
    # Procesar tabla
    processor = DataProcessor(spark, config_manager, transformation_engine, delta_manager)
    processor.process_table(args)

if __name__ == "__main__":
    main()