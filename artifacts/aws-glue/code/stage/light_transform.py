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
            content = response['Body'].read().decode('latin1')  # Cambio a UTF-8
            
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
    
    def parse_transformation(self, expression: str) -> List[Tuple[str, List[str]]]:
        """
        Parsea expresión de transformación y retorna lista de (función, parámetros)
        """
        if not expression or expression.strip() == '':
            return []
        
        functions_with_params = []
        remaining = expression.strip()
        
        match = self.function_pattern.match(remaining)
        if not match:
            # No es una función, es una columna simple
            return [('simple_column', [remaining])]
        
        function_name = match.group(1)
        params_str = match.group(2)
        
        # Extraer parámetros
        params = self._extract_parameters(params_str) if params_str else []
        functions_with_params.append((function_name, params))
        
        return functions_with_params
    
    def _extract_parameters(self, params_str: str) -> List[str]:
        """Extrae parámetros de una función manejando comas en strings"""
        if not params_str:
            return []
        
        params = []
        current_param = ""
        paren_count = 0
        in_quotes = False
        
        i = 0
        while i < len(params_str):
            char = params_str[i]
            
            if char == '"' and (i == 0 or params_str[i-1] != '\\'):
                in_quotes = not in_quotes
                current_param += char
            elif char == '(' and not in_quotes:
                paren_count += 1
                current_param += char
            elif char == ')' and not in_quotes:
                paren_count -= 1
                current_param += char
            elif char == ',' and paren_count == 0 and not in_quotes:
                if current_param.strip():
                    params.append(current_param.strip())
                current_param = ""
            else:
                current_param += char
            
            i += 1
        
        # Agregar último parámetro
        if current_param.strip():
            params.append(current_param.strip())
        
        return params

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
                if expr is not None:
                    transformation_exprs.append(expr.alias(column_meta.name))
                else:
                    # Columna simple sin transformación
                    if column_meta.transformation and column_meta.transformation.strip():
                        # Si hay transformación pero no se pudo parsear, usar la columna original
                        transformation_exprs.append(col(column_meta.transformation).alias(column_meta.name))
                    else:
                        # Sin transformación definida, crear columna null con tipo apropiado
                        spark_type = self._get_spark_type(column_meta.data_type)
                        transformation_exprs.append(lit(None).cast(spark_type).alias(column_meta.name))
            except Exception as e:
                error_msg = f"Error en columna {column_meta.name}: {str(e)}"
                errors.append(error_msg)
                logger.error(error_msg)
                # Agregar columna con valor null apropiado en caso de error
                spark_type = self._get_spark_type(column_meta.data_type)
                transformation_exprs.append(lit(None).cast(spark_type).alias(column_meta.name))
        
        # Aplicar todas las transformaciones en una sola operación
        if transformation_exprs:
            transformed_df = df.select(*transformation_exprs)
        else:
            transformed_df = df
        
        return transformed_df, errors
    
    def _get_spark_type(self, data_type: str):
        """Convierte string de tipo a tipo Spark"""
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
        
        if 'numeric' in data_type.lower():
            return self._parse_decimal_type(data_type)
        
        return type_mapping.get(data_type.lower(), StringType())
    
    def _build_transformation_expression(self, column_meta: ColumnMetadata):
        """Construye expresión de transformación para una columna"""
        functions_with_params = self.parser.parse_transformation(column_meta.transformation)
        
        if not functions_with_params:
            return None
        
        if len(functions_with_params) == 1 and functions_with_params[0][0] == 'simple_column':
            # Es una columna simple
            column_name = functions_with_params[0][1][0] if functions_with_params[0][1] else column_meta.name
            return col(column_name)
        
        # Es una función
        function_name, params = functions_with_params[0]
        return self._create_transformation_expr(function_name, params, column_meta.data_type)
    
    def _create_transformation_expr(self, function_name: str, params: List[str], data_type: str):
        """Crea expresión de transformación para función específica"""
        param_list = params if params else []
        
        if function_name == 'fn_transform_Concatenate':
            columns_to_concat = [col(p.strip()) for p in param_list]
            return concat_ws("|", *[coalesce(trim(c), lit("")) for c in columns_to_concat])
        
        elif function_name == 'fn_transform_Concatenate_ws':
            if len(param_list) < 2:
                raise TransformationException("fn_transform_Concatenate_ws", "Requiere al menos 2 parámetros")
            separator = param_list[-1]
            columns_to_concat = [col(p.strip()) for p in param_list[:-1]]
            return concat_ws(separator, *[coalesce(trim(c), lit("")) for c in columns_to_concat])
        
        elif function_name == 'fn_transform_Integer':
            if not param_list:
                raise TransformationException("fn_transform_Integer", "Requiere nombre de columna")
            origin_column = param_list[0]
            
            # Solo convertir a NULL si realmente es NULL o string vacío/espacios
            return when(
                col(origin_column).isNull(),
                lit(None).cast(IntegerType())
            ).when(
                col(origin_column).cast(StringType()).rlike("^\\s*$"),
                lit(None).cast(IntegerType())
            ).otherwise(
                # Intentar conversión, si falla devolver NULL
                when(col(origin_column).cast(StringType()).rlike("^-?\\d+$"), 
                    col(origin_column).cast(IntegerType())
                ).otherwise(lit(None).cast(IntegerType()))
            )
        
        elif function_name == 'fn_transform_Boolean':
            if not param_list:
                raise TransformationException("fn_transform_Boolean", "Requiere nombre de columna")
            origin_column = param_list[0]
            
            return when(
                col(origin_column).isNull(),
                lit(None).cast(BooleanType())
            ).when(
                col(origin_column).cast(StringType()).rlike("^\\s*$"),
                lit(None).cast(BooleanType())
            ).otherwise(
                when(
                    col(origin_column).cast(StringType()).upper().isin(["1", "TRUE", "T", "YES", "Y"]), 
                    True
                ).when(
                    col(origin_column).cast(StringType()).upper().isin(["0", "FALSE", "F", "NO", "N"]), 
                    False
                ).otherwise(
                    # Si no es un valor booleano reconocible, NULL
                    lit(None).cast(BooleanType())
                )
            )
        
        elif function_name == 'fn_transform_Numeric':
            if not param_list:
                raise TransformationException("fn_transform_Numeric", "Requiere nombre de columna")
            origin_column = param_list[0]
            
            decimal_type = self._parse_decimal_type(data_type)
            
            return when(
                col(origin_column).isNull(),
                lit(None).cast(decimal_type)
            ).when(
                col(origin_column).cast(StringType()).rlike("^\\s*$"),
                lit(None).cast(decimal_type)
            ).when(
                col(origin_column).cast(StringType()).upper().isin(["NAN", "INF", "-INF", "INFINITY", "-INFINITY"]),
                lit(None).cast(decimal_type)
            ).otherwise(
                # Intentar conversión numérica
                when(
                    col(origin_column).cast(StringType()).rlike("^-?\\d*\\.?\\d*$"),
                    col(origin_column).cast(decimal_type)
                ).otherwise(lit(None).cast(decimal_type))
            )
        
        elif function_name == 'fn_transform_ClearString':
            origin_column = param_list[0] if param_list else None
            if not origin_column:
                raise TransformationException("fn_transform_ClearString", "Requiere nombre de columna")
            
            if len(param_list) > 1:
                default = param_list[1]
                # Si el default empieza con $, es un literal
                if default.startswith('$'):
                    default_expr = lit(default[1:])  # Remover el $
                else:
                    default_expr = col(default)
                
                return when(
                    col(origin_column).isNull() | 
                    (trim(col(origin_column)) == "") |
                    (trim(col(origin_column)) == "None"),  # Agregar check para "None"
                    default_expr
                ).otherwise(trim(col(origin_column)))
            else:
                # Sin valor por defecto - devolver NULL real para valores vacíos/nulos
                return when(
                    col(origin_column).isNull() |
                    (trim(col(origin_column)) == "") |
                    (trim(col(origin_column)) == "None"),  # Agregar check para "None"
                    lit(None).cast(StringType())  # NULL real, no string "None"
                ).otherwise(
                    trim(col(origin_column))
                )
        
        elif function_name in ['fn_transform_DateMagic', 'fn_transform_DatetimeMagic']:
            if len(param_list) < 4:
                raise TransformationException(function_name, "Requiere 4 parámetros: column, NULL, format, default")
            
            origin_column = param_list[0]
            null_param = param_list[1]  # Parámetro "NULL" - ignorado
            date_format_param = param_list[2]
            value_default = param_list[3]
            
            logger.info(f"Procesando fecha - Columna: {origin_column}, Formato: {date_format_param}, Default: {value_default}")
            
            # Crear valor por defecto
            default_date_expr = None
            if value_default and value_default.upper() != "NULL" and len(value_default.strip()) > 0:
                try:
                    default_date_expr = to_timestamp(lit(value_default), date_format_param)
                except:
                    default_date_expr = lit(None).cast(TimestampType())
            else:
                default_date_expr = lit(None).cast(TimestampType())
            
            return when(
                # Caso 1: Valor es NULL
                col(origin_column).isNull(),
                default_date_expr
            ).when(
                # Caso 2: Valor es string vacío o solo espacios
                col(origin_column).cast(StringType()).rlike("^\\s*$"),
                default_date_expr
            ).when(
                # Caso 3: Valor es "None" string
                col(origin_column).cast(StringType()) == "None",
                default_date_expr
            ).when(
                # Caso 4: Es un número Magic Date válido (rango típico: 700000 - 3500000)
                col(origin_column).cast(StringType()).rlike("^([7-9]\\d{5}|[1-3]\\d{6})$") & 
                col(origin_column).cast(IntegerType()).between(700000, 3500000),
                # Convertir Magic Date a fecha real
                to_timestamp(
                    date_format(
                        date_add(
                            to_date(lit(BASE_DATE_MAGIC), "yyyy-MM-dd"), 
                            col(origin_column).cast(IntegerType()) - lit(MAGIC_OFFSET)
                        ), 
                        date_format_param
                    ), 
                    date_format_param
                )
            ).when(
                # Caso 5: Ya es una fecha en formato string (YYYY-MM-DD o similar)
                col(origin_column).cast(StringType()).rlike("^\\d{4}-\\d{2}-\\d{2}"),
                to_timestamp(col(origin_column).cast(StringType()), date_format_param)
            ).when(
                # Caso 6: Es timestamp ya formateado (con hora)
                col(origin_column).cast(StringType()).rlike("^\\d{4}-\\d{2}-\\d{2}\\s+\\d{2}:\\d{2}:\\d{2}"),
                to_timestamp(col(origin_column).cast(StringType()), date_format_param)
            ).otherwise(
                # Caso por defecto: no se puede convertir, usar default
                default_date_expr
            )
        
        elif function_name == 'fn_transform_Datetime':
            origin_column = param_list[0] if param_list else ''
            
            if origin_column == '' or origin_column.upper() == 'NULL':
                # Si no hay columna especificada, usar timestamp actual en Lima
                return from_utc_timestamp(current_timestamp(), "America/Lima")
            else:
                # Si hay columna especificada
                return when(
                    col(origin_column).isNull() |
                    (col(origin_column).cast(StringType()).rlike("^\\s*$")) |
                    (col(origin_column).cast(StringType()) == "None"),
                    from_utc_timestamp(current_timestamp(), "America/Lima")
                ).otherwise(
                    # Intentar convertir el valor existente
                    coalesce(
                        # Intentar como timestamp directo
                        to_timestamp(col(origin_column)),
                        # Si falla, intentar con formato específico
                        to_timestamp(col(origin_column), "yyyy-MM-dd HH:mm:ss"),
                        # Si todo falla, usar timestamp actual
                        from_utc_timestamp(current_timestamp(), "America/Lima")
                    )
                )
        
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

class DeltaTableManager:
    """Maneja operaciones con tablas Delta"""
    
    def __init__(self, spark_session):
        self.spark = spark_session
    
    def write_delta_table(self, df, s3_path: str, partition_columns: List[str], 
                         mode: str = "overwrite") -> None:
        """Escribe DataFrame a tabla Delta con optimizaciones válidas"""
        writer = df.write.format("delta").mode(mode)
        
        if partition_columns:
            writer = writer.partitionBy(*partition_columns)
        
        # Configuraciones Delta válidas
        writer = writer.option("delta.deletedFileRetentionDuration", "interval 7 days")
        writer = writer.option("delta.logRetentionDuration", "interval 30 days")
        
        # Optimización a nivel de Spark (no Delta específico)
        writer = writer.option("spark.sql.adaptive.enabled", "true")
        writer = writer.option("spark.sql.adaptive.coalescePartitions.enabled", "true")
        
        writer.save(s3_path)
    
    def merge_delta_table(self, df, s3_path: str, merge_condition: str) -> None:
        """Realiza merge en tabla Delta"""
        delta_table = DeltaTable.forPath(self.spark, s3_path)
        
        # Eliminar duplicados antes del merge para mejorar performance
        df_deduplicated = df.dropDuplicates()
        
        delta_table.alias("old").merge(
            df_deduplicated.alias("new"), 
            merge_condition
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    
    def optimize_delta_table(self, s3_path: str) -> None:
        """Optimiza tabla Delta con comandos válidos"""
        try:
            delta_table = DeltaTable.forPath(self.spark, s3_path)
            
            # OPTIMIZE - compacta archivos pequeños
            self.spark.sql(f"OPTIMIZE delta.`{s3_path}`")
            
            # VACUUM - limpia archivos viejos (más de 7 días)
            delta_table.vacuum(168)  # 168 horas = 7 días
            
            # Generar manifest para compatibilidad con otros sistemas
            delta_table.generate("symlink_format_manifest")
            
        except Exception as e:
            logger.warning(f"Error optimizando tabla Delta en {s3_path}: {str(e)}")

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
            
            logger.info(f"Procesando tabla {args['TABLE_NAME']} con {len(columns_metadata)} columnas")
            for col_meta in columns_metadata[:5]:  # Log primeras 5 columnas
                logger.info(f"  Columna: {col_meta.name}, Transformación: {col_meta.transformation}")
            
            # Construir rutas S3
            s3_paths = self._build_s3_paths(args, table_config)
            logger.info(f"Leyendo desde: {s3_paths['raw']}")
            logger.info(f"Escribiendo a: {s3_paths['stage']}")
            
            # Leer datos source
            source_df = self._read_source_data(s3_paths['raw'])
            
            if source_df.count() == 0:
                self._handle_empty_data(s3_paths['stage'], columns_metadata)
                return
            
            logger.info(f"Datos fuente leídos: {source_df.count()} filas")
            
            # Aplicar transformaciones
            transformed_df, transformation_errors = self.transformation_engine.apply_transformations(
                source_df, columns_metadata
            )
            
            if transformation_errors:
                logger.warning(f"Errores de transformación: {len(transformation_errors)}")
                for error in transformation_errors:
                    logger.warning(f"  {error}")
            
            # Aplicar post-procesamiento (deduplicación, ordenamiento)
            final_df = self._apply_post_processing(transformed_df, columns_metadata)
            
            logger.info(f"Datos transformados: {final_df.count()} filas")
            
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
                    is_id=row.get('IS_ID', '').upper() == 'T',
                    is_order_by=row.get('IS_ORDER_BY', '').upper() == 'T',
                    is_filter_date=row.get('IS_FILTER_DATE', '').upper() == 'T'
                )
                columns_metadata.append(column_meta)
        
        return columns_metadata
    
    def _build_s3_paths(self, args: Dict[str, str], table_config: TableConfig) -> Dict[str, str]:
        """Construye rutas S3"""
        now_lima = dt.datetime.now(TZ_LIMA)
        year = now_lima.strftime('%Y')
        month = now_lima.strftime('%m')
        day = now_lima.strftime('%d')
        
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
        except Exception as e:
            logger.error(f"Error leyendo datos desde {s3_raw_path}: {str(e)}")
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
        fields = []
        for col_meta in sorted(columns_metadata, key=lambda x: x.column_id):
            data_type = self.transformation_engine._get_spark_type(col_meta.data_type)
            fields.append(StructField(col_meta.name, data_type, True))
        
        schema = StructType(fields)
        return self.spark.createDataFrame([], schema)
    
    def _log_success(self, args: Dict[str, str], table_config: TableConfig, endpoint_config: EndpointConfig, errors: List[str]):
        """Log de éxito"""
        success_msg = f"Procesamiento exitoso para tabla {args['TABLE_NAME']}"
        if errors:
            success_msg += f" con {len(errors)} advertencias de transformación"
        logger.info(success_msg)
        # Implementar logging a DynamoDB si es necesario
        pass
    
    def _log_error(self, args: Dict[str, str], error_message: str):
        """Log de error"""
        logger.error(f"Error procesando tabla {args['TABLE_NAME']}: {error_message}")
        # Implementar logging a DynamoDB y SNS si es necesario
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
    
    # Configurar Spark con optimizaciones válidas
    spark = SparkSession.builder \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.adaptive.localShuffleReader.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED") \
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED") \
        .getOrCreate()
    
    # Configurar sistema de archivos S3
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark.sparkContext._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    
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