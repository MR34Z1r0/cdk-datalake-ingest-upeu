# Import required modules for Glue ETL
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *

import concurrent.futures as futures
import datetime as dt
import calendar
import logging
import os
import argparse
import uuid
import boto3
import pytz
import re
import io
import gzip
from dateutil.relativedelta import relativedelta

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

IS_AWS_GLUE = True
IS_AWS_S3 = True

# Import aje_libs (same as original)
try:
    import aje_libs
except ImportError as e:
    # Search for aje_libs.zip file and extract it
    import glob
    import zipfile
    
    search_paths = ['/tmp', '/tmp/glue-python-libs-*', '/glue/lib']
    for search_pattern in search_paths:
        for base_path in glob.glob(search_pattern):
            if os.path.isdir(base_path):
                # Look for aje_libs.zip file
                zip_file = os.path.join(base_path, 'aje_libs.zip')
                if os.path.exists(zip_file):
                    # Extract the zip file
                    extract_path = os.path.join(base_path, 'extracted')
                    try:
                        os.makedirs(extract_path, exist_ok=True)
                        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
                            zip_ref.extractall(extract_path)
                        
                        # Add extract path to sys.path if aje_libs is there
                        if 'aje_libs' in os.listdir(extract_path):
                            sys.path.insert(0, extract_path)
                            break
                            
                    except Exception as extract_err:
                        pass
    
    # Try importing again
    try:
        import aje_libs
    except ImportError as e2:
        pass

# Import custom helpers
from aje_libs.common.logger import custom_logger, set_logger_config
from aje_libs.common.helpers.dynamodb_helper import DynamoDBHelper
from aje_libs.common.helpers.s3_helper import S3Helper
from aje_libs.common.helpers.secrets_helper import SecretsHelper
from aje_libs.bd.helpers.datafactory_helper import DatabaseFactoryHelper

# Setup timezone and date variables
TZ_LIMA = pytz.timezone('America/Lima')
YEARS_LIMA = dt.datetime.now(TZ_LIMA).strftime('%Y')
MONTHS_LIMA = dt.datetime.now(TZ_LIMA).strftime('%m')
DAYS_LIMA = dt.datetime.now(TZ_LIMA).strftime('%d')
NOW_LIMA = dt.datetime.now(pytz.utc).astimezone(TZ_LIMA)

class DataExtractor:
    def __init__(self, config, glue_context, spark_session):
        self.config = config
        self.glueContext = glue_context
        self.spark = spark_session
        self.logger = custom_logger(__name__)
        
        # Initialize DynamoDB helpers
        self.dynamo_logs_table = self.config['DYNAMO_LOGS_TABLE'].strip()
        self.logs_table_db = DynamoDBHelper(self.dynamo_logs_table, "PROCESS_ID", None)
        self.team = self.config.get('TEAM')
        self.data_source = self.config.get('DATA_SOURCE')
        self.environment = self.config.get('ENVIRONMENT')
        self.project_name = self.config.get('PROJECT_NAME')
        self.region = self.config.get('REGION')
        self.table_name = self.config.get('TABLE_NAME')
        
        # Initialize S3 helper
        self.s3_helper = S3Helper(self.config['S3_RAW_BUCKET'])
        
        # Load configuration from CSV files in S3
        self._load_csv_configurations()
        
        # Initialize table data and endpoint data
        self._initialize_data()
        
    def _load_csv_configurations(self):
        """Load configuration from CSV files in S3"""
        try:
            import csv
            from io import StringIO
            def load_csv_from_s3(s3_path):
                """Load CSV file from S3 and return as list of dictionaries"""
                s3_client = boto3.client('s3')
                bucket = s3_path.split('/')[2]
                key = '/'.join(s3_path.split('/')[3:])
                
                response = s3_client.get_object(Bucket=bucket, Key=key)
                content = response['Body'].read().decode('latin-1')
                
                csv_data = []
                reader = csv.DictReader(StringIO(content), delimiter=';')
                for row in reader:
                    csv_data.append(row)
                
                return csv_data
            
            def load_csv_from_local(file_path):
                import csv
                """Carga un archivo CSV local y lo devuelve como lista de diccionarios"""
                csv_data = []
                with open(file_path, mode='r', encoding='latin-1') as file:
                    reader = csv.DictReader(file, delimiter=';')
                    for row in reader:
                        csv_data.append(row)
                return csv_data
            
            # Load CSV data
            self.logger.info(f"TABLES_CSV_S3: {self.config['TABLES_CSV_S3']}")
            tables_data = load_csv_from_s3(self.config['TABLES_CSV_S3']) if IS_AWS_S3 else load_csv_from_local(self.config['TABLES_CSV_S3'])
            self.logger.info(f"tables_data: {tables_data[0]}")
            self.logger.info(f"CREDENTIALS_CSV_S3: {self.config['CREDENTIALS_CSV_S3']}")
            credentials_data = load_csv_from_s3(self.config['CREDENTIALS_CSV_S3']) if IS_AWS_S3 else load_csv_from_local(self.config['CREDENTIALS_CSV_S3'])
            self.logger.info(f"credentials_data: {credentials_data[0]}")
            self.logger.info(f"COLUMNS_CSV_S3: {self.config['COLUMNS_CSV_S3']}")
            columns_data = load_csv_from_s3(self.config['COLUMNS_CSV_S3']) if IS_AWS_S3 else load_csv_from_local(self.config['COLUMNS_CSV_S3'])
            self.logger.info(f"columns_data: {columns_data[0]}")

            self.logger.info(f"config: {self.config}")

            # Filter data for current table and database
            table_name = self.config.get('TABLE_NAME')
            endpoint_name = self.config.get('ENDPOINT_NAME')
            environment = self.config.get('ENVIRONMENT')
            
            # Find table configuration
            self.table_data = None
            for row in tables_data:
                if row.get('STAGE_TABLE_NAME', '').upper() == table_name.upper():
                    self.table_data = row
                    self.logger.info(f"Found table configuration for {table_name}")
                    break
            
            if not self.table_data:
                raise Exception(f"Table configuration not found for {table_name}")
            
            # Process the COLUMNS field to handle potential SQL Server identifier length issues
            self._process_columns_field()
            
            # Find database credentials
            self.endpoint_data = None
            for row in credentials_data:
                if (row.get('ENDPOINT_NAME', '') == endpoint_name and 
                    row.get('ENV', '').upper() == environment.upper()):
                    self.endpoint_data = row
                    break
            
            if not self.endpoint_data:
                raise Exception(f"Endpoint credentials not found for {endpoint_name} in {environment}")
            
            # Add ENDPOINT_NAME for compatibility
            self.endpoint_data['ENDPOINT_NAME'] = self.endpoint_data.get('ENDPOINT_NAME', '')
            
            # Apply old logic to determine LOAD_TYPE if not explicitly set
            if not self.table_data.get('LOAD_TYPE') or self.table_data.get('LOAD_TYPE', '').strip() == '':
                if self.table_data.get('SOURCE_TABLE_TYPE', '') == 't':
                    if self.endpoint_data.get('ENDPOINT_NAME', '') == 'SALESFORCE_ING':
                        self.table_data['LOAD_TYPE'] = 'days_off'
                        self.table_data['NUM_DAYS'] = '10'
                    else:
                        self.table_data['LOAD_TYPE'] = 'incremental'
                else:
                    self.table_data['LOAD_TYPE'] = 'full'
            
            self.logger.info(f"Determined LOAD_TYPE: {self.table_data.get('LOAD_TYPE', 'not set')} for table {table_name}")
            
            # Override LOAD_TYPE if FORCE_FULL_LOAD is enabled
            if self.config.get('FORCE_FULL_LOAD', False) and self.table_data.get('LOAD_TYPE') == 'incremental':
                original_load_type = self.table_data.get('LOAD_TYPE')
                self.table_data['LOAD_TYPE'] = 'full'
                self.logger.info(f"FORCE_FULL_LOAD=true - Overriding LOAD_TYPE from '{original_load_type}' to 'full' for table {table_name}")
            
            # Filter columns for current table
            self.columns_metadata = []
            for row in columns_data:
                if row.get('TABLE_NAME', '').upper() == table_name.upper():
                    self.columns_metadata.append(row)
            
            self.logger.info(f"Loaded configuration for table: {table_name}")
            self.logger.info(f"Columns count: {len(self.columns_metadata)}")
            
        except Exception as e:
            self.logger.error(f"Error loading CSV configurations: {str(e)}")
            self._log_error(str(e))
            raise Exception(f"Failed to load CSV configurations: {str(e)}")
        
    def _initialize_data(self):
        """Initialize table and endpoint data from loaded CSV configurations"""
        try:
            # Set S3 path using loaded data
            team = self.team
            data_source = self.data_source
            endpoint_name = self.endpoint_data['ENDPOINT_NAME']
            # Get clean table name (remove alias after space) for S3 path
            clean_table_name = self._get_clean_table_name()
            self.day_route = f"{team}/{data_source}/{endpoint_name}/{clean_table_name}/year={YEARS_LIMA}/month={MONTHS_LIMA}/day={DAYS_LIMA}/"
            
            self.s3_raw_path = f"s3://{self.config['S3_RAW_BUCKET']}/{self.day_route}"
            self.bucket = self.config['S3_RAW_BUCKET']
            
            # Initialize database connection
            self.init_db_connection()
            
        except Exception as e:
            self.logger.error("Error while searching for table data")
            self.logger.error(e)
            self._log_error(str(e))
            raise Exception(f"Failed to initialize data: {str(e)}")
    
    def init_db_connection(self):
        """Initialize database connection using DatabaseFactoryHelper"""
        try:
            # Get credentials using SecretsHelper
            self.secrets_helper = SecretsHelper(f"{self.config['ENVIRONMENT']}/{self.config['PROJECT_NAME']}/{self.team}/{self.data_source}".lower())
            password = self.secrets_helper.get_secret_value(self.endpoint_data["SRC_DB_SECRET"])
            
            # Record connection information for logging
            self.db_type = self.endpoint_data['BD_TYPE']
            self.server = self.endpoint_data['SRC_SERVER_NAME']
            self.port = self.endpoint_data['DB_PORT_NUMBER']
            self.db_name = self.endpoint_data['SRC_DB_NAME']
            self.username = self.endpoint_data['SRC_DB_USERNAME']
            
            # Additional params based on database type
            additional_params = {}
            
            if self.db_type == 'oracle':
                additional_params['service_name'] = self.db_name
                self.url = f"jdbc:oracle:thin:@{self.server}:{self.port}:{self.db_name}"
                self.driver = "oracle.jdbc.driver.OracleDriver"
            elif self.db_type == 'mssql':
                self.url = f"jdbc:sqlserver://{self.server}:{self.port};databaseName={self.db_name}"
                self.driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
            elif self.db_type == 'mysql':
                additional_params['charset'] = 'utf8mb4'
                self.url = f"jdbc:mysql://{self.server}:{self.port}/{self.db_name}"
                self.driver = "com.mysql.cj.jdbc.Driver"
            
            # Store connection properties for Spark JDBC
            self.connection_properties = {
                "user": self.username,
                "password": password,
                "driver": self.driver
            }
            
            # Create database helper using factory (for simple queries)
            self.db_helper = DatabaseFactoryHelper.create_helper(
                db_type=self.db_type,
                server=self.server,
                database=self.db_name,
                username=self.username,
                password=password,
                port=int(self.port) if self.port else None,
                **additional_params
            )
            self.logger.info(f"Database connection initialized for {self.db_type} database")
            self.logger.info(f"JDBC URL: {self.url}")
            
        except Exception as e:
            self.logger.error(f"Error initializing database connection: {str(e)}")
            raise
    
    def _get_clean_table_name(self):
        """Extract clean table name from SOURCE_TABLE, removing alias after space."""
        source_table = self.table_data.get('SOURCE_TABLE', self.table_name)
        # Split by space and take only the first part (table name)
        clean_name = source_table.split()[0] if source_table and ' ' in source_table else source_table
        self.logger.info(f"Clean table name extracted: '{source_table}' -> '{clean_name}'")
        return clean_name
    
    def _log_error(self, error_message):
        """Log error to DynamoDB and update table status"""
        try:
            # Send error message via SNS if topic ARN is available
            if 'TOPIC_ARN' in self.config and self.config['TOPIC_ARN']:
                self.send_error_message(self.config['TOPIC_ARN'], self.table_data.get('TARGET_TABLE_NAME', self.table_name), error_message)
            
            # Add log entry to DynamoDB logs table
            clean_table_name = self._get_clean_table_name()
            process_id = f"DLB_{self.table_name.split('_')[0]}_{clean_table_name}_{NOW_LIMA.strftime('%Y%m%d_%H%M%S')}"
            log = {
                'PROCESS_ID': process_id,
                'DATE_SYSTEM': NOW_LIMA.strftime('%Y%m%d_%H%M%S'),
                'PROJECT_NAME': self.config['PROJECT_NAME'],
                'FLOW_NAME': 'extract_bigmagic',
                'TASK_NAME': 'extract_bigmagic_table',
                'TASK_STATUS': 'error',
                'MESSAGE': error_message,
                'PROCESS_TYPE': 'D' if self.table_data.get('LOAD_TYPE', 'full').strip() in ['incremental'] else 'F',
                'CONTEXT': f"{{server='[{self.endpoint_data['ENDPOINT_NAME']},{self.endpoint_data['SRC_SERVER_NAME']}]', user='{self.endpoint_data['SRC_DB_USERNAME']}', table='{self.table_data.get('SOURCE_TABLE', self.table_name)}'}}"
            }
            self.logs_table_db.put_item(log)
        except Exception as e:
            self.logger.error(f"Failed to log error: {str(e)}")
    
    def _log_success(self):
        """Log success to DynamoDB and update table status"""
        try:
            # Add log entry to DynamoDB logs table
            clean_table_name = self._get_clean_table_name()
            process_id = f"DLB_{self.table_name.split('_')[0]}_{clean_table_name}_{NOW_LIMA.strftime('%Y%m%d_%H%M%S')}"
            self.logger.info(f"process_id: {process_id}")
            log = {
                'PROCESS_ID': process_id,
                'DATE_SYSTEM': NOW_LIMA.strftime('%Y%m%d_%H%M%S'),
                'PROJECT_NAME': self.config['PROJECT_NAME'],
                'FLOW_NAME': 'extract_bigmagic',
                'TASK_NAME': 'extract_bigmagic_table',
                'TASK_STATUS': 'satisfactorio',
                'MESSAGE': '',
                'PROCESS_TYPE': 'D' if self.table_data.get('LOAD_TYPE', 'full').strip() in ['incremental'] else 'F',
                'CONTEXT': f"{{server='[{self.endpoint_data['ENDPOINT_NAME']},{self.endpoint_data['SRC_SERVER_NAME']}]', user='{self.endpoint_data['SRC_DB_USERNAME']}', table='{self.table_data.get('SOURCE_TABLE', self.table_name)}'}}"
            }
            self.logs_table_db.put_item(log)
            self.logger.info("DynamoDB updated with success status")
        except Exception as e:
            self.logger.error(f"Failed to log success: {str(e)}")
    
    def send_error_message(self, topic_arn, table_name, error):
        """Send error message via SNS"""
        client = boto3.client("sns")
        response = client.publish(
            TopicArn=topic_arn,
            Message=f"Failed table: {table_name} \nStep: raw job \nLog ERROR : {error}"
        )
        return response

    def delete_from_target(self, bucket, s3_raw_path):
        """Delete objects from S3 bucket with specified prefix"""
        try:
            # Use S3Helper to manage deletions
            objects_to_delete = self.s3_helper.list_objects(prefix=s3_raw_path)
            if objects_to_delete:
                keys_to_delete = [obj.get('Key') for obj in objects_to_delete]
                self.s3_helper.delete_objects(keys_to_delete)
                self.logger.info(f"Deleted {len(keys_to_delete)} objects from {s3_raw_path}")
            else:
                self.logger.info(f"No objects found to delete in {s3_raw_path}")
        except Exception as e:
            self.logger.error(f"Error deleting objects from S3: {str(e)}")
            raise e

    def transform_to_dt(self, date):
        """Convert string date to datetime object"""
        start_dt = dt.datetime(
            year=int(date[:4]),
            month=int(date[5:7]),
            day=int(date[8:10]),
            hour=int(date[11:13]),
            minute=int(date[14:16]),
            second=int(date[17:19])
        )
        return start_dt

    def get_limits_for_filter(self, month_diff, data_type):
        """Clean month_diff text value"""
        month_diff = month_diff.strip()
        #Check ' character
        if "'" in month_diff:
            month_diff = month_diff.replace("'", "")

        """Get lower and upper limits for date filters based on data type"""
        data_type = data_type.strip()
        upper_limit = dt.datetime.now(TZ_LIMA)
        lower_limit = upper_limit - relativedelta(months=(-1*int(month_diff)))
        
        if data_type == "aje_period":
            return lower_limit.strftime('%Y%m'), upper_limit.strftime('%Y%m')
        
        elif data_type == "aje_date":
            _, last_day = calendar.monthrange(upper_limit.year, upper_limit.month)
            upper_limit = upper_limit.replace(day=last_day, tzinfo=TZ_LIMA)
            lower_limit = lower_limit.replace(day=1, tzinfo=TZ_LIMA)
            upper_limit = (upper_limit - dt.datetime(1900, 1, 1, tzinfo=TZ_LIMA)).days + 693596
            lower_limit = (lower_limit - dt.datetime(1900, 1, 1, tzinfo=TZ_LIMA)).days + 693596
            return str(lower_limit), str(upper_limit)
            
        elif data_type == "aje_processperiod":
            _, last_day = calendar.monthrange(upper_limit.year, upper_limit.month)
            upper_limit = upper_limit.replace(day=last_day, tzinfo=TZ_LIMA)
            lower_limit = lower_limit.replace(day=1, tzinfo=TZ_LIMA)
            upper_limit = (upper_limit - dt.datetime(1900, 1, 1, tzinfo=TZ_LIMA)).days + 693596
            lower_limit = (lower_limit - dt.datetime(1900, 1, 1, tzinfo=TZ_LIMA)).days + 693596
            return str(lower_limit), str(upper_limit)
      
        return lower_limit.strftime('%Y%m'), upper_limit.strftime('%Y%m')

    def execute_db_query_spark(self, query):
        """Execute query using Spark JDBC and return Spark DataFrame"""
        try:
            self.logger.info(f"Executing query with Spark JDBC: {query}")
            
            # Use Spark to read from database
            df = self.spark.read \
                .format("jdbc") \
                .option("url", self.url) \
                .option("query", query) \
                .option("user", self.connection_properties["user"]) \
                .option("password", self.connection_properties["password"]) \
                .option("driver", self.connection_properties["driver"]) \
                .load()
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error executing Spark database query: {str(e)}")
            raise

    def execute_simple_query(self, query):
        """Execute a simple query that returns a dictionary (for min/max queries)"""
        try:
            self.logger.info(f"Executing simple query on database: {query}")
            df = self.db_helper.execute_query_as_dict(query)
            return df
        except Exception as e:
            self.logger.error(f"Error executing simple database query: {str(e)}")
            raise

    def write_spark_dataframe_to_s3_parquet(self, df, s3_path, filename=None):
        """Write Spark DataFrame to S3 in Parquet format using Glue"""
        try:
            if filename is None:
                filename = f"data_{uuid.uuid4().hex[:8]}"
            
            # Remove .parquet extension if provided since Spark adds it automatically
            if filename.endswith('.parquet'):
                filename = filename[:-8]
            
            # Convert Spark DataFrame to DynamicFrame for better Glue integration
            dynamic_frame = self.glueContext.create_dynamic_frame.from_rdd(
                df.rdd, "dynamic_frame"
            )
            
            # Write to S3 as Parquet
            full_s3_path = f"{s3_path}{filename}"
            
            self.glueContext.write_dynamic_frame.from_options(
                frame=dynamic_frame,
                connection_type="s3",
                connection_options={
                    "path": full_s3_path,
                    "compression": "snappy"
                },
                format="parquet"
            )
            
            self.logger.info(f"Successfully wrote Spark DataFrame to {full_s3_path} as Parquet")
            return full_s3_path
            
        except Exception as e:
            self.logger.error(f"Error writing Spark DataFrame to S3 as Parquet: {str(e)}")
            # Fallback to direct Spark write
            try:
                full_s3_path = f"{s3_path}{filename}"
                df.write.mode("overwrite").parquet(full_s3_path)
                self.logger.info(f"Successfully wrote using Spark direct write to {full_s3_path}")
                return full_s3_path
            except Exception as e2:
                self.logger.error(f"Direct Spark write also failed: {str(e2)}")
                raise e

    def _process_columns_field(self):
        """Process the COLUMNS field to handle potential SQL Server identifier length issues"""
        try:
            if not self.table_data or 'COLUMNS' not in self.table_data:
                self.logger.info("No COLUMNS field found in table_data")
                return
                
            columns_str = self.table_data['COLUMNS']
            if not columns_str or columns_str.strip() == '':
                self.logger.info("COLUMNS field is empty or None")
                return
                
            self.logger.info(f"Processing COLUMNS field for table: {self.table_data.get('STAGE_TABLE_NAME', 'UNKNOWN')}")
                
            # Split columns by comma, but be careful with complex expressions containing commas
            columns = []
            current_column = ""
            paren_count = 0
            quote_count = 0
            
            for char in columns_str:
                current_column += char
                if char == '(' and quote_count % 2 == 0:
                    paren_count += 1
                elif char == ')' and quote_count % 2 == 0:
                    paren_count -= 1
                elif char == "'":
                    quote_count += 1
                elif char == ',' and paren_count == 0 and quote_count % 2 == 0:
                    columns.append(current_column[:-1].strip())  # Remove the comma
                    current_column = ""
            
            # Add the last column
            if current_column.strip():
                columns.append(current_column.strip())
            
            # Process each individual column - keep all as-is (no truncation)
            processed_columns = []
            
            for i, col in enumerate(columns):
                col = col.strip()
                if not col:
                    continue
                processed_columns.append(col)
            
            # Update the table data with processed columns
            result = ', '.join(processed_columns)
            self.table_data['COLUMNS'] = result
            
            self.logger.info(f"Processed {len(columns)} columns successfully")
                
        except Exception as e:
            self.logger.error(f"ERROR in _process_columns_field: {e}")
            self.logger.error(f"Table: {self.table_data.get('STAGE_TABLE_NAME', 'UNKNOWN') if self.table_data else 'NO_TABLE_DATA'}")

    def get_min_max_values(self, partition_column):
        """Get min and max values for a partition column"""
        try:
            source_schema = self.table_data.get('SOURCE_SCHEMA', '')
            source_table = self.table_data.get('SOURCE_TABLE', '')
            
            # Create query to get min and max values
            min_max_query = f"SELECT MIN({partition_column}) as min_val, MAX({partition_column}) as max_val FROM {source_schema}.{source_table} {self.table_data.get('JOIN_EXPR', '')} WHERE {partition_column} <> 0"
            if self.table_data.get('FILTER_EXP', '').strip() != '':
                min_max_query = f"{min_max_query} AND {self.table_data['FILTER_EXP']}"
            self.logger.info(f"Executing min/max query: {min_max_query}")
            
            # Execute query
            df_min_max = self.execute_simple_query(min_max_query) 
            # Get min and max values from the DataFrame and convert to integers
            min_raw = df_min_max[0]['min_val']
            max_raw = df_min_max[0]['max_val']

            min_val = int(min_raw) if min_raw is not None else None
            max_val = int(max_raw) if max_raw is not None else None
            
            return min_val, max_val
            
        except Exception as e:
            self.logger.error(f"Error getting min and max values: {str(e)}")
            raise
    
    def get_partitioned_query(self, partition_column, min_val, increment, partition_index, num_partitions):
        """Generate partitioned query based on min/max range using integer values"""
        # Calculate start and end values as integers
        start_value = int(min_val + (increment * partition_index))
        
        # For the last partition, use max_val + 1 to ensure we include the max value
        if partition_index == num_partitions - 1:
            end_value = int(min_val + (increment * (partition_index + 1))) + 1
        else:
            end_value = int(min_val + (increment * (partition_index + 1)))
        
        # Get columns and build the query
        columns_aux = self.table_data['COLUMNS']
        if self.table_data.get('ID_COLUMN', '') != '':
            columns_aux = f"{self.table_data['ID_COLUMN']} as id," + self.table_data['COLUMNS']
        
        # Use >= and < for the range to avoid overlaps
        query = f"SELECT {columns_aux} FROM {self.table_data.get('SOURCE_SCHEMA', '')}.{self.table_data.get('SOURCE_TABLE', '')} {self.table_data.get('JOIN_EXPR', '')} WHERE {partition_column} >= {start_value} AND {partition_column} < {end_value}"
        
        # Add additional filters if they exist
        if self.table_data.get('FILTER_EXP', '').strip() != '':
            query += f" AND ({self.table_data['FILTER_EXP']})"
            
        self.logger.info(f"Partitioned query {partition_index}: {query} (Range: {start_value} to {end_value})")
        return query
   
    def get_query_for_date_range(self, start, end):
       """Generate query for a date range"""
       query = self.table_data['QUERY_BY_GLUE']
       
       if 'FILTER_TYPE' in self.table_data.keys():
           start, end = self.change_date_format(start, end, self.table_data['FILTER_TYPE'])
           self.logger.debug(f"Start Date: {start}")
           self.logger.debug(f"End Date: {end}")

       if ',' in self.table_data['FILTER_COLUMN']:
           filter_columns = self.table_data['FILTER_COLUMN'].split(",")
           first_filter = filter_columns[0]
           last_filter = filter_columns[1]

           query += f" WHERE ({first_filter} IS NOT NULL and {first_filter} BETWEEN {start} AND {end}) OR ({last_filter} IS NOT NULL and {last_filter} BETWEEN {start} AND {end})"
       else:
           first_filter = self.table_data['FILTER_COLUMN']
           query += f" WHERE {first_filter} is not null and {first_filter} BETWEEN {start} AND {end}"
           
       self.logger.info(query)
       return query
   
    def change_date_format(self, start, end, date_type):
       """Change date format based on database type"""
       if date_type == 'smalldatetime':
           date_format = f"CONVERT(smalldatetime, 'date_to_replace', 120)"

       elif date_type == 'DATE':
           date_format = f"TO_DATE('date_to_replace', 'YYYY-MM-DD HH24:MI:SS')"
           end = end[:19]
           start = start[:19]

       elif date_type == 'TIMESTAMP(6)':
           date_format = f"TO_TIMESTAMP('date_to_replace', 'YYYY-MM-DD HH24:MI:SS.FF')"

       elif date_type == 'SQL_DATETIME':
           date_format = f"CONVERT(DATETIME, 'date_to_replace',  102)"

       elif date_type == 'BIGINT':
           end = dt.datetime.strptime(end, "%Y-%m-%d %H:%M:%S")
           end = int(end.timestamp())
           start = dt.datetime.strptime(start, "%Y-%m-%d %H:%M:%S")
           start = int(start.timestamp())
           date_format = "date_to_replace"

       end = date_format.replace("date_to_replace", str(end))
       start = date_format.replace("date_to_replace", str(start))
       return start, end
   
    def create_standard_query(self):
       """Create a standard query for non-incremental loads"""
       columns_aux = self.table_data.get('COLUMNS', '*')
       
       if self.table_data.get('ID_COLUMN', '') != '':
           columns_aux = f"{self.table_data['ID_COLUMN']} as id," + self.table_data.get('COLUMNS', '*')
       
       query = f"select {columns_aux} from {self.table_data.get('SOURCE_SCHEMA', 'CAN NOT FIND SCHEMA NAME')}.{self.table_data.get('SOURCE_TABLE', 'CAN NOT FIND TABLE NAME')} {self.table_data.get('JOIN_EXPR', '')} "
       
       self.logger.info(f"=== COMPLETE QUERY ===")
       self.logger.info(query)
       self.logger.info(f"=== END COMPLETE QUERY ===")
       
       if self.table_data.get('FILTER_EXP', '').strip() != '' or self.table_data.get('FILTER_COLUMN', '').strip() != '':
           if self.table_data.get('LOAD_TYPE', 'full') == 'full':
               FILTER_COLUMN = '0=0'
           else:
               # Clean DELAY_INCREMENTAL_INI from ' character
               clean_delay_incremental_ini = self.table_data.get('DELAY_INCREMENTAL_INI', '-2').strip().replace("'", "")

               lower_limit, upper_limit = self.get_limits_for_filter(
                   clean_delay_incremental_ini,
                   self.table_data.get('FILTER_DATA_TYPE', ""))
               # Remove double quotes from FILTER_COLUMN
               FILTER_COLUMN = self.table_data.get('FILTER_COLUMN', '1=1').replace('{0}', lower_limit).replace('{1}', upper_limit).replace('"', '')

           # Remove double quotes from FILTER_EXP
           if self.table_data.get('FILTER_EXP', '').strip() != '':
               FILTER_EXP = self.table_data['FILTER_EXP'].replace('"', '')
           else:
               FILTER_EXP = '0=0'

           query += f'where {FILTER_EXP} AND {FILTER_COLUMN}'
           
       self.logger.info(f"=== FINAL COMPLETE QUERY WITH FILTERS ===")
       self.logger.info(query)
       self.logger.info(f"=== END FINAL COMPLETE QUERY ===")
       return query
   
    def determine_load_strategy(self):
       """Determine the load strategy based on table configuration"""
       number_threads = 1
       incremental_load = False

       load_type = self.table_data.get('LOAD_TYPE', '').strip().lower()
       table_type = self.table_data.get('SOURCE_TABLE_TYPE', '')
       partition_column = self.table_data.get('PARTITION_COLUMN', '').strip()

       # Full load with partitioning
       if load_type == 'full' and table_type == 't' and partition_column != '':
           self.logger.info("Full load with partitioning based on min/max range")

           try:
               min_val, max_val = self.get_min_max_values(partition_column)

               if min_val is None or max_val is None:
                   self.logger.warning("MIN o MAX es None, cambiando a carga estándar.")
                   raise ValueError("No min/max")

               range_size = max_val - min_val
               number_threads = 10  # Reduced for ETL job to avoid resource issues

               if range_size < number_threads:
                   number_threads = max(1, range_size)
                   self.logger.info(f"Reduciendo número de particiones a {number_threads} (rango: {range_size})")

               increment = max(1, range_size // number_threads)

               self.logger.info(
                   f"Partition column: {partition_column}, Min: {min_val}, Max: {max_val}, "
                   f"Range: {range_size}, Increment: {increment}, Partitions: {number_threads}"
               )

               return {
                   'load_type': 'partitioned_full',
                   'number_threads': number_threads,
                   'incremental_load': True,
                   'partition_column': partition_column,
                   'min_val': min_val,
                   'max_val': max_val,
                   'increment': increment
               }

           except Exception as e:
               self.logger.warning(f"No se pudo determinar min/max. Usando estrategia estándar. Motivo: {e}")

       # Incremental between-date load
       if load_type == 'between-date':
           self.logger.info("Incremental load with date range")

           start = self.table_data.get('START_VALUE', '').strip()
           end = self.table_data.get('END_VALUE', '').strip()

           if not start or not end:
               self.logger.warning("START_VALUE o END_VALUE no definidos. Usando estrategia estándar.")
               return {
                   'load_type': 'standard',
                   'number_threads': number_threads,
                   'incremental_load': incremental_load
               }

           try:
               number_threads = int(self.config.get('THREADS_FOR_INCREMENTAL_LOADS', 3))  # Reduced for ETL
           except ValueError:
               number_threads = 3
               self.logger.warning("THREADS_FOR_INCREMENTAL_LOADS inválido, usando 3 hilos.")

           start_dt = self.transform_to_dt(start)
           end_dt = self.transform_to_dt(end)
           delta = (end_dt - start_dt) / number_threads

           return {
               'load_type': 'between_date',
               'number_threads': number_threads,
               'incremental_load': True,
               'start_dt': start_dt,
               'end_dt': end_dt,
               'delta': delta
           }

       # Default case
       self.logger.info("Usando carga estándar")
       return {
           'load_type': 'standard',
           'number_threads': number_threads,
           'incremental_load': incremental_load
       }
   
    def get_data_spark(self, query, s3_raw_path, actual_thread, number_threads):
       """Get data from database using Spark and write to S3 as Parquet"""
       try:
           self.logger.info(f"Executing query for thread {actual_thread}: {query}")
           
           # Execute query using Spark JDBC
           df = self.execute_db_query_spark(query)
           
           # Check if DataFrame is empty
           if df.count() == 0:
               self.logger.info(f"No data returned for thread {actual_thread}, creating empty file")
               # Create empty DataFrame with schema if possible
               empty_df = self.spark.createDataFrame([], df.schema)
               filename = f"empty_data_thread_{actual_thread}"
               self.write_spark_dataframe_to_s3_parquet(empty_df, s3_raw_path, filename)
           else:
               # Remove duplicates
               df_deduped = df.dropDuplicates()
               
               # Write to S3 as Parquet
               filename = f"data_thread_{actual_thread}_{uuid.uuid4().hex[:8]}"
               self.write_spark_dataframe_to_s3_parquet(df_deduped, s3_raw_path, filename)
               
               self.logger.info(f"Successfully wrote {df_deduped.count()} records for thread {actual_thread}")
           
           self.logger.info(f"Finished thread {actual_thread}")
           
       except Exception as e:
           self.logger.error(f"Error in get_data_spark for thread {actual_thread}: {str(e)}")
           raise
   
    def extract_data(self):
       """Main method to extract data from source and load to S3 using Spark"""
       try:
           # Delete existing data in the target S3 path
           self.delete_from_target(self.bucket, self.day_route)
           
           # Determine load strategy
           load_strategy = self.determine_load_strategy()
           total_tasks = load_strategy['number_threads']
           incremental_load = load_strategy['incremental_load']
           
           self.logger.info(f"Processing {total_tasks} tasks using Spark")
           
           # Generate all queries upfront based on load strategy
           all_queries = []
           for i in range(total_tasks):
               if incremental_load:
                   if load_strategy['load_type'] == 'partitioned_full':
                       # Generate partitioned query for full load with partitioning
                       query = self.get_partitioned_query(
                           load_strategy['partition_column'],
                           load_strategy['min_val'],
                           load_strategy['increment'],
                           i,
                           total_tasks
                       )
                   else:
                       # Generate query for date-based incremental load
                       start_dt = load_strategy['start_dt']
                       delta = load_strategy['delta']
                       start_str = str(start_dt + delta * i)[:19]
                       end_str = str(start_dt + delta * (i + 1))[:19]
                       query = self.get_query_for_date_range(start_str, end_str)
               else:
                   # Generate standard query for non-incremental loads
                   query = self.create_standard_query()
               
               # Final query print for debugging
               self.logger.info(f"Final Query {i}: {query}")
               all_queries.append(query)
           
           # For ETL jobs, we'll process queries sequentially to avoid resource conflicts
           # Spark will handle the parallelization internally
           for i, query in enumerate(all_queries):
               self.logger.info(f"Processing query {i+1}/{total_tasks}")
               self.get_data_spark(query, self.s3_raw_path, i, total_tasks)
           
           self.logger.info(f"All {total_tasks} tasks completed successfully")
           
           # Log success
           self._log_success()
           
           return True
               
       except Exception as e:
           error_msg = str(e)
           self.logger.error(f"Error in extract_data: {error_msg}")
           self._log_error(error_msg)
           raise Exception(f"Failed to extract data: {error_msg}")

# Main execution for Glue ETL
if __name__ == "__main__":
   # Get job parameters
   args = getResolvedOptions(
       sys.argv, 
       ['S3_RAW_BUCKET', 'ARN_TOPIC_SUCCESS', 'PROJECT_NAME', 'TEAM', 'DATA_SOURCE', 
        'ENVIRONMENT', 'REGION', 'DYNAMO_LOGS_TABLE', 'ARN_TOPIC_FAILED', 'TABLE_NAME', 
        'TABLES_CSV_S3', 'CREDENTIALS_CSV_S3', 'COLUMNS_CSV_S3', 'ENDPOINT_NAME', 'JOB_NAME']
   )

   # Initialize job
   job.init(args['JOB_NAME'], args)

   # Make FORCE_FULL_LOAD optional with a default value of "false"
   try:
       force_full_load = args.get('FORCE_FULL_LOAD', 'false').lower() == 'true'
   except:
       force_full_load = False

   config = {
       "S3_RAW_BUCKET": args["S3_RAW_BUCKET"],
       "DYNAMO_LOGS_TABLE": args["DYNAMO_LOGS_TABLE"],
       "ENVIRONMENT": args["ENVIRONMENT"],
       "PROJECT_NAME": args["PROJECT_NAME"],
       "TEAM": args["TEAM"],
       "DATA_SOURCE": args["DATA_SOURCE"],
       "THREADS_FOR_INCREMENTAL_LOADS": 3,  # Reduced for ETL
       "TOPIC_ARN": args["ARN_TOPIC_FAILED"], 
       "REGION": args["REGION"],
       "TABLE_NAME": args["TABLE_NAME"],
       "TABLES_CSV_S3": args["TABLES_CSV_S3"],
       "CREDENTIALS_CSV_S3": args["CREDENTIALS_CSV_S3"],
       "COLUMNS_CSV_S3": args["COLUMNS_CSV_S3"],
       "ENDPOINT_NAME": args["ENDPOINT_NAME"],
       "FORCE_FULL_LOAD": force_full_load
   }

   logger = custom_logger(__name__)

   logger.info("=" * 80)
   logger.info("AWS Glue ETL Version: SQL Server Identifier Parsing Fix v2.0")
   logger.info(f"Table: {config['TABLE_NAME']}")
   logger.info(f"EndPoint: {config['ENDPOINT_NAME']}")
   logger.info("=" * 80)

   logger.info("Starting data extraction process with Spark")
   logger.info(f"Configuration: {config}")

   try:
       # Create extractor instance with Glue context
       logger.info("Creating extractor instance for ETL")
       extractor = DataExtractor(config, glueContext, spark)
       logger.info("Extractor instance created")
       
       # Run extraction
       success = extractor.extract_data()
       logger.info("Extraction completed")
       
       if success:
           logger.info(f"Successfully extracted data for table {config['TABLE_NAME']}") 
       else:
           logger.error(f"Failed to extract data for table {config['TABLE_NAME']}") 
           
   except Exception as e:
       logger.error(f"An error occurred during extraction: {str(e)}")
       raise e
   finally:
       # Commit the job
       job.commit()