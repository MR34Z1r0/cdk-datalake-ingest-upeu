# -*- coding: utf-8 -*-
import os
import boto3
from typing import Dict, Any, Optional

class Settings:
    """Central configuration management"""
    
    def __init__(self):
        # Determine if running in AWS or locally
        self.is_aws_glue = self._detect_aws_glue()
        self.is_aws_s3 = self._detect_aws_s3()
        
        # Load configuration
        self._config = self._load_configuration()
        
        # Setup AWS session after loading configuration
        self._setup_aws_session()
    
    def _detect_aws_glue(self) -> bool:
        """Detect if running in AWS Glue environment"""
        return 'AWS_EXECUTION_ENV' in os.environ or 'GLUE_VERSION' in os.environ
    
    def _detect_aws_s3(self) -> bool:
        """Detect if S3 should be used for config files"""
        return self.is_aws_glue or os.environ.get('USE_S3_CONFIG', 'false').lower() == 'true'
    
    def _setup_aws_session(self):
        """Setup AWS session with profile or default credentials"""
        try:
            # Get AWS configuration from settings
            region_name = self._config.get('REGION', 'us-east-1')
            profile_name = self._config.get('AWS_PROFILE')
            
            # Setup session based on environment
            if not self.is_aws_glue and profile_name:
                # Local development with profile
                boto3.setup_default_session(
                    profile_name=profile_name,
                    region_name=region_name
                )
                print(f"✅ AWS Session configured with profile: {profile_name}, region: {region_name}")
            elif not self.is_aws_glue:
                # Local development without profile (uses default credentials)
                boto3.setup_default_session(region_name=region_name)
                print(f"✅ AWS Session configured with default credentials, region: {region_name}")
            else:
                # AWS Glue environment - uses IAM role
                print(f"✅ AWS Glue environment detected, using IAM role, region: {region_name}")
                
        except Exception as e:
            print(f"⚠️ Warning: Could not setup AWS session: {e}")
            print("Continuing without AWS session setup...")
    
    def _load_configuration(self) -> Dict[str, Any]:
        """Load configuration based on environment"""
        if self.is_aws_glue:
            return self._load_glue_config()
        else:
            return self._load_local_config()
    
    def _load_glue_config(self) -> Dict[str, Any]:
        """Load configuration from AWS Glue job parameters"""
        try:
            from awsglue.utils import getResolvedOptions
            import sys
            
            args = getResolvedOptions(sys.argv, [
                'S3_RAW_BUCKET', 'PROJECT_NAME', 'TEAM', 'DATA_SOURCE', 
                'ENVIRONMENT', 'REGION', 'DYNAMO_LOGS_TABLE', 'TABLE_NAME',
                'TABLES_CSV_S3', 'CREDENTIALS_CSV_S3', 'COLUMNS_CSV_S3', 
                'ENDPOINT_NAME', 'ARN_TOPIC_FAILED'
            ])
            
            # Optional parameters
            force_full_load = args.get('FORCE_FULL_LOAD', 'false').lower() == 'true'
            max_threads = int(args.get('MAX_THREADS', '6'))
            chunk_size = int(args.get('CHUNK_SIZE', '1000000'))
            
            return {
                'S3_RAW_BUCKET': args['S3_RAW_BUCKET'],
                'PROJECT_NAME': args['PROJECT_NAME'],
                'TEAM': args['TEAM'],
                'DATA_SOURCE': args['DATA_SOURCE'],
                'ENVIRONMENT': args['ENVIRONMENT'],
                'REGION': args['REGION'],
                'DYNAMO_LOGS_TABLE': args['DYNAMO_LOGS_TABLE'],
                'TABLE_NAME': args['TABLE_NAME'],
                'TABLES_CSV_S3': args['TABLES_CSV_S3'],
                'CREDENTIALS_CSV_S3': args['CREDENTIALS_CSV_S3'],
                'COLUMNS_CSV_S3': args['COLUMNS_CSV_S3'],
                'ENDPOINT_NAME': args['ENDPOINT_NAME'],
                'TOPIC_ARN': args['ARN_TOPIC_FAILED'],
                'FORCE_FULL_LOAD': force_full_load,
                'MAX_THREADS': max_threads,
                'CHUNK_SIZE': chunk_size,
                'OUTPUT_FORMAT': args.get('OUTPUT_FORMAT', 'parquet'),
                'EXTRACTOR_TYPE': args.get('EXTRACTOR_TYPE', 'sqlserver'),
                'LOADER_TYPE': args.get('LOADER_TYPE', 's3'),
                'MONITOR_TYPE': args.get('MONITOR_TYPE', 'dynamodb'),
                # AWS Profile not used in Glue
                'AWS_PROFILE': None
            }
            
        except Exception as e:
            raise RuntimeError(f"Failed to load AWS Glue configuration: {e}")
    
    def _load_local_config(self) -> Dict[str, Any]:
        """Load configuration for local development"""
        return {
            'S3_RAW_BUCKET': os.getenv('S3_RAW_BUCKET', 'sofia-dev-datalake-509399624591-us-east-1-raw-s3'),
            'PROJECT_NAME': os.getenv('PROJECT_NAME', 'datalake'),
            'TEAM': os.getenv('TEAM', 'genesis'),
            'DATA_SOURCE': os.getenv('DATA_SOURCE', 'upeu'),
            'ENVIRONMENT': os.getenv('ENVIRONMENT', 'DEV'),
            'REGION': os.getenv('REGION', 'us-east-1'),
            'DYNAMO_LOGS_TABLE': os.getenv('DYNAMO_LOGS_TABLE', 'sofia-dev-datalake-logs-ddb'),
            'TABLE_NAME': os.getenv('TABLE_NAME', ''),
            'TABLES_CSV_S3': os.getenv('TABLES_CSV_S3', '../../artifacts/configuration/csv/tables.csv'),
            'CREDENTIALS_CSV_S3': os.getenv('CREDENTIALS_CSV_S3', '../../artifacts/configuration/csv/credentials.csv'),
            'COLUMNS_CSV_S3': os.getenv('COLUMNS_CSV_S3', '../../artifacts/configuration/csv/columns.csv'),
            'ENDPOINT_NAME': os.getenv('ENDPOINT_NAME', 'PEUPEU'),
            'TOPIC_ARN': os.getenv('TOPIC_ARN', 'arn:aws:sns:us-east-1:509399624591:sofia-dev-datalake-failed-sns'),
            'FORCE_FULL_LOAD': os.getenv('FORCE_FULL_LOAD', 'false').lower() == 'true',
            'MAX_THREADS': int(os.getenv('MAX_THREADS', '6')),
            'CHUNK_SIZE': int(os.getenv('CHUNK_SIZE', '1000000')),
            'OUTPUT_FORMAT': os.getenv('OUTPUT_FORMAT', 'parquet'),
            'EXTRACTOR_TYPE': os.getenv('EXTRACTOR_TYPE', 'sqlserver'),
            'LOADER_TYPE': os.getenv('LOADER_TYPE', 's3'),
            'MONITOR_TYPE': os.getenv('MONITOR_TYPE', 'dynamodb'),
            # AWS Profile configuration
            'AWS_PROFILE': os.getenv('AWS_PROFILE', 'prd-upeu-admin')  # Default profile
        }
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value"""
        return self._config.get(key, default)
    
    def get_all(self) -> Dict[str, Any]:
        """Get all configuration"""
        return self._config.copy()
    
    def update(self, updates: Dict[str, Any]):
        """Update configuration"""
        self._config.update(updates)

# Global settings instance
settings = Settings()