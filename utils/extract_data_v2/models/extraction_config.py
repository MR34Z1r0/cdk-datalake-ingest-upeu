# -*- coding: utf-8 -*-
from dataclasses import dataclass
from typing import Optional

@dataclass
class ExtractionConfig:
    """Main extraction configuration"""
    project_name: str
    team: str
    data_source: str
    endpoint_name: str
    environment: str
    table_name: str
    
    # Storage configuration
    s3_raw_bucket: Optional[str] = None
    local_path: Optional[str] = None
    
    # Monitoring configuration
    dynamo_logs_table: Optional[str] = None
    topic_arn: Optional[str] = None
    
    # Processing configuration
    max_threads: int = 6
    chunk_size: int = 1000000
    force_full_load: bool = False
    
    # File format
    output_format: str = "parquet"  # 'parquet', 'csv', 'json'
    
    def __post_init__(self):
        """Validate required fields"""
        if not all([self.project_name, self.team, self.data_source, 
                   self.endpoint_name, self.environment, self.table_name]):
            raise ValueError("All extraction configuration fields are required")