# -*- coding: utf-8 -*-
from abc import ABC, abstractmethod
from typing import List, Dict, Any
from models.table_config import TableConfig
from models.extraction_config import ExtractionConfig
from extract.query_builder import QueryBuilder

class BaseStrategy(ABC):
    """Base class for all extraction strategies"""
    
    def __init__(self, table_config: TableConfig, extraction_config: ExtractionConfig):
        self.table_config = table_config
        self.extraction_config = extraction_config
        self.query_builder = QueryBuilder(table_config)
    
    @abstractmethod
    def generate_queries(self) -> List[Dict[str, Any]]:
        """
        Generate list of queries to execute
        Returns: List of dicts with keys:
            - 'query': SQL query string
            - 'thread_id': Thread identifier
            - 'metadata': Additional metadata for the query
        """
        pass
    
    @abstractmethod
    def get_strategy_name(self) -> str:
        """Get strategy name for logging"""
        pass
    
    @abstractmethod
    def validate_config(self) -> bool:
        """Validate if configuration is valid for this strategy"""
        pass
    
    def estimate_resources(self) -> Dict[str, Any]:
        """Estimate resources needed (threads, memory, etc.)"""
        return {
            'estimated_threads': 1,
            'estimated_memory_mb': 500,
            'supports_chunking': False,
            'parallel_safe': True
        }
    
    def should_use_chunking(self) -> bool:
        """Determine if chunking should be used"""
        # Use chunking for large tables with partition column
        return (
            hasattr(self.table_config, 'partition_column') and 
            self.table_config.partition_column and 
            self.table_config.partition_column.strip() != '' and
            self.table_config.source_table_type == 't'
        )
    
    def get_chunking_params(self) -> Dict[str, Any]:
        """Get parameters for chunked extraction"""
        if not self.should_use_chunking():
            return {}
        
        return {
            'chunk_size': self.extraction_config.chunk_size,
            'order_by': self.table_config.partition_column or self.table_config.id_column
        }
    
    def _build_s3_path(self) -> str:
        """Build S3 destination path"""
        from utils.date_utils import get_date_parts
        
        year, month, day = get_date_parts()
        
        # Get clean table name (remove alias)
        clean_table_name = self._get_clean_table_name()
        
        return f"{self.extraction_config.team}/{self.extraction_config.data_source}/{self.extraction_config.endpoint_name}/{clean_table_name}/year={year}/month={month}/day={day}/"
    
    def _get_clean_table_name(self) -> str:
        """Extract clean table name from SOURCE_TABLE, removing alias after space"""
        source_table = self.table_config.source_table or self.extraction_config.table_name
        # Split by space and take only the first part (table name)
        clean_name = source_table.split()[0] if source_table and ' ' in source_table else source_table
        return clean_name
    
    def _apply_load_type_override(self):
        """Apply force full load override if configured"""
        if (self.extraction_config.force_full_load and 
            self.table_config.load_type.lower() == 'incremental'):
            self.table_config.load_type = 'full'