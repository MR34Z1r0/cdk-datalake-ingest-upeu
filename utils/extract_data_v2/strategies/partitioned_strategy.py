# -*- coding: utf-8 -*-
from typing import List, Dict, Any, Tuple
from .base_strategy import BaseStrategy
from ..exceptions.custom_exceptions import ConfigurationError

class PartitionedStrategy(BaseStrategy):
    """Strategy for partitioned data extraction based on min/max range"""
    
    def __init__(self, table_config, extraction_config, extractor=None):
        super().__init__(table_config, extraction_config)
        self.extractor = extractor  # Needed to get min/max values
    
    def generate_queries(self) -> List[Dict[str, Any]]:
        """Generate queries for partitioned extraction"""
        
        if not self.validate_config():
            raise ConfigurationError("Invalid configuration for partitioned strategy")
        
        if not self.extractor:
            raise ConfigurationError("Extractor required for partitioned strategy to get min/max values")
        
        # Get partition parameters
        partition_column = self.table_config.partition_column
        num_partitions = min(self.extraction_config.max_threads, 30)
        
        # Get min/max values
        min_val, max_val = self._get_min_max_values(partition_column)
        
        if min_val is None or max_val is None:
            raise ConfigurationError(f"Could not determine min/max values for partition column {partition_column}")
        
        # Calculate partitioning parameters
        range_size = max_val - min_val
        
        # Adjust number of partitions if range is small
        if range_size < num_partitions:
            num_partitions = max(1, range_size)
        
        increment = max(1, range_size // num_partitions)
        
        queries = []
        destination_path = self._build_s3_path()
        chunking_params = self.get_chunking_params()
        
        for i in range(num_partitions):
            start_value, end_value = self._calculate_partition_range(
                min_val, increment, i, num_partitions
            )
            
            # Build partitioned query
            query = self.query_builder.build_partitioned_query(
                partition_column, start_value, end_value
            )
            
            queries.append({
                'query': query,
                'thread_id': i,
                'metadata': {
                    'strategy': 'partitioned',
                    'table_name': self.extraction_config.table_name,
                    'destination_path': destination_path,
                    'partition_info': {
                        'column': partition_column,
                        'start_value': start_value,
                        'end_value': end_value,
                        'min_val': min_val,
                        'max_val': max_val,
                        'thread_id': i,
                        'total_threads': num_partitions
                    },
                    'chunking_params': chunking_params
                }
            })
        
        return queries
    
    def get_strategy_name(self) -> str:
        return "partitioned"
    
    def validate_config(self) -> bool:
        """Validate configuration for partitioned strategy"""
        # Partitioned strategy requires partition column
        required_fields = [
            self.table_config.stage_table_name,
            self.table_config.source_schema,
            self.table_config.source_table,
            self.table_config.columns,
            self.table_config.partition_column
        ]
        
        if not all(field and str(field).strip() for field in required_fields):
            return False
        
        # Should be a table type that supports partitioning
        return self.table_config.source_table_type == 't'
    
    def estimate_resources(self) -> Dict[str, Any]:
        """Estimate resources for partitioned strategy"""
        num_partitions = min(self.extraction_config.max_threads, 30)
        
        return {
            'estimated_threads': num_partitions,
            'estimated_memory_mb': 300 * num_partitions,
            'supports_chunking': self.should_use_chunking(),
            'parallel_safe': True
        }
    
    def _get_min_max_values(self, partition_column: str) -> Tuple[int, int]:
        """Get min and max values for partition column"""
        try:
            full_table_name = f"{self.table_config.source_schema}.{self.table_config.source_table}"
            
            # Build additional where clause from filter expression
            additional_where = None
            if self.table_config.filter_exp and self.table_config.filter_exp.strip():
                additional_where = self.table_config.filter_exp.replace('"', '')
            
            return self.extractor.get_min_max_values(
                full_table_name, 
                partition_column, 
                additional_where
            )
            
        except Exception as e:
            raise ConfigurationError(f"Failed to get min/max values: {e}")
    
    def _calculate_partition_range(self, min_val: int, increment: int, 
                                 partition_index: int, total_partitions: int) -> Tuple[int, int]:
        """Calculate start and end values for a partition"""
        start_value = int(min_val + (increment * partition_index))
        
        # For the last partition, extend to ensure we capture all data
        if partition_index == total_partitions - 1:
            end_value = int(min_val + (increment * (partition_index + 1))) + 1
        else:
            end_value = int(min_val + (increment * (partition_index + 1)))
        
        return start_value, end_value