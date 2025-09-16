# -*- coding: utf-8 -*-
from typing import List, Dict, Any
from datetime import datetime, timedelta
from strategies.strategy.base_strategy import BaseStrategy
from utils.date_utils import transform_to_datetime
from exceptions.custom_exceptions import ConfigurationError

class DateRangeStrategy(BaseStrategy):
    """Strategy for date range based extraction with parallel processing"""
    
    def generate_queries(self) -> List[Dict[str, Any]]:
        """Generate queries for date range extraction"""
        
        if not self.validate_config():
            raise ConfigurationError("Invalid configuration for date range strategy")
        
        # Get date range parameters
        start_dt = transform_to_datetime(self.table_config.start_value)
        end_dt = transform_to_datetime(self.table_config.end_value)
        
        # Determine number of threads
        num_threads = min(self.extraction_config.max_threads, 6)
        
        # Calculate time delta per thread
        total_duration = end_dt - start_dt
        delta_per_thread = total_duration / num_threads
        
        queries = []
        destination_path = self._build_s3_path()
        
        for i in range(num_threads):
            thread_start = start_dt + (delta_per_thread * i)
            thread_end = start_dt + (delta_per_thread * (i + 1))
            
            # Format dates as strings
            start_str = thread_start.strftime('%Y-%m-%d %H:%M:%S')
            end_str = thread_end.strftime('%Y-%m-%d %H:%M:%S')
            
            # Build query for this date range
            query = self._build_date_range_query(start_str, end_str)
            
            queries.append({
                'query': query,
                'thread_id': i,
                'metadata': {
                    'strategy': 'date_range',
                    'table_name': self.extraction_config.table_name,
                    'destination_path': destination_path,
                    'date_range': {
                        'start': start_str,
                        'end': end_str
                    },
                    'thread_info': {
                        'thread_id': i,
                        'total_threads': num_threads
                    }
                }
            })
        
        return queries
    
    def get_strategy_name(self) -> str:
        return "date_range"
    
    def validate_config(self) -> bool:
        """Validate configuration for date range strategy"""
        # Date range requires start/end values and filter column
        required_fields = [
            self.table_config.stage_table_name,
            self.table_config.source_schema,
            self.table_config.source_table,
            self.table_config.columns,
            self.table_config.start_value,
            self.table_config.end_value,
            self.table_config.filter_column
        ]
        
        if not all(field and str(field).strip() for field in required_fields):
            return False
        
        # Validate date format
        try:
            start_dt = transform_to_datetime(self.table_config.start_value)
            end_dt = transform_to_datetime(self.table_config.end_value)
            return end_dt > start_dt
        except Exception:
            return False
    
    def estimate_resources(self) -> Dict[str, Any]:
        """Estimate resources for date range strategy"""
        num_threads = min(self.extraction_config.max_threads, 6)
        
        return {
            'estimated_threads': num_threads,
            'estimated_memory_mb': 200 * num_threads,
            'supports_chunking': False,  # Date range already provides parallelism
            'parallel_safe': True
        }
    
    def _build_date_range_query(self, start_date: str, end_date: str) -> str:
        """Build query for specific date range"""
        from utils.date_utils import format_date_for_db
        
        # Get filter column and type
        filter_column = self.table_config.filter_column.strip()
        filter_type = getattr(self.table_config, 'filter_data_type', None)
        
        # Format dates based on database type
        if filter_type:
            formatted_start = format_date_for_db(start_date, filter_type)
            formatted_end = format_date_for_db(end_date, filter_type)
        else:
            formatted_start = f"'{start_date}'"
            formatted_end = f"'{end_date}'"
        
        # Build date range condition
        date_condition = self._build_date_condition(filter_column, formatted_start, formatted_end)
        
        # Use query builder with date condition
        return self.query_builder.build_standard_query(date_condition)
    
    def _build_date_condition(self, filter_column: str, start_date: str, end_date: str) -> str:
        """Build date range condition supporting multiple columns"""
        
        # Handle multiple date columns (comma-separated)
        if ',' in filter_column:
            date_columns = [col.strip() for col in filter_column.split(',')]
            conditions = []
            
            for col in date_columns:
                conditions.append(
                    f"({col} IS NOT NULL AND {col} BETWEEN {start_date} AND {end_date})"
                )
            
            return f"({' OR '.join(conditions)})"
        else:
            # Single date column
            return f"{filter_column} IS NOT NULL AND {filter_column} BETWEEN {start_date} AND {end_date}"