# -*- coding: utf-8 -*-
from typing import List, Dict, Any
from base_strategy import BaseStrategy
from utils.date_utils import get_date_limits
from exceptions.custom_exceptions import ConfigurationError

class IncrementalStrategy(BaseStrategy):
    """Strategy for incremental data extraction"""
    
    def generate_queries(self) -> List[Dict[str, Any]]:
        """Generate query for incremental load"""
        
        if not self.validate_config():
            raise ConfigurationError("Invalid configuration for incremental strategy")
        
        # Build filter WHERE clause
        filter_where = self._build_incremental_filter()
        
        # Generate the main query
        query = self.query_builder.build_standard_query(filter_where)
        
        chunking_params = self.get_chunking_params()
        
        return [{
            'query': query,
            'thread_id': 0,
            'metadata': {
                'strategy': 'incremental',
                'table_name': self.extraction_config.table_name,
                'destination_path': self._build_s3_path(),
                'chunking_params': chunking_params,
                'filter_applied': filter_where
            }
        }]
    
    def get_strategy_name(self) -> str:
        return "incremental"
    
    def validate_config(self) -> bool:
        """Validate configuration for incremental load"""
        # Incremental load requires filter configuration
        required_fields = [
            self.table_config.stage_table_name,
            self.table_config.source_schema,
            self.table_config.source_table,
            self.table_config.columns
        ]
        
        if not all(field and str(field).strip() for field in required_fields):
            return False
        
        # Must have filter column or delay configuration
        has_filter_column = (self.table_config.filter_column and 
                           self.table_config.filter_column.strip())
        has_delay_config = (self.table_config.delay_incremental_ini and 
                          self.table_config.delay_incremental_ini.strip())
        
        return has_filter_column or has_delay_config
    
    def estimate_resources(self) -> Dict[str, Any]:
        """Estimate resources for incremental load"""
        base_estimate = super().estimate_resources()
        
        # Incremental loads are typically smaller
        base_estimate.update({
            'estimated_memory_mb': 300,
            'supports_chunking': self.should_use_chunking(),
            'parallel_safe': True
        })
        
        return base_estimate
    
    def _build_incremental_filter(self) -> str:
        """Build WHERE clause for incremental filtering"""
        where_conditions = []
        
        # Handle delay-based incremental filtering
        if (self.table_config.delay_incremental_ini and 
            self.table_config.delay_incremental_ini.strip()):
            
            delay_filter = self._build_delay_filter()
            if delay_filter:
                where_conditions.append(delay_filter)
        
        if not where_conditions:
            # Fallback: use a default incremental filter
            where_conditions.append("1=1")  # This should be customized based on your needs
        
        return ' AND '.join(where_conditions)
    
    def _build_delay_filter(self) -> str:
        """Build delay-based filter condition"""
        try:
            # Clean delay value
            clean_delay = self.table_config.delay_incremental_ini.strip().replace("'", "")
            
            # Get date limits
            lower_limit, upper_limit = get_date_limits(
                clean_delay,
                self.table_config.filter_data_type or ""
            )
            
            # Build filter condition
            if self.table_config.filter_column and self.table_config.filter_column.strip():
                filter_condition = self.table_config.filter_column.replace(
                    '{0}', lower_limit
                ).replace(
                    '{1}', upper_limit
                ).replace('"', '')
                
                return filter_condition
            
            return None
            
        except Exception:
            return None