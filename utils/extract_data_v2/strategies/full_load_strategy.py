# -*- coding: utf-8 -*-
from typing import List, Dict, Any
from .base_strategy import BaseStrategy
from ..utils.date_utils import get_date_limits

class FullLoadStrategy(BaseStrategy):
    """Strategy for full data extraction"""
    
    def generate_queries(self) -> List[Dict[str, Any]]:
        """Generate query for full load"""
        
        # Apply any load type overrides
        self._apply_load_type_override()
        
        # Build additional WHERE clause for filtered full loads
        additional_where = self._build_filter_where_clause()
        
        # Generate the main query
        query = self.query_builder.build_standard_query(additional_where)
        
        chunking_params = self.get_chunking_params()
        
        return [{
            'query': query,
            'thread_id': 0,
            'metadata': {
                'strategy': 'full_load',
                'table_name': self.extraction_config.table_name,
                'destination_path': self._build_s3_path(),
                'chunking_params': chunking_params
            }
        }]
    
    def get_strategy_name(self) -> str:
        return "full_load"
    
    def validate_config(self) -> bool:
        """Validate configuration for full load"""
        # Full load requires minimal configuration
        required_fields = [
            self.table_config.stage_table_name,
            self.table_config.source_schema,
            self.table_config.source_table,
            self.table_config.columns
        ]
        
        return all(field and str(field).strip() for field in required_fields)
    
    def estimate_resources(self) -> Dict[str, Any]:
        """Estimate resources for full load"""
        base_estimate = super().estimate_resources()
        
        # Full loads can be resource intensive
        base_estimate.update({
            'estimated_memory_mb': 1000,
            'supports_chunking': self.should_use_chunking(),
            'parallel_safe': True
        })
        
        return base_estimate
    
    def _build_filter_where_clause(self) -> str:
        """Build WHERE clause for filtered full loads"""
        where_conditions = []
        
        # Add filter column conditions for incremental-like filtering
        if (self.table_config.filter_column and 
            self.table_config.filter_column.strip() and
            self.table_config.delay_incremental_ini):
            
            try:
                # Clean delay value
                clean_delay = self.table_config.delay_incremental_ini.strip().replace("'", "")
                
                # Get date limits
                lower_limit, upper_limit = get_date_limits(
                    clean_delay,
                    self.table_config.filter_data_type or ""
                )
                
                # Build filter condition
                filter_condition = self.table_config.filter_column.replace('{0}', lower_limit).replace('{1}', upper_limit).replace('"', '')
                where_conditions.append(filter_condition)
                
            except Exception:
                # If date filtering fails, continue without it
                pass
        
        return ' AND '.join(where_conditions) if where_conditions else None