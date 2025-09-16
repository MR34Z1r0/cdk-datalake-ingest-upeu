# -*- coding: utf-8 -*-
from typing import Dict, Type
from interfaces.strategy_interface import StrategyInterface
from models.table_config import TableConfig
from models.extraction_config import ExtractionConfig
from strategies.strategy.full_load_strategy import FullLoadStrategy
from strategies.strategy.incremental_strategy import IncrementalStrategy
from strategies.strategy.date_range_strategy import DateRangeStrategy
from strategies.strategy.partitioned_strategy import PartitionedStrategy
from exceptions.custom_exceptions import ConfigurationError

class StrategyFactory:
    """Factory to create appropriate extraction strategies"""
    
    _strategies: Dict[str, Type[StrategyInterface]] = {
        'full': FullLoadStrategy,
        'incremental': IncrementalStrategy,
        'date_range': DateRangeStrategy,
        'between-date': DateRangeStrategy,  # Alias for date_range
        'partitioned': PartitionedStrategy,
    }
    
    @classmethod
    def create(cls, table_config: TableConfig, extraction_config: ExtractionConfig, 
              extractor=None) -> StrategyInterface:
        """
        Create appropriate strategy based on load type
        
        Args:
            table_config: Table configuration
            extraction_config: Extraction configuration  
            extractor: Extractor instance (needed for some strategies)
            
        Returns:
            Configured strategy instance
            
        Raises:
            ConfigurationError: If strategy type is not supported
        """
        # Determine strategy based on configuration
        strategy_type = cls._determine_strategy_type(table_config, extraction_config)
        
        if strategy_type not in cls._strategies:
            available_strategies = ', '.join(cls._strategies.keys())
            raise ConfigurationError(
                f"Unsupported strategy type '{strategy_type}'. "
                f"Available strategies: {available_strategies}"
            )
        
        strategy_class = cls._strategies[strategy_type]
        
        # Some strategies need the extractor
        if strategy_type == 'partitioned':
            return strategy_class(table_config, extraction_config, extractor)
        else:
            return strategy_class(table_config, extraction_config)
    
    @classmethod
    def _determine_strategy_type(cls, table_config: TableConfig, 
                               extraction_config: ExtractionConfig) -> str:
        """Determine which strategy to use based on configuration"""
        
        load_type = table_config.load_type.lower().strip()
        
        # Handle force full load override
        if extraction_config.force_full_load and load_type == 'incremental':
            load_type = 'full'
        
        # Determine if partitioned strategy should be used for full loads
        if (load_type == 'full' and 
            table_config.source_table_type == 't' and
            table_config.partition_column and 
            table_config.partition_column.strip()):
            return 'partitioned'
        
        # Map load types to strategies
        strategy_mapping = {
            'full': 'full',
            'incremental': 'incremental', 
            'between-date': 'date_range',
            'date_range': 'date_range'
        }
        
        return strategy_mapping.get(load_type, 'full')
    
    @classmethod
    def register_strategy(cls, strategy_type: str, strategy_class: Type[StrategyInterface]):
        """Register a new strategy type"""
        cls._strategies[strategy_type.lower()] = strategy_class
    
    @classmethod
    def get_supported_strategies(cls) -> list:
        """Get list of supported strategy types"""
        return list(cls._strategies.keys())