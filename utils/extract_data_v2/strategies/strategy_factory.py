# strategies/strategy_factory_v2.py
from typing import Optional
from .base.extraction_strategy import ExtractionStrategy
from .base.strategy_types import ExtractionStrategyType
from .registry.strategy_registry import StrategyRegistry
from interfaces.strategy_interface import StrategyInterface
from models.table_config import TableConfig
from models.extraction_config import ExtractionConfig
from exceptions.custom_exceptions import ConfigurationError
from aje_libs.common.logger import custom_logger

logger = custom_logger(__name__)

class StrategyFactory:
    """Factory simplificado para crear estrategias de extracción"""
    
    @classmethod
    def create(cls, table_config: TableConfig, extraction_config: ExtractionConfig) -> StrategyInterface:
        """Crea la estrategia apropiada basada en configuración"""
        
        logger.info(f"=== STRATEGY FACTORY V2 ===")
        logger.info(f"Table: {extraction_config.table_name}")
        
        # Determinar tipo de estrategia
        strategy_type = cls._determine_strategy_type(table_config, extraction_config)
        logger.info(f"Determined strategy type: {strategy_type.value}")
        
        # Verificar que esté registrada
        if not StrategyRegistry.is_registered(strategy_type):
            available = [st.value for st in StrategyRegistry.get_available_strategies()]
            error_msg = f"Strategy type '{strategy_type.value}' not registered. Available: {available}"
            logger.error(error_msg)
            raise ConfigurationError(error_msg)
        
        # Crear instancia de la nueva estrategia
        strategy_class = StrategyRegistry.get_strategy_class(strategy_type)
        logger.info(f"Creating strategy instance: {strategy_class.__name__}")
        
        new_strategy = strategy_class(table_config, extraction_config)
        
        # Validar configuración
        if not new_strategy.validate_and_cache():
            error_msg = f"Strategy validation failed for {strategy_class.__name__}"
            logger.error(error_msg)
            raise ConfigurationError(error_msg)
        
        # Envolver en adaptador para compatibilidad
        from .adapters.strategy_adapter import StrategyAdapter
        strategy_adapter = StrategyAdapter(new_strategy)
        
        logger.info(f"Strategy instance created and wrapped in adapter successfully")
        logger.info("=== END STRATEGY FACTORY V2 ===")
        
        return strategy_adapter
    
    @classmethod
    def _determine_strategy_type(cls, table_config: TableConfig, 
                               extraction_config: ExtractionConfig) -> ExtractionStrategyType:
        """Determina qué estrategia usar basada en configuración (lógica simplificada)"""
        
        # Force full load override
        if extraction_config.force_full_load:
            logger.info("Force full load enabled - using FULL_LOAD strategy")
            return ExtractionStrategyType.FULL_LOAD
        
        # Usar load_type de configuración
        load_type = table_config.load_type.lower().strip() if table_config.load_type else 'full'
        logger.info(f"Load type from config: '{load_type}'")
        
        try:
            strategy_type = ExtractionStrategyType.from_string(load_type)
            logger.info(f"Mapped to strategy type: {strategy_type.value}")
            return strategy_type
        except ValueError as e:
            logger.warning(f"Could not map load_type '{load_type}': {e}")
            logger.info("Defaulting to FULL_LOAD strategy")
            return ExtractionStrategyType.FULL_LOAD
    
    @classmethod
    def get_supported_strategies(cls) -> list:
        """Obtiene lista de estrategias soportadas"""
        return [st.value for st in StrategyRegistry.get_available_strategies()]