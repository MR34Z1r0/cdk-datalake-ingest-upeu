# strategies/implementations/incremental.py
from typing import List
from ..base.extraction_strategy import ExtractionStrategy
from ..base.extraction_params import ExtractionParams
from ..base.strategy_types import ExtractionStrategyType
from aje_libs.common.logger import custom_logger

logger = custom_logger(__name__)

class IncrementalStrategy(ExtractionStrategy):
    """Estrategia para carga incremental basada en columnas de control"""
    
    def get_strategy_type(self) -> ExtractionStrategyType:
        return ExtractionStrategyType.INCREMENTAL
    
    def build_extraction_params(self) -> ExtractionParams:
        """Construye parámetros para carga incremental"""
        logger.info(f"=== INCREMENTAL STRATEGY - Building Params ===")
        logger.info(f"Table: {self.extraction_config.table_name}")
        
        # Crear parámetros básicos
        params = ExtractionParams(
            table_name=self._get_source_table_name(),
            columns=self._parse_columns(),
            metadata=self._build_basic_metadata()
        )
        
        # Agregar filtros incrementales
        incremental_filters = self._build_incremental_filters_with_watermark()
        for filter_condition in incremental_filters:
            params.add_where_condition(filter_condition)
        
        # Configurar chunking si es apropiado
        if self._should_use_chunking():
            params.chunk_size = self.extraction_config.chunk_size
            params.chunk_column = self._get_chunking_column()
            logger.info(f"Chunking enabled - Size: {params.chunk_size}, Column: {params.chunk_column}")
        
        logger.info(f"Incremental extraction params built - Columns: {len(params.columns)}, Where conditions: {len(params.where_conditions)}")
        logger.info("=== END INCREMENTAL STRATEGY ===")
        
        return params
    
    def validate(self) -> bool:
        """Valida configuración para carga incremental"""
        logger.info("=== INCREMENTAL STRATEGY VALIDATION ===")
        
        # Campos requeridos básicos
        required_fields = [
            ('stage_table_name', self.table_config.stage_table_name),
            ('source_schema', self.table_config.source_schema),
            ('source_table', self.table_config.source_table),
            ('columns', self.table_config.columns)
        ]
        
        validation_errors = []
        for field_name, field_value in required_fields:
            logger.info(f"Checking {field_name}: '{field_value}'")
            
            if field_value is None:
                validation_errors.append(f"{field_name} is None")
            elif not str(field_value).strip():
                validation_errors.append(f"{field_name} is empty")
            else:
                logger.info(f"  ✅ {field_name} is valid")
        
        # Validaciones específicas para incremental
        incremental_errors = self._validate_incremental_config()
        validation_errors.extend(incremental_errors)
        
        if validation_errors:
            logger.error("❌ VALIDATION FAILED:")
            for error in validation_errors:
                logger.error(f"  - {error}")
            return False
        
        logger.info("✅ ALL VALIDATION CHECKS PASSED")
        logger.info("=== END VALIDATION ===")
        return True
    
    def estimate_resources(self) -> dict:
        """Estima recursos para carga incremental"""
        base_estimate = super().estimate_resources()
        
        # Incrementales suelen ser más ligeros
        base_estimate.update({
            'estimated_memory_mb': 500,
            'supports_chunking': self._should_use_chunking(),
            'parallel_safe': True,
            'strategy_complexity': 'medium'
        })
        
        return base_estimate
    
    def _validate_incremental_config(self) -> List[str]:
        """Validaciones específicas para estrategia incremental"""
        errors = []
        
        # Debe tener al menos una columna de control incremental
        has_filter_column = (hasattr(self.table_config, 'filter_column') and 
                           self.table_config.filter_column and 
                           self.table_config.filter_column.strip())
        
        has_delay_config = (hasattr(self.table_config, 'delay_incremental_ini') and 
                          self.table_config.delay_incremental_ini and 
                          self.table_config.delay_incremental_ini.strip())
        
        has_partition_column = (hasattr(self.table_config, 'partition_column') and 
                              self.table_config.partition_column and 
                              self.table_config.partition_column.strip())
        
        if not (has_filter_column or has_partition_column):
            errors.append("Incremental strategy requires either filter_column or partition_column")
        
        if has_filter_column and not has_delay_config:
            errors.append("filter_column specified but delay_incremental_ini is missing")
        
        return errors
    
    def _build_incremental_filters(self) -> List[str]:
        """Construye filtros específicos para carga incremental"""
        filters = []
        
        # Filtro básico de la configuración
        if hasattr(self.table_config, 'filter_exp') and self.table_config.filter_exp:
            clean_filter = self.table_config.filter_exp.replace('"', '').strip()
            if clean_filter:
                filters.append(clean_filter)
        
        # Filtro incremental basado en fechas
        if (hasattr(self.table_config, 'filter_column') and 
            self.table_config.filter_column and 
            hasattr(self.table_config, 'delay_incremental_ini') and
            self.table_config.delay_incremental_ini):
            
            try:
                date_filter = self._build_incremental_date_filter()
                if date_filter:
                    filters.append(date_filter)
            except Exception as e:
                logger.warning(f"Could not build incremental date filter: {e}")
        
        # Filtro incremental basado en ID/sequence
        if (hasattr(self.table_config, 'partition_column') and 
            self.table_config.partition_column and
            hasattr(self.table_config, 'start_value') and
            self.table_config.start_value):
            
            try:
                id_filter = self._build_incremental_id_filter()
                if id_filter:
                    filters.append(id_filter)
            except Exception as e:
                logger.warning(f"Could not build incremental ID filter: {e}")
        
        return filters
    
    def _build_incremental_date_filter(self) -> str:
        """Construye filtro incremental basado en fechas"""
        try:
            from utils.date_utils import get_date_limits
            
            # Limpiar delay value
            clean_delay = self.table_config.delay_incremental_ini.strip().replace("'", "")
            
            # Obtener límites de fecha para incremental
            lower_limit, upper_limit = get_date_limits(
                clean_delay,
                getattr(self.table_config, 'filter_data_type', '') or ""
            )
            
            # Construir condición de filtro incremental
            filter_condition = self.table_config.filter_column.replace(
                '{0}', lower_limit
            ).replace(
                '{1}', upper_limit
            ).replace('"', '')
            
            logger.info(f"Built incremental date filter: {filter_condition}")
            return filter_condition
            
        except Exception as e:
            logger.warning(f"Failed to build incremental date filter: {e}")
            return None
    
    def _build_incremental_filters_with_watermark(self) -> List[str]:
        """Construye filtros incrementales usando watermark storage"""
        filters = []
        
        # Filtro básico
        if hasattr(self.table_config, 'filter_exp') and self.table_config.filter_exp:
            clean_filter = self.table_config.filter_exp.replace('"', '').strip()
            if clean_filter:
                filters.append(clean_filter)
        
        # 🎯 USAR WATERMARK PARA INCREMENTAL
        if (hasattr(self.table_config, 'partition_column') and 
            self.table_config.partition_column and
            self.watermark_storage):  # 👈 VERIFICAR QUE EXISTE
            
            try:
                # Obtener último valor del watermark storage
                last_value = self.watermark_storage.get_last_extracted_value(
                    table_name=self.table_config.stage_table_name,
                    column_name=self.table_config.partition_column
                )
                
                if last_value:
                    logger.info(f"Found watermark for {self.table_config.stage_table_name}.{self.table_config.partition_column}: {last_value}")
                    
                    # Construir filtro con watermark
                    watermark_filter = f"{self.table_config.partition_column} > {last_value}"
                    filters.append(watermark_filter)
                    
                    logger.info(f"Applied watermark filter: {watermark_filter}")
                else:
                    logger.info(f"No watermark found for {self.table_config.stage_table_name}, will do full incremental")
                    
            except Exception as e:
                logger.warning(f"Failed to get watermark, continuing without: {e}")
        
        return filters

    def _build_incremental_id_filter(self) -> str:
        """Construye filtro incremental basado en ID"""
        try:
            partition_column = self.table_config.partition_column.strip()
            start_value = str(self.table_config.start_value).strip()
            
            # Construir filtro básico de ID mayor que valor inicial
            filter_condition = f"{partition_column} > {start_value}"
            
            # Si hay end_value, agregar límite superior
            if (hasattr(self.table_config, 'end_value') and 
                self.table_config.end_value and 
                str(self.table_config.end_value).strip()):
                
                end_value = str(self.table_config.end_value).strip()
                filter_condition += f" AND {partition_column} <= {end_value}"
            
            logger.info(f"Built incremental ID filter: {filter_condition}")
            return filter_condition
            
        except Exception as e:
            logger.warning(f"Failed to build incremental ID filter: {e}")
            return None
    
    def _should_use_chunking(self) -> bool:
        """Determina si debería usar chunking para incremental"""
        return (
            hasattr(self.table_config, 'partition_column') and 
            self.table_config.partition_column and 
            self.table_config.partition_column.strip() != '' and
            getattr(self.table_config, 'source_table_type', '') == 't'
        )
    
    def _get_chunking_column(self) -> str:
        """Obtiene la columna para chunking en incremental"""
        if hasattr(self.table_config, 'partition_column') and self.table_config.partition_column:
            return self.table_config.partition_column.strip()
        
        if hasattr(self.table_config, 'id_column') and self.table_config.id_column:
            return self.table_config.id_column.strip()
        
        return None