# load/watermark_storage/dynamodb_watermark_storage.py
from interfaces.watermark_interface import WatermarkStorageInterface
from aje_libs.common.helpers.dynamodb_helper import DynamoDBHelper
from utils.date_utils import get_current_lima_time
from aje_libs.common.logger import custom_logger
from typing import Optional, Dict, Any, List
import json
from datetime import timedelta

class DynamoDBWatermarkStorage(WatermarkStorageInterface):
    """Implementación DynamoDB para watermarks"""
    
    def __init__(self, table_name: str, project_name: str):
        self.logger = custom_logger(__name__)
        self.table_name = table_name
        self.project_name = project_name
        self.dynamo_helper = DynamoDBHelper(
            table_name=table_name,
            pk_name="WATERMARK_KEY",
            sk_name="TIMESTAMP"
        )
    
    def get_last_extracted_value(self, table_name: str, column_name: str) -> Optional[str]:
        """Obtiene el último watermark de DynamoDB"""
        try:
            watermark_key = f"{self.project_name}#{table_name}#{column_name}"
            
            # Query para obtener el más reciente
            response = self.dynamo_helper.query_table(
                key_condition=f"WATERMARK_KEY = '{watermark_key}'",
                limit=1,
                scan_forward=False  # Más reciente primero
            )
            
            if response:
                return response[0].get('EXTRACTED_VALUE')
            
            return None
            
        except Exception as e: 
            self.logger.warning(f"Failed to read watermark from Dynamodb: {e}")
            return None
    
    def set_last_extracted_value(self, table_name: str, column_name: str, 
                               value: str, metadata: Dict[str, Any] = None) -> bool:
        """Guarda watermark en DynamoDB"""
        try:
            now = get_current_lima_time()
            watermark_key = f"{self.project_name}#{table_name}#{column_name}"
            
            watermark_entry = {
                'WATERMARK_KEY': watermark_key,
                'TIMESTAMP': now.strftime('%Y-%m-%d %H:%M:%S'),
                'TABLE_NAME': table_name,
                'COLUMN_NAME': column_name,
                'EXTRACTED_VALUE': str(value),
                'PROJECT_NAME': self.project_name,
                'METADATA': json.dumps(metadata or {}),
                'TTL': int((now + timedelta(days=90)).timestamp())  # Auto cleanup
            }
            
            self.dynamo_helper.put_item(watermark_entry)
            return True
            
        except Exception as e: 
            self.logger.error(f"Failed to save watermark to Dynamodb: {e}")
            return False

    def get_extraction_history(self, table_name: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Obtiene el historial de extracciones"""
        try:
            watermark_key = f"{self.project_name}#{table_name}#*"
            
            # Query para obtener historial
            response = self.dynamo_helper.scan_table(
                filter_expression="begins_with(WATERMARK_KEY, :pk)",
                expression_attribute_values={':pk': f"{self.project_name}#{table_name}#"},
                limit=limit
            )
            
            # Formatear respuesta
            history = []
            for item in response:
                history.append({
                    'table_name': item.get('TABLE_NAME'),
                    'column_name': item.get('COLUMN_NAME'),
                    'extracted_value': item.get('EXTRACTED_VALUE'),
                    'timestamp': item.get('TIMESTAMP'),
                    'metadata': json.loads(item.get('METADATA', '{}'))
                })
            
            # Ordenar por timestamp
            history.sort(key=lambda x: x['timestamp'], reverse=True)
            return history[:limit]
            
        except Exception as e:
            self.logger.warning(f"Failed to read extraction history from DynamoDB: {e}")
            return []

    def cleanup_old_watermarks(self, days_to_keep: int = 90) -> int:
        """Limpia watermarks antiguos usando TTL"""
        # En DynamoDB, esto se maneja automáticamente con TTL
        # Solo retornamos 0 porque el cleanup es automático
        return 0