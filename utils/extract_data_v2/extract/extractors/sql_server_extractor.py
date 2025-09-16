# -*- coding: utf-8 -*-
import pymssql
import pandas as pd
from typing import Optional, Tuple, Iterator, Dict, Any
from interfaces.extractor_interface import ExtractorInterface
from models.database_config import DatabaseConfig
from exceptions.custom_exceptions import ConnectionError, ExtractionError
from aje_libs.common.helpers.secrets_helper import SecretsHelper

class SQLServerExtractor(ExtractorInterface):
    """SQL Server implementation of ExtractorInterface"""
    
    def __init__(self, config: DatabaseConfig):
        super().__init__(config)
        self.connection = None
        self._secrets_helper = None
        self._password = None
    
    def connect(self):
        """Establish connection to SQL Server"""
        try:
            # Get password from secrets manager
            if not self._password:
                self._get_password()
            
            self.connection = pymssql.connect(
                server=self.config.server,
                user=self.config.username,
                password=self._password,
                database=self.config.database,
                port=self.config.port or 1433,
                timeout=900,  # 15 minutes
                login_timeout=900,
                charset='utf8'
            )
            
        except Exception as e:
            raise ConnectionError(f"Failed to connect to SQL Server: {e}")
    
    def test_connection(self) -> bool:
        """Test connection to SQL Server"""
        try:
            if not self.connection:
                self.connect()
            
            cursor = self.connection.cursor()
            cursor.execute("SELECT 1 as test")
            result = cursor.fetchone()
            cursor.close()
            
            return result is not None
            
        except Exception:
            return False
    
    def execute_query(self, query: str, params: Optional[Tuple] = None) -> pd.DataFrame:
        """Execute query and return DataFrame"""
        try:
            if not self.connection:
                self.connect()
            
            if params:
                df = pd.read_sql(query, self.connection, params=params)
            else:
                df = pd.read_sql(query, self.connection)
            
            # Fix duplicate column names
            df = self._fix_duplicate_columns(df)
            
            return df
            
        except Exception as e:
            raise ExtractionError(f"Failed to execute query: {e}")
    
    def execute_query_chunked(self, query: str, chunk_size: int, 
                            order_by: str, params: Optional[Tuple] = None) -> Iterator[pd.DataFrame]:
        """Execute query with chunked results using OFFSET/FETCH"""
        try:
            if not self.connection:
                self.connect()
            
            offset = 0
            while True:
                # Add ORDER BY and OFFSET/FETCH to the query
                chunked_query = f"{query.rstrip().rstrip(';')} ORDER BY {order_by} OFFSET {offset} ROWS FETCH NEXT {chunk_size} ROWS ONLY"
                
                if params:
                    df = pd.read_sql(chunked_query, self.connection, params=params)
                else:
                    df = pd.read_sql(chunked_query, self.connection)
                
                if df.empty:
                    break
                
                # Fix duplicate column names
                df = self._fix_duplicate_columns(df)
                
                yield df
                offset += chunk_size
                
        except Exception as e:
            raise ExtractionError(f"Failed to execute chunked query: {e}")
    
    def get_min_max_values(self, table: str, column: str, 
                          where_clause: Optional[str] = None) -> Tuple[Optional[int], Optional[int]]:
        """Get min and max values for a column"""
        try:
            query = f"SELECT MIN({column}) as min_val, MAX({column}) as max_val FROM {table} WHERE {column} <> 0"
            
            if where_clause:
                query += f" AND ({where_clause})"
            
            df = self.execute_query(query)
            
            min_val = df['min_val'].iloc[0] if not df.empty else None
            max_val = df['max_val'].iloc[0] if not df.empty else None
            
            # Convert to int if not None
            min_val = int(min_val) if min_val is not None else None
            max_val = int(max_val) if max_val is not None else None
            
            return min_val, max_val
            
        except Exception as e:
            raise ExtractionError(f"Failed to get min/max values: {e}")
    
    def close(self):
        """Close connection"""
        if self.connection:
            try:
                self.connection.close()
            except Exception:
                pass
            finally:
                self.connection = None
    
    def _get_password(self):
        """Get password from secrets manager"""
        if not self._secrets_helper:
            # Build secret path from config
            secret_path = f"{self.config.endpoint_name.lower()}"  # Adjust based on your secret naming
            self._secrets_helper = SecretsHelper(secret_path)
        
        self._password = self._secrets_helper.get_secret_value(self.config.secret_key)
    
    def _fix_duplicate_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Fix duplicate column names by appending numbers"""
        if df.empty:
            return df
        
        columns = list(df.columns)
        
        # Check for duplicates
        if len(columns) != len(set(columns)):
            seen = {}
            new_columns = []
            
            for col in columns:
                if col in seen:
                    seen[col] += 1
                    new_col = f"{col}_{seen[col]}"
                else:
                    seen[col] = 0
                    new_col = col
                new_columns.append(new_col)
            
            df.columns = new_columns
        
        return df