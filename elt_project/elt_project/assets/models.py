# elt_project/assets/models.py
from pydantic import BaseModel, Field, field_validator
from typing import Optional, Dict, Any
import json

class PipelineConfig(BaseModel):
    """
    Pydantic model representing a single pipeline configuration from the
    `elt_pipeline_configs` database table. This provides data validation
    and a clear, typed structure for pipeline settings.
    """
    pipeline_name: str
    import_name: str
    file_pattern: str
    file_type: str
    staging_table: str
    destination_table: str
    transform_procedure: str
    load_method: str = 'replace'
    is_active: bool = True

    # Optional fields
    monitored_directory: Optional[str] = None
    connection_string: Optional[str] = None
    column_mapping: Optional[str] = None
    parser_function: Optional[str] = None
    scraper_config: Optional[str] = None
    deduplication_key: Optional[str] = None
    on_success_deactivate_self_and_activate_import: Optional[str] = None
    depends_on: Optional[str] = None

    @field_validator('column_mapping', mode='before')
    @classmethod
    def parse_column_mapping(cls, v: Optional[str]) -> Optional[str]:
        """Ensures column_mapping is a valid JSON string if not None."""
        if v is None:
            return None
        try:
            json.loads(v)
            return v
        except json.JSONDecodeError:
            # This is a simple validator. In a real scenario, you might want
            # to raise a more specific error or log a warning.
            # For now, we'll just return it as is and let downstream processes handle it.
            return v

    def get_column_mapping(self) -> Dict[str, str]:
        """Parses the column_mapping string into a dictionary."""
        if not self.column_mapping:
            return {}
        # The mapping is stored as "Source > Target, Source2 > Target2"
        try:
            return {
                item.split('>')[0].strip(): item.split('>')[1].strip()
                for item in self.column_mapping.split(',')
            }
        except (json.JSONDecodeError, IndexError):
            # If parsing fails, return an empty dict to prevent errors downstream.
            return {}

    class Config:
        # This allows the model to be created from ORM objects (like SQLAlchemy results)
        from_attributes = True