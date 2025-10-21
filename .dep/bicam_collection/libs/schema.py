"""
Schema Configuration System

This module provides a more programmatic way to define and manage
scraper configurations using Pydantic models instead of raw YAML files.
"""

import logging
from collections import OrderedDict
from enum import Enum
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field, field_validator

logger = logging.getLogger(__name__)


class FieldType(str, Enum):
    """Data types for schema fields"""

    STRING = "string"
    INTEGER = "integer"
    BOOLEAN = "boolean"
    DATE = "date"
    DATETIME = "datetime"
    TEXT = "text"
    URL = "url"
    JSON = "json"
    FLOAT = "float"


class SchemaField(BaseModel):
    """Definition of a single field in a data schema"""

    name: str = Field(..., description="Field name")
    type: FieldType = Field(FieldType.STRING, description="Field data type")
    required: bool = Field(True, description="Whether field is required")
    description: str | None = Field(None, description="Field description")
    max_length: int | None = Field(None, description="Maximum field length")
    default: Any = Field(None, description="Default value")


class NestedField(BaseModel):
    """Definition of a nested field structure"""

    name: str = Field(..., description="Nested field name")
    fields: list[SchemaField] = Field(default_factory=list, description="Sub-fields")
    is_array: bool = Field(True, description="Whether this is an array field")


class DataTypeSchema(BaseModel):
    """Schema definition for a specific data type"""

    name: str = Field(..., description="Data type name")
    table_name: str | None = Field(None, description="Database table name")
    is_main: bool = Field(True, description="Whether this is a main entity type")
    description: str | None = Field(None, description="Data type description")
    create_raw: bool = Field(
        True, description="Whether a raw* table should be created for this data type"
    )

    # Field definitions
    fields: list[SchemaField] = Field(default_factory=list, description="Main fields")
    id_fields: list[str] = Field(default_factory=list, description="Primary key fields")
    nested_fields: list[NestedField] = Field(
        default_factory=list, description="Nested/complex fields"
    )
    related_fields: list[str] = Field(
        default_factory=list, description="Related entity fields"
    )

    # API configuration
    api_endpoint: str | None = Field(None, description="API endpoint for requests")
    list_key: str | list[str] | None = Field(
        None, description="Key(s) to access list of items in response"
    )
    full_key: str | None = Field(
        None, description="Key to access full data item in response"
    )

    # Processing configuration
    batch_size: int = Field(1000, description="Batch size for processing")
    retry_attempts: int = Field(3, description="Number of retry attempts")
    checkpoint_frequency: int = Field(100, description="Checkpoint save frequency")

    @property
    def table_name_computed(self) -> str:
        """Get table name, using name if not explicitly set"""
        return self.table_name or self.name.lower().replace("-", "_")

    def to_legacy_config(self) -> dict[str, Any]:
        """Convert to legacy YAML config format"""
        config = {
            "main": self.is_main,
            "fields": [field.name for field in self.fields],
            "id_fields": self.id_fields,
            "related_fields": self.related_fields,
            "nested_fields": {},
        }

        # Add nested fields
        for nested in self.nested_fields:
            if nested.fields:
                config["nested_fields"][nested.name] = {
                    "fields": [field.name for field in nested.fields]
                }
            else:
                if "nested_fields" not in config:
                    config["nested_fields"] = []
                if isinstance(config["nested_fields"], list):
                    config["nested_fields"].append(nested.name)

        return config

    @field_validator("nested_fields", mode="before")
    @classmethod
    def _convert_nested_fields(cls, v):
        """Allow nested_fields to be given as list of strings."""
        if not v:
            return []
        converted = []
        if isinstance(v, list):
            for item in v:
                if isinstance(item, str):
                    converted.append({"name": item, "fields": []})
                elif isinstance(item, dict):
                    converted.append(item)
                else:
                    raise ValueError("Invalid nested_field entry")
        elif isinstance(v, dict):
            # transform dict mapping name -> {fields: [...]}
            for n, defn in v.items():
                if isinstance(defn, dict):
                    converted.append({"name": n, **defn})
                else:
                    converted.append({"name": n, "fields": []})
        else:
            raise ValueError("nested_fields must be list or dict")
        return converted


class ScrapingSchema(BaseModel):
    """Complete schema definition for a scraping source"""

    name: str = Field(..., description="Schema name (e.g., 'congressional', 'govinfo')")
    version: str = Field("1.0", description="Schema version")
    description: str | None = Field(None, description="Schema description")

    data_types: list[DataTypeSchema] = Field(
        default_factory=list, description="Data type definitions"
    )

    def get_data_type(self, name: str) -> DataTypeSchema | None:
        """Get data type schema by name"""
        for dt in self.data_types:
            if dt.name == name:
                return dt
        return None

    def to_legacy_yaml(self, output_path: Path | str) -> None:
        """Export to legacy YAML format"""
        legacy_config = {}

        for data_type in self.data_types:
            legacy_config[data_type.name] = data_type.to_legacy_config()

        output_path = Path(output_path)
        with open(output_path, "w") as f:
            yaml.dump(legacy_config, f, default_flow_style=False, indent=2)

        logger.info(f"Exported legacy config to {output_path}")

    @classmethod
    def from_legacy_yaml(cls, yaml_path: Path | str) -> "ScrapingSchema":
        """Import from legacy YAML format"""
        yaml_path = Path(yaml_path)
        schema_name = yaml_path.stem.replace("_config", "")

        with open(yaml_path) as f:
            legacy_config = yaml.safe_load(f)

        data_types = []
        if not legacy_config:  # Handle empty YAML
            return cls(
                name=schema_name,
                description=f"Schema imported from empty file: {yaml_path}",
                data_types=[],
            )

        for dt_name, dt_config in legacy_config.items():
            if not isinstance(dt_config, dict):
                logger.warning(
                    f"Skipping malformed data type '{dt_name}' in {yaml_path}"
                )
                continue

            # Convert fields
            fields = [
                SchemaField(name=field_name, type=FieldType.STRING)
                for field_name in dt_config.get("fields", [])
            ]

            # Convert nested fields
            nested_fields = []
            nested_config = dt_config.get("nested_fields", {})  # Default to dict

            if isinstance(nested_config, dict):
                for nested_name, nested_def in nested_config.items():
                    nested_field_objs = []
                    if isinstance(nested_def, dict) and "fields" in nested_def:
                        nested_field_objs = [
                            SchemaField(name=field_name, type=FieldType.STRING)
                            for field_name in nested_def.get("fields", [])
                        ]
                    nested_fields.append(
                        NestedField(name=nested_name, fields=nested_field_objs)
                    )
            elif isinstance(nested_config, list):
                for item in nested_config:
                    if isinstance(item, dict):
                        for nested_name, nested_def in item.items():
                            nested_field_objs: list[SchemaField] = []
                            if isinstance(nested_def, dict) and "fields" in nested_def:
                                nested_field_objs = [
                                    SchemaField(name=field_name, type=FieldType.STRING)
                                    for field_name in nested_def.get("fields", [])
                                ]
                            nested_fields.append(
                                NestedField(name=nested_name, fields=nested_field_objs)
                            )
                    elif isinstance(item, str):
                        nested_fields.append(NestedField(name=item, fields=[]))

            # Convert related fields (flatten any nested lists)
            raw_related = dt_config.get("related_fields", [])
            related_fields: list[str] = []
            if isinstance(raw_related, list):
                for rel in raw_related:
                    if isinstance(rel, str):
                        related_fields.append(rel)
                    elif isinstance(rel, list):
                        related_fields.extend(
                            [str(r) for r in rel if isinstance(r, str)]
                        )
            elif isinstance(raw_related, str):
                related_fields.append(raw_related)

            data_type = DataTypeSchema(
                name=dt_name,
                table_name=dt_name,
                is_main=dt_config.get("main", False),
                fields=fields,
                id_fields=dt_config.get("id_fields", []),
                nested_fields=nested_fields,
                related_fields=related_fields,
                # API configuration - extract from nested api section if present
                api_endpoint=dt_config.get("api", {}).get("api_endpoint"),
                list_key=dt_config.get("api", {}).get("list_key"),
                full_key=dt_config.get("api", {}).get("full_key"),
            )
            data_types.append(data_type)

        return cls(
            name=schema_name,
            description=f"Schema imported from {yaml_path}",
            data_types=data_types,
        )


class SchemaManager:
    """Manager for schema configurations"""

    def __init__(self, config_dir: Path | None = None):
        if config_dir is None:
            self.config_dir = Path("configs")
        else:
            self.config_dir = Path(config_dir)
        self.config_dir.mkdir(exist_ok=True)

    def save_schema(self, schema: ScrapingSchema) -> Path:
        """Save schema to file in a readable, deterministic YAML format."""
        schema_path = self.config_dir / f"{schema.name}_schema.yaml"

        # Custom representer for OrderedDict to ensure order is kept
        def represent_ordereddict(dumper, data):
            value = []
            for item_key, item_value in data.items():
                node_key = dumper.represent_data(item_key)
                node_value = dumper.represent_data(item_value)
                value.append((node_key, node_value))
            return yaml.nodes.MappingNode("tag:yaml.org,2002:map", value)

        yaml.add_representer(OrderedDict, represent_ordereddict, Dumper=yaml.SafeDumper)

        # Build ordered representation for the entire schema
        root = OrderedDict()
        root["name"] = schema.name
        root["version"] = schema.version
        root["description"] = schema.description

        data_types_serialised: list[OrderedDict] = []
        # Sort data types by name for deterministic output
        for dt in sorted(schema.data_types, key=lambda x: x.name):
            dt_od = OrderedDict()
            # User requested order: name, table_name, description, is_main, columns, metadata
            dt_od["name"] = dt.name
            dt_od["table_name"] = dt.table_name_computed
            dt_od["description"] = dt.description
            dt_od["is_main"] = dt.is_main
            dt_od["create_raw"] = dt.create_raw

            # Columns - also sorted for determinism
            dt_od["id_fields"] = sorted(dt.id_fields)
            dt_od["list_key"] = dt.list_key
            dt_od["full_key"] = dt.full_key
            dt_od["api_endpoint"] = dt.api_endpoint
            dt_od["fields"] = [
                f.model_dump(mode="json")
                for f in sorted(dt.fields, key=lambda x: x.name)
            ]
            dt_od["nested_fields"] = [
                nf.model_dump(mode="json")
                for nf in sorted(dt.nested_fields, key=lambda x: x.name)
            ]
            dt_od["related_fields"] = sorted(dt.related_fields)

            # Processing Metadata
            dt_od["batch_size"] = dt.batch_size
            dt_od["checkpoint_frequency"] = dt.checkpoint_frequency
            dt_od["retry_attempts"] = dt.retry_attempts

            data_types_serialised.append(dt_od)

        root["data_types"] = data_types_serialised

        with open(schema_path, "w") as f:
            yaml.safe_dump(
                root, stream=f, default_flow_style=False, sort_keys=False, indent=2
            )

        logger.info(f"Saved schema: {schema_path}")
        return schema_path

    def load_schema(self, name: str) -> ScrapingSchema:
        """Load schema from file"""
        schema_path = self.config_dir / f"{name}_schema.yaml"

        if not schema_path.exists():
            raise FileNotFoundError(f"Schema not found: {schema_path}")

        with open(schema_path) as f:
            schema_dict = yaml.safe_load(f)

        return ScrapingSchema(**schema_dict)

    def list_schemas(self) -> list[str]:
        """List available schemas"""
        schema_files = list(self.config_dir.glob("*_schema.yaml"))
        return [f.stem.replace("_schema", "") for f in schema_files]

    def generate_legacy_configs(self, schema_name: str) -> None:
        """Generate legacy YAML configs from schema"""
        schema = self.load_schema(schema_name)

        # Generate legacy config
        legacy_path = Path(f"scrapers/{schema_name}/{schema_name}_config.yaml")
        legacy_path.parent.mkdir(parents=True, exist_ok=True)

        schema.to_legacy_yaml(legacy_path)
        logger.info(f"Generated legacy config: {legacy_path}")

    def import_legacy_config(self, yaml_path: Path | str) -> ScrapingSchema:
        """Helper to create a schema from a legacy YAML config file."""
        schema = ScrapingSchema.from_legacy_yaml(yaml_path)
        self.save_schema(schema)
        # Also regenerate (overwrite) the legacy config to ensure parity
        schema.to_legacy_yaml(yaml_path)
        logger.info(f"Saved schema and synced legacy YAML for {schema.name}")
        return schema


def create_bicam_congressional_schema() -> ScrapingSchema:
    """Create the congressional data schema from the legacy YAML config."""
    yaml_path = Path("scrapers/congressional/bicam_congressional_config.yaml")
    if not yaml_path.exists():
        logger.warning("Congressional YAML config not found – returning empty schema")
        return ScrapingSchema(
            name="bicam_congressional", description="Empty congressional schema"
        )
    return ScrapingSchema.from_legacy_yaml(yaml_path)


def create_bicam_govinfo_schema() -> ScrapingSchema:
    """Create the govinfo data schema from the legacy YAML config."""
    yaml_path = Path("scrapers/govinfo/bicam_govinfo_config.yaml")
    if not yaml_path.exists():
        logger.warning("GovInfo YAML config not found – returning empty schema")
        return ScrapingSchema(name="bicam_govinfo", description="Empty govinfo schema")
    return ScrapingSchema.from_legacy_yaml(yaml_path)


def create_default_schemas() -> None:
    """Create and save default schemas by converting the legacy YAML configs."""
    manager = SchemaManager()

    # Import schemas from legacy configs
    for name, func in {
        "bicam_congressional": create_bicam_congressional_schema,
        "bicam_govinfo": create_bicam_govinfo_schema,
    }.items():
        schema = func()
        if schema.data_types:
            manager.save_schema(schema)
            # Also regenerate (overwrite) the legacy config to ensure parity
            legacy_path = Path(f"scrapers/{name}/{name}_config.yaml")
            schema.to_legacy_yaml(legacy_path)
            logger.info(f"Saved schema and synced legacy YAML for {name}")

    logger.info("Default schemas regenerated from YAML configs")


SCHEMA_CREATORS = {
    "bicam_congressional": create_bicam_congressional_schema,
    "bicam_govinfo": create_bicam_govinfo_schema,
}


def get_schema(name: str) -> ScrapingSchema:
    """Get schema by name"""
    manager = SchemaManager()
    return manager.load_schema(name)


if __name__ == "__main__":
    # Example usage
    logging.basicConfig(level=logging.INFO)

    # Create default schemas
    create_default_schemas()

    # Example: Import existing legacy config
    manager = SchemaManager()

    # Import congressional config
    if Path("scrapers/congressional/congressional_config.yaml").exists():
        schema = manager.import_legacy_config(
            "scrapers/congressional/congressional_config.yaml"
        )
        logger.info(
            f"Imported congressional schema with {len(schema.data_types)} data types"
        )

    # Import govinfo config
    if Path("scrapers/govinfo/govinfo_config.yaml").exists():
        schema = manager.import_legacy_config("scrapers/govinfo/govinfo_config.yaml")
        logger.info(f"Imported govinfo schema with {len(schema.data_types)} data types")
