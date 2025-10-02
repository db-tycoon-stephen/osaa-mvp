"""SDG Schema Version 2 - Extended schema with data quality fields.

This version adds data quality and lineage tracking fields to the SDG schema.

New fields in v2:
- data_source: Source of the data (default: "UN")
- confidence_level: Confidence level of the data point (0-1 scale)

Changes from v1:
- Added data_source field (nullable, default "UN")
- Added confidence_level field (nullable)
"""

try:
    from .v1 import SDG_SCHEMA_V1
except ImportError:
    from v1 import SDG_SCHEMA_V1

SDG_SCHEMA_V2 = {
    **SDG_SCHEMA_V1,
    "data_source": {
        "type": "String",
        "nullable": True,
        "default": "UN",
        "description": "Source of the data (e.g., UN, WHO, World Bank)"
    },
    "confidence_level": {
        "type": "Decimal",
        "nullable": True,
        "description": "Confidence level of the data point (0-1 scale)"
    },
}


# Metadata for version 2
SDG_V2_METADATA = {
    "version": 2,
    "created_at": "2024-10-01",
    "created_by": "system",
    "description": "Extended SDG schema with data quality tracking fields",
    "grain": ["indicator_id", "country_id", "year"],
    "changes_from_v1": [
        "Added data_source field for tracking data provenance",
        "Added confidence_level field for data quality assessment"
    ]
}
