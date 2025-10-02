"""WDI Schema Version 1 - Initial schema definition.

This is the initial schema for WDI (World Development Indicators),
containing core fields for tracking development indicators.

Fields:
- country_id: Unique identifier for the country
- indicator_id: Unique identifier for the indicator
- year: Year of the data
- value: Indicator value (decimal)
- magnitude: Magnitude of the indicator
- qualifier: Qualifier for the data point
- indicator_description: Human-readable description of the indicator
"""

WDI_SCHEMA_V1 = {
    "country_id": {
        "type": "String",
        "nullable": False,
        "description": "The unique identifier for the country"
    },
    "indicator_id": {
        "type": "String",
        "nullable": False,
        "description": "The unique identifier for the indicator"
    },
    "year": {
        "type": "Int",
        "nullable": False,
        "description": "The year of the data"
    },
    "value": {
        "type": "Decimal",
        "nullable": True,
        "description": "The value of the indicator for the country and year"
    },
    "magnitude": {
        "type": "String",
        "nullable": True,
        "description": "The magnitude of the indicator for the country and year"
    },
    "qualifier": {
        "type": "String",
        "nullable": True,
        "description": "The qualifier of the indicator for the country and year"
    },
    "indicator_description": {
        "type": "String",
        "nullable": True,
        "description": "The description of the indicator"
    },
}


# Metadata for version 1
WDI_V1_METADATA = {
    "version": 1,
    "created_at": "2024-10-01",
    "created_by": "system",
    "description": "Initial WDI schema with core indicator fields",
    "grain": ["country_id", "indicator_id", "year"],
}
