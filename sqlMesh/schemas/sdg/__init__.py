"""SDG schema definitions and versions.

Versioning:
- v1: Initial schema with core indicator fields
- v2: Added data_source and confidence_level fields (planned)
"""

from .v1 import SDG_SCHEMA_V1

__all__ = ["SDG_SCHEMA_V1"]
