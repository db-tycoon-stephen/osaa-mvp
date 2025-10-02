"""Schema definitions and versioning for OSAA data models.

This package contains versioned schema definitions for all data sources:
- SDG (Sustainable Development Goals)
- OPRI (Open-source Policy Research Institute)
- WDI (World Development Indicators)

Each schema is versioned to support schema evolution and migration.
"""

from .sdg import SDG_SCHEMA_V1
from .opri import OPRI_SCHEMA_V1
from .wdi import WDI_SCHEMA_V1

__all__ = [
    "SDG_SCHEMA_V1",
    "OPRI_SCHEMA_V1",
    "WDI_SCHEMA_V1",
]
