# OSAA Data Pipeline - Business Glossary

## Table of Contents

1. [Overview](#overview)
2. [Key Concepts](#key-concepts)
3. [Data Sources](#data-sources)
4. [Indicator Types](#indicator-types)
5. [Measurement Terms](#measurement-terms)
6. [Geographic Classifications](#geographic-classifications)
7. [Time Periods](#time-periods)
8. [Data Quality Terms](#data-quality-terms)
9. [Technical Terms](#technical-terms)
10. [Acronyms and Abbreviations](#acronyms-and-abbreviations)

## Overview

This business glossary provides definitions and explanations for terms used throughout the OSAA Data Pipeline. It serves as a reference for both technical and non-technical stakeholders to ensure consistent understanding of concepts, metrics, and terminology.

## Key Concepts

### 2030 Agenda for Sustainable Development
The global framework adopted by all United Nations Member States in 2015, providing a shared blueprint for peace and prosperity for people and the planet. It includes 17 Sustainable Development Goals (SDGs) with 169 targets to be achieved by 2030.

### Development Indicator
A quantitative or qualitative measure that provides information about development progress, conditions, or trends. Examples include GDP per capita, literacy rates, or life expectancy.

### Data Pipeline
An automated system that ingests, transforms, and delivers data from various sources to end users. The OSAA pipeline processes development indicators from multiple international organizations.

### Master Data
The single, authoritative source of truth for critical business data. In the OSAA pipeline, the master.indicators table serves as master data for all development metrics.

### Data Lake
A centralized repository (S3 in this case) that stores structured and unstructured data at scale. The OSAA data lake contains raw, processed, and analytical datasets.

### ETL (Extract, Transform, Load)
The process of extracting data from source systems, transforming it into a useful format, and loading it into a destination system. The OSAA pipeline performs ETL on development indicators.

## Data Sources

### SDG (Sustainable Development Goals)
**Source**: United Nations Statistics Division
**Description**: Official indicators measuring progress toward the 17 Sustainable Development Goals
**Coverage**: 231 unique indicators across all UN member states
**Update Frequency**: Annual
**Key Areas**: Poverty, hunger, health, education, gender equality, climate action

### WDI (World Development Indicators)
**Source**: World Bank
**Description**: Comprehensive collection of development indicators compiled from officially recognized international sources
**Coverage**: 1,400+ indicators for 217 economies
**Update Frequency**: Quarterly
**Key Areas**: Economic development, social development, environment, global links

### OPRI (Operational Performance and Risk Indicators)
**Source**: UN-OSAA Internal Systems
**Description**: Metrics measuring institutional effectiveness and operational risk
**Coverage**: Operational metrics for UN operations in Africa
**Update Frequency**: Monthly
**Key Areas**: Operational efficiency, program delivery, risk management, financial performance

### EDU (Education Statistics)
**Source**: UNESCO Institute for Statistics
**Description**: Comprehensive education data covering access, participation, and outcomes
**Coverage**: Education metrics for all countries
**Update Frequency**: Annual
**Key Areas**: Enrollment rates, literacy, educational attainment, education expenditure

## Indicator Types

### Tier I Indicators
SDG indicators that are conceptually clear, have internationally established methodology, and data are regularly produced by countries for at least 50% of countries.

### Tier II Indicators
SDG indicators that are conceptually clear and have internationally established methodology but data are not regularly produced by countries.

### Tier III Indicators
SDG indicators for which no internationally established methodology or standards are yet available, but methodology/standards are being developed.

### Composite Indicators
Indicators that combine multiple individual indicators into a single measure. Examples include the Human Development Index (HDI) or the Gender Inequality Index (GII).

### Leading Indicators
Metrics that predict future trends or outcomes. For example, education enrollment rates may indicate future economic development.

### Lagging Indicators
Metrics that confirm trends after they have occurred. GDP growth rates are typically lagging indicators of economic performance.

## Measurement Terms

### Baseline
The initial measurement against which progress is assessed. For SDGs, 2015 is often used as the baseline year.

### Target
A specific, measurable goal to be achieved by a certain date. SDG targets specify desired outcomes by 2030.

### Benchmark
A standard or reference point against which performance is measured. Can be based on best practices or peer comparisons.

### Threshold
A critical value that triggers action or indicates a significant change in status. For example, poverty threshold of $1.90 per day.

### Coverage
The extent to which data is available for a given indicator. Expressed as percentage of countries or population covered.

### Granularity
The level of detail in the data. Can refer to geographic (national vs. subnational) or temporal (annual vs. monthly) detail.

### Magnitude
The scale or unit of measurement for an indicator value. Common magnitudes include:
- **Units**: Individual count (people, schools, etc.)
- **Thousands**: Values multiplied by 1,000
- **Millions**: Values multiplied by 1,000,000
- **Percentage**: Proportion expressed as per 100
- **Per Capita**: Per person measure

## Geographic Classifications

### Country Code (ISO 3166-1 alpha-3)
Three-letter codes representing countries and territories. Examples:
- USA: United States
- GBR: United Kingdom
- KEN: Kenya
- BRA: Brazil

### Region
Geographic groupings of countries:
- **Sub-Saharan Africa**: African countries south of the Sahara
- **MENA**: Middle East and North Africa
- **LAC**: Latin America and Caribbean
- **EAP**: East Asia and Pacific
- **ECA**: Europe and Central Asia
- **SAR**: South Asia Region

### Income Groups (World Bank Classification)
- **Low Income**: GNI per capita ≤ $1,085
- **Lower Middle Income**: GNI per capita $1,086-$4,255
- **Upper Middle Income**: GNI per capita $4,256-$13,205
- **High Income**: GNI per capita > $13,205

### LDCs (Least Developed Countries)
Countries identified by the UN as having the lowest indicators of socioeconomic development. Currently 46 countries, primarily in Africa and Asia.

### SIDS (Small Island Developing States)
A group of small island countries facing specific social, economic, and environmental vulnerabilities.

## Time Periods

### Reference Year
The year to which the data point applies. May differ from the collection or publication year.

### Reporting Period
The time span covered by the data (e.g., calendar year, fiscal year, academic year).

### Time Series
A sequence of data points measured at successive time intervals, showing trends over time.

### Lag Time
The delay between the reference period and data availability. SDG indicators typically have 12-18 month lag.

### Forecast Period
Future time period for which projections are made based on current trends and models.

## Data Quality Terms

### Completeness
The extent to which all required data is present. Measured as percentage of non-null values.

### Accuracy
The degree to which data correctly represents real-world values. Verified through validation against source systems.

### Timeliness
How current the data is relative to the reference period. Critical for decision-making relevance.

### Consistency
The degree to which data follows uniform standards and formats across sources and time periods.

### Provisional Data
Preliminary data subject to revision. Often released for timeliness before final validation.

### Estimated Data
Values calculated using statistical models when direct observation is unavailable.

### Imputed Data
Missing values filled using statistical techniques based on available information.

### Metadata
Information describing the data, including definitions, sources, methodologies, and quality notes.

## Technical Terms

### Schema
The structure defining how data is organized, including tables, columns, and relationships.

### Parquet
A columnar storage file format optimized for analytics, used in the pipeline for efficient data storage.

### DuckDB
An embedded analytical database used for data transformations in the pipeline.

### SQLMesh
A data transformation framework that manages SQL-based data pipelines with version control.

### Ibis
A Python data analysis framework providing a pandas-like interface for multiple backends.

### Data Lineage
The documented flow of data from source to destination, showing transformations and dependencies.

### SLA (Service Level Agreement)
Commitment to data delivery timing and quality. For example, 24-hour SLA for SDG updates.

### Idempotent
Operations that produce the same result regardless of how many times they're executed. Important for pipeline reliability.

## Acronyms and Abbreviations

### Organizations

| Acronym | Full Name | Description |
|---------|-----------|-------------|
| UN | United Nations | International organization for global cooperation |
| UN-OSAA | UN Office of the Special Adviser on Africa | UN office focused on African development |
| WHO | World Health Organization | UN agency for international public health |
| UNESCO | UN Educational, Scientific and Cultural Organization | UN agency for education and culture |
| UNDP | UN Development Programme | UN's global development network |
| UNICEF | UN Children's Fund | UN agency for children's rights |
| WB | World Bank | International financial institution |
| IMF | International Monetary Fund | International monetary cooperation organization |
| AfDB | African Development Bank | Regional development bank for Africa |

### Technical Terms

| Acronym | Full Name | Description |
|---------|-----------|-------------|
| API | Application Programming Interface | Interface for programmatic access |
| AWS | Amazon Web Services | Cloud computing platform |
| CI/CD | Continuous Integration/Continuous Deployment | Automated software delivery |
| CSV | Comma-Separated Values | Simple data file format |
| JSON | JavaScript Object Notation | Data interchange format |
| S3 | Simple Storage Service | AWS object storage service |
| SQL | Structured Query Language | Database query language |
| YAML | Yet Another Markup Language | Configuration file format |
| UTC | Coordinated Universal Time | Standard time reference |

### Indicators and Metrics

| Acronym | Full Name | Description |
|---------|-----------|-------------|
| GDP | Gross Domestic Product | Total value of goods and services |
| GNI | Gross National Income | Total income earned by residents |
| HDI | Human Development Index | Composite development measure |
| PPP | Purchasing Power Parity | Economic comparison metric |
| FDI | Foreign Direct Investment | Cross-border investment |
| ODA | Official Development Assistance | Government aid for development |
| NER | Net Enrollment Rate | Education participation metric |
| MMR | Maternal Mortality Ratio | Maternal deaths per 100,000 births |
| U5MR | Under-5 Mortality Rate | Deaths under age 5 per 1,000 births |

## Calculation Methodologies

### Aggregation Methods

#### Simple Average
The arithmetic mean of values. Used when all observations have equal weight.
```
Average = Sum of all values / Number of values
```

#### Weighted Average
Mean where observations have different importance weights. Often weighted by population.
```
Weighted Average = Sum(Value × Weight) / Sum(Weights)
```

#### Median
The middle value when data is ordered. Less sensitive to outliers than mean.

#### Geometric Mean
Used for rates and ratios. Appropriate for data that grows exponentially.
```
Geometric Mean = (Product of n values)^(1/n)
```

### Growth Calculations

#### Year-over-Year Growth
Percentage change from same period in previous year.
```
YoY Growth = ((Current Year - Previous Year) / Previous Year) × 100
```

#### Compound Annual Growth Rate (CAGR)
Average annual growth rate over a period.
```
CAGR = ((Ending Value / Beginning Value)^(1/Number of Years) - 1) × 100
```

### Normalization Techniques

#### Min-Max Normalization
Scales values to 0-1 range.
```
Normalized = (Value - Min) / (Max - Min)
```

#### Z-Score Normalization
Standardizes based on mean and standard deviation.
```
Z-Score = (Value - Mean) / Standard Deviation
```

## Quality Standards

### Data Quality Dimensions

1. **Relevance**: Data meets user needs for decision-making
2. **Accuracy**: Data correctly represents intended measures
3. **Timeliness**: Data is available when needed
4. **Accessibility**: Data can be easily obtained and used
5. **Interpretability**: Data is presented clearly with adequate metadata
6. **Coherence**: Data is consistent across sources and over time

### Quality Assurance Levels

- **Bronze**: Raw data with basic validation
- **Silver**: Cleaned and standardized data
- **Gold**: Fully validated, business-ready data

### Validation Rules

- **Range Checks**: Values within expected bounds
- **Format Validation**: Data matches expected patterns
- **Referential Integrity**: Relationships between datasets maintained
- **Business Rules**: Domain-specific constraints satisfied

## Compliance and Standards

### International Standards

#### ISO 3166
International standard for country codes and subdivisions.

#### ISO 8601
International standard for date and time representations (YYYY-MM-DD).

#### SDMX (Statistical Data and Metadata eXchange)
International standard for exchanging statistical data and metadata.

### Data Protection

#### GDPR (General Data Protection Regulation)
European regulation on data protection and privacy.

#### Data Classification
- **Public**: No restrictions on access
- **Internal**: Limited to organization members
- **Confidential**: Restricted to authorized personnel
- **Sensitive**: Requires special handling and protection

## Usage Examples

### Interpreting Indicator Values

**Example 1: Poverty Rate**
- Indicator: SI.POV.DDAY
- Value: 8.5
- Magnitude: Percentage
- Interpretation: 8.5% of the population lives below $1.90 per day

**Example 2: GDP Growth**
- Indicator: NY.GDP.MKTP.KD.ZG
- Value: 3.2
- Magnitude: Percentage
- Interpretation: Real GDP grew by 3.2% compared to previous year

**Example 3: School Enrollment**
- Indicator: SE.PRM.NENR
- Value: 95.3
- Magnitude: Percentage
- Interpretation: 95.3% of primary school-age children are enrolled

### Common Calculations

**Per Capita Calculation**:
```
GDP per capita = Total GDP / Population
```

**Ratio Calculation**:
```
Pupil-Teacher Ratio = Number of Students / Number of Teachers
```

**Coverage Rate**:
```
Vaccination Coverage = (Children Vaccinated / Total Children) × 100
```

## Related Resources

### External References
- [UN SDG Indicators](https://unstats.un.org/sdgs/indicators/indicators-list/)
- [World Bank Data Help Desk](https://datahelpdesk.worldbank.org/)
- [ISO Country Codes](https://www.iso.org/iso-3166-country-codes.html)

### Internal Documentation
- [Data Catalog](DATA_CATALOG.md)
- [Operational Runbook](OPERATIONAL_RUNBOOK.md)
- [API Reference](API_REFERENCE.md)

---

*Last Updated: 2025-10-02*
*Version: 1.0.0*
*Contact: stephen.sciortino@un.org*