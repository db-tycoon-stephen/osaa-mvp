MODEL (
    name wdi.csv,
    kind INCREMENTAL_BY_TIME_RANGE(
      time_column loaded_at
    ),
    columns (
      "Country Name" TEXT,
      "Country Code" TEXT,
      "Indicator Name" TEXT,
      "Indicator Code" TEXT,
      "1960" DECIMAL,
      "1961" DECIMAL,
      "1962" DECIMAL,
      "1963" DECIMAL,
      "1964" DECIMAL,
      "1965" DECIMAL,
      "1966" DECIMAL,
      "1967" DECIMAL,
      "1968" DECIMAL,
      "1969" DECIMAL,
      "1970" DECIMAL,
      "1971" DECIMAL,
      "1972" DECIMAL,
      "1973" DECIMAL,
      "1974" DECIMAL,
      "1975" DECIMAL,
      "1976" DECIMAL,
      "1977" DECIMAL,
      "1978" DECIMAL,
      "1979" DECIMAL,
      "1980" DECIMAL,
      "1981" DECIMAL,
      "1982" DECIMAL,
      "1983" DECIMAL,
      "1984" DECIMAL,
      "1985" DECIMAL,
      "1986" DECIMAL,
      "1987" DECIMAL,
      "1988" DECIMAL,
      "1989" DECIMAL,
      "1990" DECIMAL,
      "1991" DECIMAL,
      "1992" DECIMAL,
      "1993" DECIMAL,
      "1994" DECIMAL,
      "1995" DECIMAL,
      "1996" DECIMAL,
      "1997" DECIMAL,
      "1998" DECIMAL,
      "1999" DECIMAL,
      "2000" DECIMAL,
      "2001" DECIMAL,
      "2002" DECIMAL,
      "2003" DECIMAL,
      "2004" DECIMAL,
      "2005" DECIMAL,
      "2006" DECIMAL,
      "2007" DECIMAL,
      "2008" DECIMAL,
      "2009" DECIMAL,
      "2010" DECIMAL,
      "2011" DECIMAL,
      "2012" DECIMAL,
      "2013" DECIMAL,
      "2014" DECIMAL,
      "2015" DECIMAL,
      "2016" DECIMAL,
      "2017" DECIMAL,
      "2018" DECIMAL,
      "2019" DECIMAL,
      "2020" DECIMAL,
      "2021" DECIMAL,
      "2022" DECIMAL,
      "2023" DECIMAL,
      loaded_at TIMESTAMP,
      file_modified_at TIMESTAMP
    )
  );

  -- Phase 2: Incremental processing by time range with file modification tracking
  -- Only processes records where loaded_at is within the execution time range
  -- Uses file_modified_at to track source file changes
  SELECT
    *,
    CURRENT_TIMESTAMP AS loaded_at,
    CURRENT_TIMESTAMP AS file_modified_at  -- TODO: Replace with actual file modification time from metadata
  FROM
      read_parquet(
        @s3_read('wdi/WDICSV')
    )
  WHERE
    -- Incremental filter: only process new/updated data
    loaded_at >= @start_ds
    AND loaded_at < @end_ds;
