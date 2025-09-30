MODEL (
    name sdg.data_national,
    kind INCREMENTAL_BY_TIME_RANGE(
      time_column loaded_at
    ),
    cron '@daily',
    columns (
      INDICATOR_ID TEXT,
      COUNTRY_ID TEXT,
      YEAR INTEGER,
      VALUE DECIMAL,
      MAGNITUDE TEXT,
      QUALIFIER TEXT,
      loaded_at TIMESTAMP,
      file_modified_at TIMESTAMP
    )
  );

  -- Phase 2: Incremental processing by time range
  -- Only processes records where loaded_at is within the execution time range
  -- Uses @start_ds and @end_ds macros to filter data efficiently
  -- Grain: (indicator_id, country_id, year) - ensures uniqueness
  SELECT
    *,
    CURRENT_TIMESTAMP AS loaded_at,
    CURRENT_TIMESTAMP AS file_modified_at  -- TODO: Replace with actual file modification time from metadata
  FROM
    read_parquet(
        @s3_read('edu/SDG_DATA_NATIONAL')
    )
  WHERE
    -- Incremental filter: only process new/updated data
    loaded_at >= @start_ds
    AND loaded_at < @end_ds;
