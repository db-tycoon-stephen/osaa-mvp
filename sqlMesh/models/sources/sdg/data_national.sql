MODEL (
    name sdg.data_national,
    kind FULL,
    cron '@daily',
    columns (
      INDICATOR_ID TEXT,
      COUNTRY_ID TEXT,
      YEAR INTEGER,
      VALUE DECIMAL,
      MAGNITUDE TEXT,
      QUALIFIER TEXT
    )
  );

  SELECT
    *
  FROM
    read_parquet(
        @s3_read('edu/SDG_DATA_NATIONAL')
    );

@s3_write();