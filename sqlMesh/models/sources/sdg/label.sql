MODEL (
    name sdg.label,
    kind FULL,
    cron '@daily',
    columns (
      INDICATOR_ID TEXT,
      INDICATOR_LABEL_EN TEXT
    )
  );

  SELECT
    *
  FROM
    read_parquet(
        @s3_landing_path('edu/SDG_LABEL')
    )
