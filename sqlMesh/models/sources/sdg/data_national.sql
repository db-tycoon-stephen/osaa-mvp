MODEL (
    name sdg.data_national,
    kind FULL,
    cron '@daily'
  );

  SELECT
    *
  FROM
    read_parquet(
        @s3_landing_path('edu/SDG_DATA_NATIONAL')
    )
