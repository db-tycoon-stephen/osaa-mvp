MODEL (
    name sdg.label,
    kind FULL,
    cron '@daily'
  );

  SELECT
    *
  FROM
    read_parquet(
        @s3_landing_path('edu/SDG_LABEL')
    )
