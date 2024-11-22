MODEL (
    name opri.label,
    kind FULL,
    cron '@daily'
  );

  SELECT
    *
  FROM
    read_parquet(
        @s3_landing_path('edu/OPRI_LABEL')
    )
