MODEL (
    name opri.data_national,
    kind FULL,
    cron '@daily'
  );

  SELECT
    *
  FROM

    read_parquet(
        @s3_landing_path('edu/OPRI_DATA_NATIONAL')
    )
