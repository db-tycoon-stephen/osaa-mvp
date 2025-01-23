MODEL (
    name opri.label,
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
        @s3_read('edu/OPRI_LABEL')
    );
