CREATE EXTERNAL TABLE IF NOT EXISTS analytics.account_business_output (
  record_date       date,
  account_id        string,
  customer_id       string,
  account_type      string,
  account_status    string,
  currency          string,
  balance           double,
  credit_limit      double,
  opened_date       date,
  branch_code       string,
  city              string,
  last_updated_ts   timestamp
)
PARTITIONED BY (
  feed           string,
  business_date  string
)
STORED AS PARQUET
LOCATION 's3://pavan-account-output-pkr/business_output/';