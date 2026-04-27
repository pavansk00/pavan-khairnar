ALTER TABLE analytics.account_business_output
ADD IF NOT EXISTS
PARTITION (feed='ACCOUNT', business_date='2026-04-25')
LOCATION 's3://pavan-account-output-pkr/business_output/feed=ACCOUNT/business_date=2026-04-25/';