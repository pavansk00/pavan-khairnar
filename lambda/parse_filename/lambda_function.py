import re

# Expected filename: ACCOUNT_YYYY-MM-DD.csv
FILENAME_PATTERN = re.compile(r"^(?P<feed>ACCOUNT)_(?P<date>\d{4}-\d{2}-\d{2})\.csv$")


def lambda_handler(event, context):
    """
    Parse and validate the input file name, returning feed + businessDate.

    Supported event shapes:

    (1) Simple (manual testing):
        {
          "bucket": "pavan-account-raw-pkr",
          "key": "incoming/ACCOUNT_2026-04-25.csv"
        }

    (2) EventBridge S3 event:
        {
          "detail": {
            "bucket": { "name": "..." },
            "object": { "key": "incoming/ACCOUNT_2026-04-25.csv" }
          }
        }
    """

    bucket = None
    key = None

    # EventBridge S3 shape (common)
    if isinstance(event, dict) and "detail" in event:
        bucket = event.get("detail", {}).get("bucket", {}).get("name")
        key = event.get("detail", {}).get("object", {}).get("key")

    # Simple test shape
    if not bucket and isinstance(event, dict) and "bucket" in event:
        bucket = event.get("bucket")
    if not key and isinstance(event, dict) and "key" in event:
        key = event.get("key")

    # Validate required input
    if not key:
        raise Exception(f"Missing S3 object key in event: {event}")

    filename = key.split("/")[-1]  # last component of the S3 key

    # Validate filename format
    match = FILENAME_PATTERN.match(filename)
    if not match:
        raise Exception(
            f"Invalid filename '{filename}'. Expected format: ACCOUNT_YYYY-MM-DD.csv"
        )

    feed = match.group("feed")
    business_date = match.group("date")

    return {
        "feed": feed,
        "businessDate": business_date,
        "bucket": bucket,
        "key": key,
        "filename": filename
    }