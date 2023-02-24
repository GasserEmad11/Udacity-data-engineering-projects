## Summary 
This project creates a high grade data pipeline that is dynamic and built from reusable tasks, can be monitored, and allow easy backfills. As data quality plays a big part when analyses are executed on top the data warehouse, thus the pipeline runs tests against Sparkify's datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.
