# Fivetran x Robin

This is an [AWS lambda Fivetran custom connector](https://fivetran.com/docs/functions/aws-lambda) 
that allows you to use the [Robin Analytics API](https://support.robinpowered.com/hc/en-us/articles/11028082938253-Robin-s-analytics-API)
as a Fivetran source.

Practically, this would allow you to use Fivetran to make the data from the Robin Analytics API available in a dataw arehouse
or RDBMS.

## Setup

You'll need a [Fivetran](https://www.fivetran.com/) account, [Robin](https://robinpowered.com/) account, and a Robin API key.

The first thing you'll want to do is set up a "Destination" in Fivetran for where you want your analytics data.
This could be a RDBMS like Postgres, a data warehouse like Snowflake, or an AWS S3 bucket to testing things out.

Next, follow the instructions on [AWS Lambda Setup Guide](https://fivetran.com/docs/functions/aws-lambda/setup-guide) to get 
the plumbing setup. 
When you get to the "Create Lambda function" step,  you'll need to upload the "latest.zip" file under the "Code source -> Upload from" menu.

**NOTE:** As of writing (3/29/23) the policy JSON under "Create IAM policy" contains an invalid version string. It should be "2012-10-17"
not "2022-07-13".

## Secrets

At the "Finish Fivetran configuration" step you'll need to add the secrets which the AWS Lambda is looking for.

The required secrets are:

* **apiKey** - Your Robin API key
* **organizationId** - Your Robin Organization ID

These are optional additional configurations:
* **startDate** - The earliest date to sync data from in format "2008-01-01T00:00:00Z" (default)
* **tableName** - The destination table name. Defaults to "robin_analytics"

## Sync

After everything is set up Fivetran will kick of a "full sync" of your data. Subsequent syncs will be incremental 
from the time of the last sync to the current time. 
Assuming everything worked, you should see a table named "robin_analytics" in your destination.

## Help!

Stuck? Something didn't work? You can reach us at [contact@setfive.com]()
