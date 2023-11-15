# Reddit Ads API Data Extraction

This script is designed to interface with the Reddit Ads API to fetch various data points related to advertising campaigns, ad groups, and individual ads. It structures the data into pandas DataFrames and then uploads it to a specified BigQuery dataset.

## Features

- Data acquisition from Reddit Ads API.
- Transformation of raw data into structured pandas DataFrames.
- Uploading the structured data into Google BigQuery for further analysis.

## Requirements

- Python 3.8 or higher.
- pandas: A powerful data analysis and manipulation library for Python.
- requests: An elegant and simple HTTP library for Python.
- click: A package for creating beautiful command-line interfaces in a composable way.
- ratelimit: A simple Python decorator for rate limiting API calls.
- Google Cloud BigQuery Client Library for Python.

## Setup

Before running the script, ensure you have installed all the necessary Python libraries listed in `requirements.txt`.

```bash
pip install -r requirements.txt
```
- Additionally, you will need to set up authentication credentials for both Reddit API access and Google Cloud BigQuery.

- For Reddit API, you'll need to set up an authentication file as auth.py, which should contain your client_id, client_secret, and refresh_token.

- For Google BigQuery, ensure that you have set up your Google Cloud credentials in the environment variable GOOGLE_APPLICATION_CREDENTIALS.

## Usage
The script can be run from the command line with the following options:

```
python main.py --start-date YYYY-MM-DD --project_id YOUR_PROJECT_ID
--start-date: The date from which to start fetching the report data (format YYYY-MM-DD).
--project_id: Your Google Cloud project ID where the BigQuery dataset resides.
```

## Function Descriptions
get_reports: Fetches reports from a given start date.
get_ad_groups: Fetches ad group data.
get_ads: Fetches ads data.
get_campaigns: Fetches campaign data.
transform_report: Transforms report data into a DataFrame.
transform_ads: Transforms ads data into a DataFrame.
transform_ad_groups: Transforms ad group data into a DataFrame.
transform_campaigns: Transforms campaign data into a DataFrame.
get_result: Aggregates all data into a single DataFrame.
main: The main function that initializes the API call and handles the data upload to BigQuery.

## Logging
The script uses Python's built-in logging module to log its operation. The log level is set to INFO, and it will output logs to the console during execution.

## Error Handling
The script includes error handling for HTTP errors that may occur during API requests. It will log any exceptions encountered and abort execution.


## Authors
Anupam Kumar - Initial work - [github.com/anupamk36](https://github.com/anupamk36)

## Acknowledgments
Reddit API documentation
Google BigQuery documentation
