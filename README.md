
Australia EPA Data Collection Scripts

These two scripts interact with the EPA Victoria Environment Monitoring API to:
	1.	Retrieve a list of air-quality monitoring sites.
	2.	Fetch time-series parameter data for each site, with built-in rate limiting, retries, and formatting into pandas DataFrames/CSV.

⸻

1. site_list_script.py

Purpose
	•	Fetches all air environmental monitoring sites using:

GET /sites?environmentalSegment=air



Key Features
	•	Loads the API key from .env using python-dotenv.
	•	Calls the EPA API gateway endpoint with httpx.AsyncClient.
	•	Prints the total number of sites and details such as site name, ID, type, and coordinates.

Usage
	•	Run directly with uv run site_list_script.py.
	•	Outputs a console listing of all sites returned by the API.

⸻

2. aussie_parameter_search.py

Purpose
	•	Builds on the first script by:
	1.	Fetching the site list.
	2.	Iterating through each siteID to call:

GET /sites/{siteID}/parameters?environmentalSegment=air

(returns hourly and daily parameter series like PM2.5, NO₂, etc.)

	3.	Aggregating the results into a clean, tabular format using pandas.

Key Features
	•	Loads the API key from .env.
	•	Implements an async token-bucket rate limiter to respect the EPA default of 5 requests/sec, crucial to avoid API throttling.  ￼ ￼ ￼ ￼
	•	Adds retry logic with Retry-After handling for HTTP 429 responses (Too Many Requests).
	•	Normalizes API’s nested JSON (parameters → timeSeriesReadings → readings) into a tidy DataFrame with columns:

siteID, parameter, unit, series, since, until, averageValue, healthAdvice, healthAdviceColor, healthCode


	•	Flags any problematic site fetches (e.g., exhausted retries) and logs them.
	•	Optionally saves output to CSV (e.g., aussie_data.csv).

Usage
	•	Run with uv run aussie_parameter_search.py
	•	It prints summary stats and the first few rows of the resulting DataFrame to the console.
	•	Saves the collected parameter data to aussie_data.csv.

⸻

Summary Table

Script Name	Functionality
site_list_script.py	Retrieve & display all air monitoring sites
aussie_parameter_search.py	Retrieve, normalize, and save parameter readings for each site, with rate limiting and robust error handling


⸻

Setup Requirements
	•	.env file containing:

API_KEY=<your-EPA-portal-key>


	•	Python dependencies:

pip install httpx pandas python-dotenv



⸻

Notes
	•	Rate Limiting: The EPA’s developer portal enforces 5 requests/sec. Respecting this ensures reliable operation.
	•	Parameter Fetching: The API’s parameters endpoint does not accept since as a query parameter. It returns recent hourly/daily readings (e.g., last 48 hrs) embedded in the response.
	•	Data Persistence: For long-term data analysis, persist the developer-accessible live data and augment with EPA’s published historic datasets as needed.

⸻

Let me know if you’d like help integrating these scripts into a scheduled pipeline or extending them with filtering and visualizations!

