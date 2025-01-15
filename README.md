# python-for-data-engineer
How to use Python for Data Engineering

To better understand the data engineering process, let’s walk through a small project. 

In this example, I’ll demonstrate how to extract data from an API, perform basic 

https://finnhub.io/
Let’s choose FINNHUB API, cause we want to track daily price change of NVIDIA stock and build a dataset for analysis. 

The data retrieved from Finhub comes in JSON format, which consists of key-value pairs. 

You can extract specific values using your key.


To make this data more readable and useful, we transform it.

A basic transformation job involves converting the JSON data into a structured, tabular format, similar to an Excel sheet or a relational database table. 

The pandas library in Python is particularly useful for this task, as it allows us to create DataFrames that organize data in a two-dimensional table. And that one we will convert into csv file

In this project, we’ll store the transformed data in Amazon S3. Later, it can be loaded into a data warehouse for analysis or used directly to build dashboards.

To streamline the process, we can automate it to run at specific intervals daily.

Let’s keep it simple and run on our local machine

Run the following command to open your crontab file in the default editor:
```crontab -e```

Add your desired cron job at the bottom of the file or modify an existing one. For example:

```0 0 * * * /path/to/your/script.sh```

This will run the script at the top of every day
