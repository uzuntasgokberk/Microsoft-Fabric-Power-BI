#!/usr/bin/env python
# coding: utf-8

# ## Scrape Currency Data with Python and Store in Microsoft Fabric
# 
# New notebook

# In[1]:


import requests
from bs4 import BeautifulSoup
import pandas as pd
import datetime as dt
#Spark Connector
import com.microsoft.spark.fabric
from com.microsoft.spark.fabric import Constants

URL = "https://bigpara.hurriyet.com.tr/doviz/"

def fetch_rates() -> pd.DataFrame:
    """
    Fetch the currency page, extract USD/EUR/GBP values, and return
    a single-row pandas DataFrame with timestamps.
    """
    #Request the page (raise on HTTP errors)
    r = requests.get(URL, headers=HDRS, timeout=30)
    r.raise_for_status()

    #Parse HTML with an explicit parser
    soup = BeautifulSoup(r.text, "html.parser")

    #Select all spans that hold currency values (site-specific)
    spans = soup.select("span.value")
    if len(spans) < 9:
        raise ValueError("Expected at least 9 currency elements (span.value).")

    #Extract numbers by position (matches your original logic)
    usd = spans[2].get_text(strip=True)
    eur = spans[5].get_text(strip=True)
    gbp = spans[8].get_text(strip=True)

    #Build a one-row DataFrame with dates
    now = dt.datetime.now()
    today = now.date()
    df = pd.DataFrame([{
        "ABD_Doları": usd,
        "EURO": eur,
        "STERLIN": gbp,
        "Date": today,
        "Date_Time": now
    }])
    return df

def write_fabric(df: pd.DataFrame):
    #Write the DataFrame into Microsoft Fabric (Lakehouse + Warehouse).
    # Convert pandas to Spark
    spark_df = spark.createDataFrame(df)

    #Lakehouse (Delta-Parquet format). Prefer 'append' for ETL runs
    spark_df.write.format("delta").mode("append").save(
        "abfss://DevelopmentWS@onelake.dfs.fabric.microsoft.com/SampleLH.Lakehouse/Tables/web_currency"
    )

    #Warehouse table (append new rows)
    spark_df.write.mode("append").synapsesql("SampleWH.dbo.web_currency")

def main():
    #Fetch & print
    web_currency = fetch_rates()
    print(web_currency)

    #try to write to Lakehouse/Warehouse
    try:
        write_fabric(web_currency)
        print("✔ Write completed.")
    except NameError:
        #Error
        print("Error")

if __name__ == "__main__":
    main()

