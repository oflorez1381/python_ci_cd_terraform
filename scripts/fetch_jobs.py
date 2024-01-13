#!/usr/bin python3
"""
This is the main file for the py_cloud project. It can be used at any situation
"""
import os
from collections import ChainMap

import boto3
import pandas as pd
import requests
import toml
from dotenv import load_dotenv


def read_api(url):
    """
    Reads the API and returns the response
    """
    print("Reading the API...")
    response = requests.get(url)
    return response


def output_jobs(response, output_file_path=""):
    response = response.json()
    # the company name
    print("Building the dataframe...")
    company_list = [
        response["results"][i]["company"]["name"]
        for i in range(len(response["results"]))
    ]
    company_name = {"company": company_list}

    # the locations
    location_list = [
        response["results"][i]["locations"][0]["name"]
        for i in range(len(response["results"]))
    ]
    location_name = {"locations": location_list}

    # the job name
    job_list = [response["results"][i]["name"] for i in range(len(response["results"]))]
    job_name = {"job": job_list}

    # the job type
    job_type_list = [
        response["results"][i]["type"] for i in range(len(response["results"]))
    ]
    job_type = {"job_type": job_type_list}

    # the publication date
    publication_date_list = [
        response["results"][i]["publication_date"]
        for i in range(len(response["results"]))
    ]
    publication_date = {"publication_date": publication_date_list}

    # merge the dictionaries with ChainMap and dict "from collections import ChainMap"
    data = dict(
        ChainMap(company_name, location_name, job_name, job_type, publication_date)
    )
    df = pd.DataFrame.from_dict(data)

    # Cut publication date to date
    df["publication_date"] = df["publication_date"].str[:10]

    # split location to city and country and drop the location column
    df["city"] = df["locations"].str.split(",").str[0]
    df["country"] = df["locations"].str.split(",").str[1]
    df.drop("locations", axis=1, inplace=True)

    # save the dataframe to a csv file locally first
    df.to_csv(output_file_path, index=False)
    print(f"datafrme saved to local file output_file_path")


def upload_to_s3(source_path, bucket, destination):
    # read secret_access_key of AWS form the .env file
    print("uploading to AWS S3...")
    load_dotenv()
    access_key = os.getenv("access_key")
    secret_access_key = os.getenv("secret_access_key")

    s3_client = boto3.client(
        "s3", aws_access_key_id=access_key, aws_secret_access_key=secret_access_key
    )
    s3_client.upload_file(source_path, bucket, destination)
    print("File uploading Done!")


# main function
if __name__ == "__main__":
    file_path = "data/output_jobs.csv"

    app_config = toml.load("config/config.toml")
    url = app_config["api"]["url"]
    bucket = app_config["aws"]["bucket"]
    folder = app_config["aws"]["folder"]

    response = read_api(url=url)

    output_jobs(response=response, output_file_path=file_path)

    upload_to_s3(source_path=file_path, bucket=bucket, destination=folder + file_path)
