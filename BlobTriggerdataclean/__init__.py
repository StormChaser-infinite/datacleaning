import logging
import pandas as pd
import numpy as np
import azure.functions as func
from azure.storage.blob import ContainerClient
import datetime as dt



def export_ouput(df_output):
    '''export the results into the outputs container'''
    output = df_output.to_csv()
    blobService = ContainerClient(account_url = "https://funcdemo.blob.core.windows.net", 
                                   credential= "e4MRGQUsGoQLwqIQw2pw5fEVSonqVoSpMJV1X0QSZ6gaYmXTaE6aLdz4n6a8BD18wmRa/qbSsU5I+AStB1JRKg==",
                                   container_name = "blobtrigger")
    file_name = 'cleanedinput'+ '.csv'
    blobService.upload_blob(file_name, output, overwrite=True, encoding='utf-8')

def convert_date(date_string, col_name):
    """convert the string to date format"""
    try:
        date_convert = dt.datetime.strptime(date_string, '%d/%m/%Y').date()
    except:
        try:
           date_convert = dt.datetime.strptime(date_string, '%d-%b-%y').date() 
        except:
            date_convert = None
    return date_convert

def main(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob \n"
                 f"Name: {myblob.name}\n")
    df_input = pd.read_csv(myblob)
    #### remove duplicated event ids
    df_import = df_input.drop_duplicates(subset='EventID', keep= 'first')

    date_cols = ['EventStartDate','EventEndDate','CTED','BalloonCTED',
                 'CapitalisedPeriodStartDate','StructuredPayment1Date','StructuredPayment2Date','NextRepaymentDate',
                 'InterestRateEffectDateAlteration1','InterestRateEffectDateAlteration2','InterestRateEffectDateAlteration3']
    num_cols = ['StructuredPayment1Amt', 'StructuredPayment2Amt', 'LoanCurrentAmtOwed', 'InterestRateInitital', 
                'InterestRateAlteration1','InterestRateAlteration2','InterestRateAlteration3' ]

    for i in df_import.columns:
        if i in date_cols:
            df_import[i] = df_import[i].apply(lambda x: convert_date(x, i))
        elif i in num_cols:
            df_import[i] = df_import[i].apply(lambda x: float(x))

    export_ouput(df_import)


