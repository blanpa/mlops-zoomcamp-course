#!/usr/bin/env python
# coding: utf-8

import os
import sys

import uuid
import pickle

from datetime import datetime

import pandas as pd

import mlflow

from prefect import task, flow, get_run_logger
from prefect.context import get_run_context

from dateutil.relativedelta import relativedelta

from sklearn.feature_extraction import DictVectorizer
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error
from sklearn.pipeline import make_pipeline


def read_dataframe(filename: str):
    categorical = ['PULocationID', 'DOLocationID']
    df = pd.read_parquet(filename)
    
    df['duration'] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60
    

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()
    

    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')
    
    return df

def load_model():
    logged_model = ("model.bin")
    with open(logged_model, 'rb') as f_in:
        dv, model = pickle.load(f_in)
    return dv, model


def save_results(df, y_pred, output_file, year: str, month: str):
    df_result = pd.DataFrame()
    df_result['ride_id'] = f'{year:04d}/{month:02d}_' + df.index.astype('str')
    df_result['predicted_duration'] = y_pred

    df_result.to_parquet(
            output_file,
            engine='pyarrow',
            compression=None,
            index=False)


@task
def apply_model(input_file, output_file, year, month):
    logger = get_run_logger()

    logger.info(f'reading the data from {input_file}...')
    df = read_dataframe(input_file)
    categorical = ['PULocationID', 'DOLocationID']
    dicts = df[categorical].to_dict(orient='records')

    logger.info(f'loading the model...')
    dv, model = load_model()
    X_val = dv.transform(dicts)

    logger.info(f'applying the model...')
    y_pred = model.predict(X_val)

    logger.info(f'saving the result to {output_file}...')

    save_results(df, y_pred, output_file, year, month)
    return output_file


def get_paths(run_date, taxi_type):
    prev_month = run_date #- relativedelta(months=1)
    year = prev_month.year
    month = prev_month.month 

    input_file = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year:04d}-{month:02d}.parquet"

    output_file = f'./output/{taxi_type}/{year:04d}-{month:02d}.parquet'

    return input_file, output_file

@flow
def ride_duration_prediction(
        taxi_type: str,
        run_date: datetime = None):
    if run_date is None:
        ctx = get_run_context()
        run_date = ctx.flow_run.expected_start_time
    
    input_file, output_file = get_paths(run_date, taxi_type)

    apply_model(
        input_file=input_file,
        output_file=output_file,
        year = run_date.year, 
        month = run_date.month
    )

def run():
    taxi_type = sys.argv[1] # 'green'
    year = int(sys.argv[2]) # 2021
    month = int(sys.argv[3]) # 3


    print(taxi_type, year, month)

    ride_duration_prediction(
        taxi_type=taxi_type,
        run_date=datetime(year=year, month=month, day=1)
    )


if __name__ == '__main__':
    run()




