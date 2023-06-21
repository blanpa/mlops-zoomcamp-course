## Batch deployment

* Turn the notebook for training a model into a notebook for applying the model
* Turn the notebook into a script 
* Clean it and parametrize

    pipenv --python=3.9
    pipenv install prefect==2.6 mlflow scikit-learn==2.0b6 pandas boto3

    python score.py green 2021 04 8bb01fbcd9ab42ae9ba041b3d28788ad

    python score_deploy.py

    python score_backfill.py