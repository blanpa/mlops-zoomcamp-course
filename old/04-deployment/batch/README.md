## Batch deployment

* Turn the notebook for training a model into a notebook for applying the model
* Turn the notebook into a script 
* Clean it and parametrize

    pipenv --python=3.9
    pipenv install prefect==2.6 mlflow scikit-learn==2.0b6 pandas boto3

    python score.py green 2021 04 8bb01fbcd9ab42ae9ba041b3d28788ad

    python score_deploy.py

    python score_backfill.py

    Updated Pipfile.lock (a688fce454e43fe3bb580d96447c45e3f513e42b1b469b15c43591b4bc8e23e2)!
Installing dependencies from Pipfile.lock (8e23e2)...
To activate this project's virtualenv, run pipenv shell.
Alternatively, run a command inside the virtualenv with pipenv run.