

prefect project init

prefect deploy --help

# prefect deploy orchestrate.py:main_flow -n taxi1 -p mlobs-zoomcamp-pool 

prefect deployment build orchestrate.py:main_flow -n taxi1 -p mlobs-zoomcamp-pool 

prefect agent start --pool mlobs-zoomcamp-pool --work-queue default

prefect worker  start -p lobs-zoomcamp-pool -t process

prefect deployment apply main_flow-deployment.yaml