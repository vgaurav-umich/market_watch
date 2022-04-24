market_watch
==============================

Capstone project for UMSI MADS program. RL bot for stock trading.

Project Organization
------------

    ├── LICENSE
    ├── README.md          <- The top-level README for developers using this project.
    │
    ├── docs               <- A default Sphinx project; see sphinx-doc.org for details
    │
    ├──| output
    │   ├── noteboooks        <- executed notebooks form the tasks
    │   └── model             <- trained Model 
    |   └── data
    │      ├── interim        <- Intermediate data that has been transformed.
    │      └── raw            <- The original, immutable data dump.
    │
    ├── references         <- Data dictionaries, manuals, and all other explanatory materials.
    │
    ├── requirements.txt   <- The requirements file for reproducing the analysis environment, e.g.
    │                         generated with `pip freeze > requirements.txt`
    │
    ├── setup.py           <- makes project pip installable (pip install -e .) so src can be imported
    ├── src                <- Source code for use in this project.
    │   ├── __init__.py    <- Makes src a Python module
    │   │
    │   ├── utils.py       <- Utility functions
    │   │
    │   ├── data                              <- Scripts to download or generate data
    │   │   └── fetch_n_filter_Gdelt_bq.py    <- Script to download and filter GDELT data from bigquery
    │   │   └── clean_gdelt_data.py           <- Script to clean GDELT data, like org name normalization
    │   │   └── fetch_all_securities_list.py  <- Script to download list of all publicly traded companies in United States
    │   │   └── fetch_securities.py           <- Script to download extra metadata about each security, like former name and list of filings. 
    │   │   └── fetch_n_filter_Gdelt_bq.py    <- Script to download and filter GDELT data from bigquery
    │   │   └── fetch_yfinance_data.py        <- Script to download High, Open , Close, Volume info about each public company on a copnfigurable window
    │   │   └── normalize_security_names.py   <- Script to normalize security names, so that it can be matched with normalized gdelt org names
    │   │   └── clean_gdelt_data.py           <- Script to clean gdelt orga data, like normalization of org names
    │   │   └── fetch_fred.py                 <- Script to download FRED economic indicator data
    │   │   └── combine_fred_yahoo.py         <- Script to copmbine stock performance and fred economic data
    │   │   └── total_org_counts.py           <- Script to match orga names with security names and count their occurance 
    │       ├── __init__.py    <- Makes data a Python module
    │   │
    │   ├── features                          <- Scripts to turn raw data into features for modeling
    │   │   └── create_tfidf_scores.py        <- Scripts to turn total org counts into a TF-IDF matrics to be used as feature weigths  
    │       ├── __init__.py                   <- Makes features a Python module
    │   │
    │   ├── models                  <- Scripts to train models and then use trained models to make predictions
    │   │   └── train_model.py
        │   │   └── fetch_n_filter_Gdelt_bq.py    <- Script to download and filter GDELT data from bigquery
    │   │   └── clean_gdelt_data.py             <- Script to clean GDELT data, like org name normalization
    │   │   └── lib                             <- utility functions to support model tranining  
    │   │       └── data.py             <- Script to 
    │   │       └── models.py           <- Script to 
    │   │       └── environ.py          <- Script to 
    │   │       └── validation.py       <- Script to 
    │   │       └── common.py           <- Script to 
    │           ├── __init__.py         <- Makes lib a Python module    
    │       ├── __init__.py             <- Makes models a Python module
    │   │
    │   └── visualization           <- Scripts to create exploratory and results oriented visualizations
    │       └── visualize_gdelt.py     <- Scripts to create exploratory and results oriented visualizations for gdelt data
    │       └── visualize_yfinance.py  <- Scripts to create exploratory and results oriented visualizations for yahoo data
    │
    └── tox.ini            <- tox file with settings for running tox; see tox.readthedocs.io
    │
    └── Pipfile            <- Simillar to requirements.txt. This file is used by pipenv to build venv
    │
    └── pipeline.yaml       <- DAG file used by ploomber to build tasks and generate output from individual script execution
    │
    └── env.yaml           <- configuration file for parameter externalization
    │
    └── pipeline.features.yaml     <- Simillar to pipeline.yaml, but only for feature tasks. The main idea is that 
    |                                 we can reuse this file between train and serving tasks
    │
    └── market_watch.env    <- environment file for OS level environment variable declarations 
    │
    └── Dockerfile          <- Docker image manifest
    
<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>

--------

## Get Started with Docker

- Ensure you have Docker or Docker desktop installed. See web for details to install docker on your machine
- you can donwload the git repo locally or run build from github URL directly. Since ti is a development image I'll be using build from github directly.
- Before we build, first download the secret files like - git hub ssh key file and google cloud service key files to `~/.ssh` directopry.
- Build docker image 
  ```
  docker build https://github.com/vgaurav-umich/market_watch.git#main --build-arg PNAME=market_watch -t veenagaurav/market_watch:rc1
  ```
- or Download a pre-built image
  ```
  docker pull veenagaurav/market_watch:rc1
  ```
 - docker run for GPU enabled machines
    ```
     docker run --gpus all -it -p 8888:8888 veenagaurav/market_watch:rc1
    ```
  - docker run for non GPU enabled machines
    ```
     docker run -it -p 8888:8888 veenagaurav/market_watch:rc1
    ```
#### Notes
- The -p option does port mapping to enable jupyter notebook access. We are using --gpus flag to indicate that container should be able to use all available gpus from host machine. See this [article](https://docs.nvidia.com/cuda/wsl-user-guide/index.html#:~:text=Getting%20started%20with%20running%20CUDA%20on%20WSL%20requires,that%20you%20install%20Build%20version%2020145%20or%20higher.?msclkid=006e44f5c1ae11ec907e2c04a54f448a) for help related to CUDA enablement on WSL.
    
- Once you are in Docker. issue following command to start jupyter server.
    `jupyter notebook --ip 0.0.0.0 --no-browser --allow-root`
- On the HOST machine access jupyter notebooks by visting the URL. For more info see this [article](https://stackoverflow.com/questions/38830610/access-jupyter-notebook-running-on-docker-container?msclkid=bdd29106c00011ecbd22cd2a0b9cf245)
    
- You will find a utility shell script that will copy your local credentials, like - key files used to authenticate with Github, Google Cloud service key file etc. from host machine to docker container. This is needed to run sucessfull ploomber build. For more details on environment variable and docker see this [article](https://aggarwal-rohan17.medium.com/docker-build-arguments-and-environment-variables-1bdca0c0ef92#:~:text=Docker%20environment%20variables%20are%20used%20to%20make%20the,be%20accessed%20in%20the%20application%20code%20as%20well.)

## Ploomber

Ploomber is a better and more function rich version of the Makefile. At the very core it does what Makefile’s do i.e. execute individual tasks/scripts in order of dependency between them. 
The Docker image already comes preloaded with Ploomber. 

To get started with Ploomber we need run Ploomber build with this command –
* Please pip install ploomber if not already done.
```
Ploomber build
```
Above command will make use of pipeline.yaml file which is a ploomber’s equivalent of Makefile which contains data pipeline specification to build each individual tasks.  

Our full pipeline is found in the pipeline.yaml file which ploomber uses. It is a convenient way to walk step by step thru the code.

