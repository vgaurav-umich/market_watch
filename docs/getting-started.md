Getting started
===============

## Prereqs
0. Watch this [Video](https://www.youtube.com/watch?v=GH4TonRGL4o) to get started with Mac from scratch.
1. Watch this [Video](https://www.youtube.com/watch?v=5HUL5lWkEyg) about how to use pipenv. We will be making use of  `pipenv` to create virtual environment.
2. Upgrade pip or pip3 and install pyenv
    ```
     pip install --upgrade pip
     pip install pyenv
     pip install --user --upgrade pipenv
    ```
3. Find user’s base directory
    ```
    python -m site --user-base
   ```
4. export this to PATH variable
    ```
    export PATH="$PATH:<User Base Directory>/bin
   ```
5. To make pipenv available in current session - Add above export statement to ~/.bashrc file. 
    ```
    echo 'export PATH="$PATH:<User Base Directory>/bin' >> ~/.bashrc
   ```
6. Run 
   ```
    source ~/.bashrc
   ``` 
7. Add above statement to ~/.bash_profile file.
    ```
    echo "source ~/.bash_rc" >> ~/.bash_profile
    ```

## Working with Google Cloud
1. We will be using Service Account to connect to Google Cloud.
2. GCP [docs](https://cloud.google.com/docs/authentication/getting-started) lists clear steps to create and enable Service Account. Please check them out. For the purpose of this project I've already created a service account and generated a .json file containing private key.
3. A more detailed [BigQuery Python API](https://github.com/googleapis/python-bigquery) can be found here. See also APPI [Usage Guide](https://googleapis.dev/python/bigquery/latest/usage/queries.html)
4. Please contact me to obtain .json file that you can store in a safe place on your desktop. I typically put it inside `~/.ssh/`
5. Once you have .json file placed on a safe location. Add that path to market_watch.env file. 
   ```
   GOOGLE_APPLICATION_CREDENTIALS="<<path to .json file>>"
   ```

## Development and build
1. Watch a couple of videos on Ploomber on  and checkout ploomber.io website.  
2. You can use your choice of code editor and Jupyter Notebook. First make sure you open this project into IDE and run following - 
    ```
   pip install -e . # builds local package and execute setup.py
   pipenv install -e . # builds dependencies form Pipfile
   ploomber build # installs all python package dependecies
   ```
3. Now Open Terminal window from IDE and run - `jupyter lab` command
4. In Jupyter lab, you will see typical `.py` file. Right-click on a `.py` file and select "Open As --> Notebook". Ploomber converts .py file into notebook and magically inserts upstream and product variables to make build easier.
5. Add a new `.py` file NOT `.ipynb` if needed and open it as Notebook as explained in previous step. Make sure to add a task in to `pipline.yaml`


