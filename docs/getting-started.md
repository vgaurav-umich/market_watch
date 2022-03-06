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

## Development and build
1. You can use your choice of code editor and Jupyter Notebook. First make sure you open this project into IDE and run following - 
    ```
   pipenv install -e . # builds local package and execute setup.py
   ploomber build # installs all python package dependecies
   ```
2. Now Open Terminal window from IDE and run - `jupyter lab` command
3. In Jupyter lab, you will see typical `.py` file. Right-click on a `.py` file and select "Open As --> Notebook". Ploomber converts .py file into notebook and magically inserts upstream and product variables to make build easier.
4. Add a new `.py` file NOT `.ipynb` if needed and open it as Notebook as explained in previous step. Make sure to add a task in to `pipline.yaml`
5. 


