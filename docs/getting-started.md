Getting started
===============

1. Watch this [Video](https://www.youtube.com/watch?v=5HUL5lWkEyg) about how to use pipenv. We will be making use of  `pipenv` to create virtual environment.
2. Upgrade pip or pip3 and install pyenv
    ```
     pip install --upgrade pip
     pip install pyenv
     pip install --user --upgrade pipenv
    ```
3. Find user’s base directory
    `python -m site --user-base`
4. export this to PATH variable
    `export PATH="$PATH:<User Base Directory>/bin`
5. To make pipenv available in current session - Add above export statement to ~/.bashrc file. `echo 'export PATH="$PATH:<User Base Directory>/bin' >> ~/.bashrc`
6. Run `source ~/.bashrc` and also add this statement to ~/.bash_profile file. `echo "source ~/.bash_rc" >> ~/.bash_profile`
7. 

