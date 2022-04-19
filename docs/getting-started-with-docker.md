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
 - docker run
    ```
     docker run -it -v ~/market_watch:/root/market_watch -p 8888:8888 veenagaurav/market_watch:rc1
    ```
    replace ~/market_watch after -v option with your local folder. -p option does port mapping to enable jupyter notebook access.
    
    Once you are in Docker. issue following command to start jupyter server.
    `jupyter notebook --ip 0.0.0.0 --no-browser --allow-root`
    On the HOST machine access jupyter notebooks by visting the URL. For more info see this [article](https://stackoverflow.com/questions/38830610/access-jupyter-notebook-running-on-docker-container?msclkid=bdd29106c00011ecbd22cd2a0b9cf245)
    
You will find a utility shell script that will copy your local credentials from host machine to docker container. This is needed to run sucessfull ploomber build.

for more details on environment variable and docker see this [article](https://aggarwal-rohan17.medium.com/docker-build-arguments-and-environment-variables-1bdca0c0ef92#:~:text=Docker%20environment%20variables%20are%20used%20to%20make%20the,be%20accessed%20in%20the%20application%20code%20as%20well.)
