- Ensure you have Docker or Docker desktop installed. See web for details to install docker on your machine
- you can donwload the git repo locally or run build from github URL directly. Since ti is a development image I'll be using build from github directly.
- Before we build, first download the secret files like - git hub ssh key file and google cloud service key files to `~/.ssh` directopry.
- Build docker image 
  ```
  docker build https://github.com/vgaurav-umich/market_watch.git#main -t veenagaurav/market_watch
  ```
- or Download a pre-built image
  ```
  docker pull veenagaurav/market_watch
  ```
 - docker run
    ```
    docker run -it --rm -v ~/.ssh:/root/.ssh veenagaurav/market_watch
    ```
for more details on environment variable and docker see this [article](https://aggarwal-rohan17.medium.com/docker-build-arguments-and-environment-variables-1bdca0c0ef92#:~:text=Docker%20environment%20variables%20are%20used%20to%20make%20the,be%20accessed%20in%20the%20application%20code%20as%20well.)
