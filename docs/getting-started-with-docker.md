- Ensure you have Docker or Docker desktop installed. See web for details to install docker on your machine
- Git clone repo
- Download the secret files like - git hub ssh key file and google cloud service key files to `/tmp` directopry.
- From there copy those secrets to following location
  ```
  mv /tmp/vgaurav-4d0e95d3663a.json ~/market_watch/screts/
  ```
- Switch to project directory
- Build docker image 
  ```
  docker build . -t devbox
  ```
- or Download a pre-built image
  ```
  docker pull veenagaurav/devbox
  ```
 - docker run
  ```
  docker run -it --rm --env-file ./market_watch.env veenagaurav/devbox
  ```
for more details on environment variable and docker see this [article](https://aggarwal-rohan17.medium.com/docker-build-arguments-and-environment-variables-1bdca0c0ef92#:~:text=Docker%20environment%20variables%20are%20used%20to%20make%20the,be%20accessed%20in%20the%20application%20code%20as%20well.)
