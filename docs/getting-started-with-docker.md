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
  docker run -it --rm gaurav/devbox
  ```
