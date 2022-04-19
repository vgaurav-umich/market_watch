#!/bin/bash

read -p "Enter Docker Container Image ID (HINT: docker ps -a):" image
sudo docker cp .ssh/id_rsa_github_umich $image:/root/.ssh
sudo docker cp .ssh/id_rsa_github_umich.pub $image:/root/.ssh
sudo docker cp .ssh/vgaurav-4d0e95d3663a.json $image:/root/.ssh
sudo docker cp .ssh/config $image:/root/.ssh
