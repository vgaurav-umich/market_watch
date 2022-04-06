# syntax = docker/dockerfile:1.3
FROM python:3.8-bullseye

ARG PNAME=/root/code

RUN apt update && apt upgrade -y
RUN pip install --user --upgrade pipenv
RUN export PATH="$PATH:/root/.local/bin"
RUN echo 'export PATH="$PATH:/root/.local/bin"' >> ~/.bashrc
RUN echo "source ~/.bash_rc" >> ~/.bash_profile
RUN apt install git -y
RUN apt install nano
RUN pip install ploomber jupyterlab

COPY . /root/$PNAME/

ENTRYPOINT ["bash"]
