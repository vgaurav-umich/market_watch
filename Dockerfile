FROM ubuntu

RUN apt update && apt upgrade -y
RUN apt install python3 -y
RUN apt install python3-pip -y
RUN apt install python-is-python3
RUN pip install --user --upgrade pipenv
RUN export PATH="$PATH:/root/.local/bin"
RUN echo 'export PATH="$PATH:/root/.local/bin"' >> ~/.bashrc
RUN echo "source ~/.bash_rc" >> ~/.bash_profile
RUN apt install git -y
RUN pip install ploomber
RUN pip install jupyterlab

COPY . /opt/market_watch

ENTRYPOINT ["bash"]
