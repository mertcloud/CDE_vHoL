FROM python:3.8-slim

RUN useradd -rm -d /home/cdeuser -s /bin/bash -g users -u 1001 cdeuser
WORKDIR /home/cdeuser

ADD cde .
RUN chmod +x cde

USER cdeuser
RUN echo 'alias cde=~/cde' >> ~/.bashrc
RUN mkdir .cde
