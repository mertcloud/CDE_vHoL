FROM python:3.8-slim

# set up cde user and home directory
RUN useradd -rm -d /home/cdeuser -s /bin/bash -g users -u 1001 cdeuser
WORKDIR /home/cdeuser

# add the cde cli binary & make it executable
ADD cde .
RUN chmod +x cde

# change to cde user w/o root & set alias for cde
USER cdeuser
RUN echo 'alias cde=~/cde' >> ~/.bashrc

# create $HOME/.cde directory to enable config.yaml auto discovery
# https://docs.cloudera.com/data-engineering/cloud/cli-access/topics/cde-cli-config-options.html
RUN mkdir .cde
