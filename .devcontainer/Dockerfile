############################################################
# Developer environment for Riverscapes Tools
# This environment is based off of what cybercastor uses
# 
############################################################

# Now swap over to the new machine
FROM osgeo/gdal:ubuntu-full-3.3.1 AS worker
ARG DEBIAN_FRONTEND=noninteractive

## Set user:group for running docker in detached mode
RUN groupadd --gid 5000 vscode \
    && useradd --home-dir /home/vscode --create-home --uid 5000 \
    --gid 5000 --shell /bin/zsh --skel /dev/null vscode

# Installing Prerequisite Packages
# =======================================================================================
RUN apt-get update && apt-get install vim git awscli curl locales -y

# Our python scripts use UTF-8 so let's make sure we're in the right locale
RUN locale-gen en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8

################################################################################################
# TauDEM as a multi-stage install: DO all our compiling on a multi-stage build
# Method borrowed from: https://github.com/NOAA-OWP/cahaba/blob/dev/Dockerfile.prod
# https://hub.docker.com/r/osgeo/gdal/tags?page=1&ordering=last_updated&name=ubuntu-full-3.2.1
################################################################################################
FROM worker AS builder
WORKDIR /opt/builder
ARG dataDir=/data
ARG projectDir=/foss_fim
ARG taudemVersion=v5.3.8
ARG taudemSrcDir=/opt/builder/taudem
ENV DEBIAN_FRONTEND noninteractive
ENV taudemDir=/dependencies/taudem/bin

RUN apt-get update && apt-get install -y git  && rm -rf /var/lib/apt/lists/* 

RUN git clone --depth 1 --branch ${taudemVersion} \
    https://github.com/dtarb/taudem.git $taudemSrcDir

RUN apt-get update && apt-get install -y cmake mpich \
    libgtest-dev libboost-test-dev libnetcdf-dev \
    && rm -rf /var/lib/apt/lists/* 

## Compile Main taudem repo ##
RUN cd taudem/src && make
# Now put the binaries somewhere we can find easily
RUN mkdir -p $taudemDir
RUN cp -a ${taudemSrcDir}/* ${taudemDir}/


################################################################################################
# Now switch back to the worker and Copy the builder's scripts over
################################################################################################
FROM worker

ARG depDir=/dependencies
ARG taudemDir=$depDir/taudem/bin
RUN mkdir -p $depDir
COPY --from=builder $depDir $depDir

# Add TauDEM to the path so we have access to those methods
ENV PATH="${taudemDir}:${PATH}"

# Let's try adding gdal this way
# RUN add-apt-repository ppa:ubuntugis/ppa && apt-get update
RUN apt update --fix-missing
RUN apt install -y p7zip-full python3-pip time mpich=3.3.2-2build1 python3.8-venv \
  parallel=20161222-1.1 libgeos-dev=3.8.0-1build1 expect=5.45.4-2build1 libspatialindex-dev
RUN apt auto-remove

# https://launchpad.net/~ubuntugis/+archive/ppa/
# Install node please. Order matters here so it should come after all other apt-get update steps or it may be removed by a cleanup process
RUN curl -sL https://deb.nodesource.com/setup_12.x | bash -
RUN apt-get install -y nodejs
RUN npm install -g npm
RUN node --version
RUN npm --version

# This is the last command we need to run as root
RUN apt-get install zsh -y
# Pull in the python libraries. We build it in a different stage to keep the id_rsa private
# =======================================================================================
USER vscode
WORKDIR /home/vscode
# Install oh-my-zsh for better console experience
RUN set -uex; \
    wget https://raw.githubusercontent.com/robbyrussell/oh-my-zsh/master/tools/install.sh; \
    sh ./install.sh; \
    rm ./install.sh

# Now set up our developer environment
RUN wget https://raw.githubusercontent.com/MattReimer/environment/master/nar-ys.zsh-theme -O ~/.oh-my-zsh/custom/themes/nar-ys.zsh-theme
RUN wget https://raw.githubusercontent.com/MattReimer/environment/master/.aliases -O ~/.aliases
RUN wget https://raw.githubusercontent.com/MattReimer/environment/master/.zshrc -O ~/.zshrc

# Make a virtualenv for opengis
ARG VENV_DIR=/home/vscode/venv
ARG VENV_PYTHON=$VENV_DIR/bin/python
ARG VENV_PIP=$VENV_DIR/bin/pip

# Create our venv for python
RUN python3 -m venv $VENV_DIR

# Upgrade pip to latest
RUN $VENV_PYTHON -m pip install --upgrade pip

# Install some tricky prerequisites
RUN $VENV_PIP install Cython==0.29.23 numpy==1.21.0 shapely==1.7.1 scipy==1.5.1 Rtree==0.9.4 --no-binary shapely
  
# Install GDAL AFTER NUMPY 
RUN $VENV_PIP install \
  GDAL==$(gdal-config --version) \
  --global-option=build_ext \
  --global-option="-I/usr/include/gdal"
RUN $VENV_PIP install pygeoprocessing==2.1.0 --no-binary pygeoprocessing

# Now install riverscapes CLI
USER root
ARG CACHEBREAKER3=121
RUN npm -v
RUN npm install -g @riverscapes/cli

# Set up a folder for our code space
USER vscode
