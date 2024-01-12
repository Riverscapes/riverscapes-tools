############################################################################################
# Dev Container for riverscapes-tools
############################################################################################
FROM mcr.microsoft.com/devcontainers/python:3.11 AS worker

# Some preliminary dependencies
RUN apt update && apt upgrade -y && apt autoremove -y

RUN apt-get install -y curl wget git build-essential libssl-dev gcc g++ make zsh vim \
                software-properties-common awscli locales

# Python stuff
RUN apt-get install -y python3-pip proj-data proj-bin \
              gdal-bin libgdal-dev libspatialindex-dev \
              python3-matplotlib

# Install Node18
RUN curl -fsSL https://deb.nodesource.com/setup_18.x | sudo -E bash -
RUN apt-get install -y nodejs
# Install Gatsby CLI and Yarn
RUN npm install -g gatsby-cli
RUN npm install -g @riverscapes/cli

# Let's get the latest version of Yarn
RUN corepack enable
RUN yarn set version berry

# Now we build TauDEM
# Prerequisites first:
RUN apt-get update && apt-get install -y cmake mpich \
    libgtest-dev libboost-test-dev libnetcdf-dev \
    && rm -rf /var/lib/apt/lists/* 

# Now we build TauDEM
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

RUN git clone --depth 1 --branch ${taudemVersion} \
    https://github.com/dtarb/taudem.git $taudemSrcDir

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

# Now follow up on python install
RUN pip install --upgrade pip
RUN pip install numpy==1.23.4 Cython==0.29.23

# https://bobbyhadz.com/blog/python-error-in-package-setup-command-use-2to3-is-invalid
RUN pip install "setuptools<58.0"
# Now install the right version of gdal from the apt-get above
RUN pip install GDAL==$(gdal-config --version)

# Now install any lingering dependencies
COPY requirements.docker.txt /workspaces/requirements.txt
RUN pip --timeout=120 install -r /workspaces/requirements.txt

# Now install  18 
USER vscode
RUN wget https://raw.githubusercontent.com/Riverscapes/environment/master/nar-ys.zsh-theme -O ~/.oh-my-zsh/custom/themes/nar-ys.zsh-theme
RUN wget https://raw.githubusercontent.com/Riverscapes/environment/master/.aliases -O ~/.aliases
RUN wget https://raw.githubusercontent.com/Riverscapes/environment/master/.zshrc -O ~/.zshrc

# set memory to 8Gb so that Gatsby has a chance
ENV NODE_OPTIONS=--max-old-space-size=8192
ENV GATSBY_CPU_COUNT=2
ENV GATSBY_TELEMETRY_DISABLED=1


WORKDIR /workspaces/riverscapes-tools
