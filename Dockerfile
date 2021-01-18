############################################################################
# RSTools Dockerbox
# Author: Matt Reimer
# Description: A working linux box that can run our riverscapes-tools
############################################################################

# We use OSGEO's ubuntu box as a base so we don't need to compile GDAL ourselves
FROM osgeo/gdal:ubuntu-full-3.1.2 AS WORKER
ARG DEBIAN_FRONTEND=noninteractive
ARG CACHEBUST=1
RUN apt-get update

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

FROM WORKER AS BUILDER
WORKDIR /opt/builder
ARG dataDir=/data
ARG projectDir=/foss_fim
ARG depDir=/dependencies
ARG taudemVersion=bf9417172225a9ce2462f11138c72c569c253a1a
ARG taudemVersion2=81f7a07cdd3721617a30ee4e087804fddbcffa88
ENV DEBIAN_FRONTEND noninteractive
ENV taudemDir=$depDir/taudem/bin
ENV taudemDir2=$depDir/taudem_accelerated_flowDirections/taudem/build/bin

RUN apt-get update && apt-get install -y git  && rm -rf /var/lib/apt/lists/* 

RUN git clone https://github.com/dtarb/taudem.git
RUN git clone https://github.com/fernandoa123/cybergis-toolkit.git taudem_accelerated_flowDirections

RUN apt-get update && apt-get install -y cmake mpich \
    libgtest-dev libboost-test-dev libnetcdf-dev && rm -rf /var/lib/apt/lists/* 

## Compile Main taudem repo ##
RUN mkdir -p taudem/bin
RUN cd taudem \
    && git checkout $taudemVersion \
    && cd src \
    && make

## Compile taudem repo with accelerated flow directions ##
RUN cd taudem_accelerated_flowDirections/taudem \
    && git checkout $taudemVersion2 \
    && mkdir build \
    && cd build \
    && cmake .. \
    && make

RUN mkdir -p $taudemDir
RUN mkdir -p $taudemDir2

## Move needed binaries to the next stage of the image
RUN cd taudem/bin && mv -t $taudemDir flowdircond aread8 threshold streamnet gagewatershed catchhydrogeo dinfdistdown pitremove
RUN cd taudem_accelerated_flowDirections/taudem/build/bin && mv -t $taudemDir2 d8flowdir dinfflowdir


################################################################################################
# Now switch back to the worker and Copy the builder's scripts over
################################################################################################
FROM WORKER

ARG depDir=/dependencies
ARG taudemDir=$depDir/taudem/bin
ARG taudemDir2=$depDir/taudem_accelerated_flowDirections/taudem/build/bin
RUN mkdir -p $depDir
COPY --from=builder $depDir $depDir

# Add TauDEM to the path so we have access to those methods
ENV PATH="${taudemDir}:${taudemDir2}:${PATH}"

# Let's try adding gdal this way
# RUN add-apt-repository ppa:ubuntugis/ppa && apt-get update
RUN apt update --fix-missing
RUN apt install -y p7zip-full python3-pip time mpich=3.3.2-2build1 parallel=20161222-1.1 libgeos-dev=3.8.0-1build1 expect=5.45.4-2build1 libspatialindex-dev
RUN apt auto-remove

# https://launchpad.net/~ubuntugis/+archive/ppa/
# Install node please. Order matters here so it should come after all other apt-get update steps or it may be removed by a cleanup process
RUN curl -sL https://deb.nodesource.com/setup_12.x | bash -
RUN apt-get install -y nodejs
RUN npm install -g npm
RUN node --version
RUN npm --version

# Add in the right version of pip and modules
# =======================================================================================
RUN python3 -m pip install --upgrade pip 
RUN python3 -m pip install virtualenv

# Now install riverscapes CLI
ARG CACHEBREAKER3=121
RUN npm -v
RUN npm install -g @riverscapes/cli

# Never run as root
ARG GroupName=nar
RUN addgroup $GroupName

## Set user:group for running docker in detached mode
USER root:$GroupName


# Pull in the python libraries. We build it in a different stage to keep the id_rsa private
# =======================================================================================
COPY . /usr/local/riverscapes-tools
WORKDIR /usr/local/riverscapes-tools
RUN ./scripts/bootstrap.sh

WORKDIR /shared

ENTRYPOINT [ "sh", "/usr/local/riverscapes-tools/bin/run.sh"]