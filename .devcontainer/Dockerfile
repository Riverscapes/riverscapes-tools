############################################################################################
# Dev Container for riverscapes-tools
############################################################################################
FROM mcr.microsoft.com/devcontainers/python:3.10

# Some preliminary dependencies
RUN apt update && apt upgrade -y && apt autoremove -y

RUN apt-get install -y curl wget git build-essential libssl-dev gcc g++ make zsh vim \
                software-properties-common awscli locales

# Python stuff
RUN apt-get install -y python3-pip proj-data proj-bin \
              gdal-bin libgdal-dev libspatialindex-dev \
              python3-matplotlib

# Now we build TauDEM
# Prerequisites first:
RUN apt-get install -y cmake mpich libgtest-dev libboost-test-dev libnetcdf-dev

# Clone the right version of TauDEM
# https://cran.r-project.org/web/packages/traudem/vignettes/taudem-installation.html
RUN git clone --depth 1 -b v5.3.8 https://github.com/dtarb/taudem.git /workspaces/taudem
# Compile Main taudem repo ##
RUN mkdir -p  /workspaces/taudem/bin
WORKDIR /workspaces/taudem/bin

# Configure with CMAKE
RUN cmake ../src
RUN make && sudo make install

RUN pip install --upgrade pip
RUN pip install numpy==1.23.4 Cython==0.29.23

# https://bobbyhadz.com/blog/python-error-in-package-setup-command-use-2to3-is-invalid
RUN pip install "setuptools<58.0"
# Now install the right version of gdal from the apt-get above
RUN pip install GDAL==$(gdal-config --version)

# Now install any lingering dependencies
COPY requirements.docker.txt /workspaces/requirements.txt
RUN pip --timeout=120 install -r /workspaces/requirements.txt

WORKDIR /workspaces/riverscapes-tools
