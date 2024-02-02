
## Ubuntu

Notes:

- We build the `5.3.8` version of taudem explicitly here because that's what we've always done. 

```bash
# Now we build TauDEM
# Prerequisites first:
sudo apt-get update && sudo apt-get install -y cmake mpich \
    libgtest-dev libboost-test-dev libnetcdf-dev
# Clone the right version of TauDEM
git clone --depth 1 -b v5.3.8 https://github.com/dtarb/taudem.git ~/code/taudem
## Compile Main taudem repo ##
mkdir -p  ~/code/taudem/bin
cd ~/code/taudem/src
make
```

## Steps for compiling Taudem on OSX

- For compiling on M1 Macs I was not able to get ` v5.3.8` to compile. I was able to compile the latest code on the repo though

First off you're going to need to install a few prerequisites

```bash
brew install gdal mpich cmake
```

then clone the repo somewhere:

```bash
git clone git@github.com:dtarb/TauDEM.git /wherever/TauDEM
```

Now you're ready to build

```bash
cd /wherever/TauDEM
# Make a build folder
mkdir build
cd build

# Now do the build...
cmake ../src/
make
sudo make install
```

Then you can see if it's installed. You may need to add something to your `$PATH` variable to get it to work anywhere but this worked for me out of the box:

```bash
which dinfavalanche
# /usr/local/taudem/dinfavalanche
```