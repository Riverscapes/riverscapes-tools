---
title: Installation
weight: 3
---

BRAT is written in [Python](http://python.org) and exclusively uses open source libraries. You do **not** need ArcGIS to run the latest version of BRAT. However, the cost of this open source freedom is that some of the open source libraries used can be tricky to install, especially on Microsoft Windows (see the [explanation at the bottom of this page](http://127.0.0.1:4001/sqlBRAT/Technical_Reference/installation.html#why-was-this-so-hard) as to why this is).

# Preparation

Before you perform the instructions below it is strongly recommended that you are familiar with Python [Virtual Environments](https://www.python.org/dev/peps/pep-0405/). This is a way to isolate a copy of Python together with all the necessary site packages and avoid conflicts with other uses of Python on your computer. It is invaluable when you need to ensure a specific version of your dependencies (as is the case with [GDAL](https://gdal.org/) for BRAT) or when you have multiple versions of Python on your computer (as is often the case if you have ArcGIS installed).

# 1. Python

[Python 3.7.4](https://www.python.org/downloads/release/python-374/) is required and the specific version matters! Preferably 64 bit, but 32 bit is OK too. Note that if you already have a copy of Python 2 or 3 installed that you can install 3.7.4 alongside these other versions and use a Virtual Environment to ensure that BRAT uses the correct version.

# 2. PIP

You will need [PIP](https://pypi.org/project/pip) installed for the Python 3.7.4 version that you are using. You should already have PIP if you installed a newer version of Python downloaded from Python.org but here are the [PIP installation instructions](https://pip.pypa.io/en/stable/installing/) that show how to verify if this is the case.

# 3. Text Editor

BRAT is a command line tool. It does not currently have a user interface. We strongly recommend a good text editor with Python development tools to run BRAT. We recommend [Visual Studio Code](https://code.visualstudio.com) or [pyCharm](https://www.jetbrains.com/pycharm) Community edition, both of which are free.

# 4. Desktop GIS

BRAT is a spatially explicit model and you will want desktop GIS software to visualize the data. If you have have ArcGIS then you should also install the [RAVE AddIn](https://rave.riverscapes.net/) that will automate viewing BRAT projects and layers. However, you can also use the free and open source [QGIS](https://www.qgis.org/en/site/) and symbolize the map layers yourself.

# 5. Path Variables

Add the following folder locations to your PATH environment variable. The popular [Rapid Environment Editor](https://www.rapidee.com/en/about) is a useful tool with which to do this. Be sure to substitute your username wherever there are angular braces below:

```
C:\Users\<YOUR_USERNAME>\AppData\Roaming\Python\Python37\Scripts
C:\Users\<YOUR_USERNAME>\AppData\Roaming\Python\Python37
```

# 6. Create a Virtual Environment

First make sure that the Python virtual environment tools are installed by typing the following command in a DOS prompt.

```
python -m pip install virtualenv --user
```

Now create a virtual environment by typing the following:

```
virtualenv --no-site-packages .venv
```

# 7. Python Site Packages

Open a command prompt and ensure that you are using the virtual environment that you just created. Here are the instructions to do this in Visual Studio Code:

1. Open Visual Studio Code
1. Click the Windows Key + SHIFT + P to open the [command palette](https://code.visualstudio.com/docs/getstarted/tips-and-tricks#_command-palette)
1. Start typing `python: s` and choose the "Select Interpreter option"
![select interpreter]({{site.baseurl}}/assets/images/select-interpreters-command.png)
1. Choose the Python 3.7.4 virtual environment from the list.
1. Open a command terminal by clicking `Windows Key + J` and verify that `.venv` appears before the command prompt.

# 8. Cython

Install Cython using PIP and the following command. Cython is big, so be patient!

```
pip --timeout=120 install Cython==0.29.7
```

# 9. GIS Site Packages

[Shapely](https://shapely.readthedocs.io/en/stable/index.html), [GDAL](https://gdal.org/), [PyProj](https://github.com/pyproj4/pyproj) and [Rasterio](https://rasterio.readthedocs.io/en/latest/) all need to be downloaded as pre-compiled binary files (referred to as [wheels](https://pythonwheels.com)) and then installed using PIP. Visit the link below and download the appropriate wheel files being careful to pick the correct versions for your operating system and architecture (32 or 64 bit).

[https://www.lfd.uci.edu/~gohlke/pythonlibs/#shapely](https://www.lfd.uci.edu/~gohlke/pythonlibs/#shapely)

Now run each of the following commands to install each downloaded wheel file.

```
pip install <DOWNLOAD_FOLDER>/Shapely-1.6.4.post2-cp37-cp37m-win_amd64.whl
pip install <DOWNLOAD_FOLDER>/GDAL-2.4.1-cp37-cp37m-win_amd64.whl
pip install <DOWNLOAD_FOLDER>/pyproj-2.4.0-cp37-cp37m-win_amd64.whl 
pip install <DOWNLOAD_FOLDER>/rasterio-1.0.24+gdal24-cp37-cp37m-win_amd64.whl 
```

# 10. SciPy

Download the SciPy pre-compiled wheel file from the following link and then install it at the command line using PIP.

[https://pypi.org/project/scipy/#files](https://pypi.org/project/scipy/#files)

```
pip install ~/Downloads/scipy-1.3.1-cp37-cp37m-win_amd64.whl 
```

# 11. Get the BRAT Code

Get the [BRAT source code](https://github.com/Riverscapes/sqlBRAT) from GitHub one of two ways. Those proficient with the [git version control software](https://git-scm.com/docs/git-svn) can simply clone the repo to their computer. This method is recommended if you intend to modify or contribute to the code. If you're keen to learn git then a good place to start is either the [free desktop git client provided by GitHub](https://desktop.github.com/) or the excellent [GitKraken](https://www.gitkraken.com/) software.

Those unfamiliar with git can simply click the green button on the right of the page to download a copy of the code. It's recommended that you unzip the code into a folder that does not contain any spaces in the path (i.e. `C:\SourceCode\brat` and not `C:\Source Code\brat`).


# 12. Remaining Site Packages

The remaining Python site packages can be installed directly using PIP, leveraging the [requirements.txt](https://github.com/Riverscapes/sqlBRAT/blob/master/requirements.txt) file that comes with the BRAT source code. In the command terminal, change directory into the root of the code folder and then type the following:

```
pip --timeout=120 install -r requirements.txt
```

# Why Was This So Hard?

Python is meant to be platform indepedent after all, so why do you need to download binary files etc?

Some of the Python site packages needed to run BRAT have their own dependencies that are written in C. This C code has to be compiled using the specific architecture of the operating sysem on which it will run (Linux, Windows 32 bit, Windows 64 bit etc). Rather than compile the C code yourself, it's simpler to download the pre-compiled binary files that are called [wheels](https://pythonwheels.com/).

Those dependencies that are written in pure Python can simply be installed using PIP with the help of the [requirements.txt](https://github.com/Riverscapes/sqlBRAT/blob/master/requirements.txt) file that is provided with the BRAT code and enumerates all the relevant dependencies needed.