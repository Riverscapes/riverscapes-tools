# riverscapes-common

Python 3.0 + Open source tools and common libraries

**NOTE:** This is for python3.0 and open-source GIS only parts of this library may be repurposed to work with python2 and Arcpy but there are no guarantees and no plan to support this.

## Getting up and going for devs

1. get your virtualenv set up

***NB: # On OSX you must have run `brew install gdal` so that the header files are findable***

```bash
./scripts/create_venv.py
```

## Notes about classes

Take a look at `rscommons/classes/__init__.py`. It's the only init file that has anything in it and we only use it to make classes more convenient to the user who is typing imports 

```python
# So instead of this
from rscommons.classes.Logger import Logger
from rscommons.classes.ProgressBar import ProgressBar
# We can type 
from rscommons import Logger, ProgressBar
```

We also use it to make classes we'd rather people use more convenient
