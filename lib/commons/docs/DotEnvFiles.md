# `.env` files

There are sometimes things you don't want to commit to git:

* Passwords
* Drive paths like `C:\mypath...`
* Database connection strings

1. Create a `.env` file
2. fill the file with the key=value pairs you need 

```
MY_USERNAME=user1
PASSWORD=password123
OUTPUT_PATH=C:\MyOutput\Path
```

3. Make sure the `.gitignore` file has `.env` listed so that this doesn't ever end up in git.
4. Pass the environment to your script. There are several ways to do this but in VSCode the method we like is to use is written into our `ModelConfig` library

## VSCode and .env files:

<https://code.visualstudio.com/docs/python/environments>

A lesser known feature of the Python module in VSCode is that there is a Workspace setting called `python.envFile` which defaults to be a `.env` file in your workspace root folder. This means you don't have to use the launch.json or any other method to set envfiles. 

```json
{
    "python.envFile": "${workspaceFolder}/.env",
}
```


### `.env` file

```
FLOW_DEM=0.234234
FLOW_OUTPUT=20.3
FLOW_DRAINAGE=6000
```


### `launch.json`

```json
    {
      "name": "flow_accumulation",
      "type": "python",
      "request": "launch",
      "console": "integratedTerminal",
      "module": "lib.whatevermodule",
      "console": "internalConsole",
      // Add FLOW_DEM, FLOW_OUTPUT, FLOW_DRAINAGE to your .env file
      "args": [
        "{env:FLOW_DEM}",
        "{env:FLOW_ACCUM}",
        "{env:FLOW_DRAINAGE}",
        "--pitfill",
        "--verbose"
      ]
    }
```

Now, when you use argparse the `dotenv.parse_args_env` command will make the appropriate substitutions. See `/examples/gistool.py`


```python
parser = argparse.ArgumentParser()
parser.add_argument('arg1', help='Some Argument 1', type=str)
parser.add_argument('arg2', help='Some Argument 2', type=int)
parser.add_argument('--verbose', help='(optional) a little extra logging ', action='store_true', default=False)
# dotenv.parse_args_env will parse and replace any environment variables with the pattern {env:MYENVNAME}
args = dotenv.parse_args_env(parser)
```