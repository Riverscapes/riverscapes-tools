## VSCode can't find my environemt

Because our workspaces are running in subfolders (usually `packages/TOOL_NAME`) sometimes VSCode doesn't include our `.venv` folder which is sitting in the root of the repo.

One way to make this work is just to symlink the `.venv` folder into the `packages/TOOL_NAME` folder. This way VSCode will be able to find the environment.

```bash
cd packages/TOOL_NAME
ln -s ../../.venv .venv
```

## Instructions for using `pyenv` on a MAc

### 1. Install `pyenv` using Homebrew
```bash
brew install pyenv
pyenv init
# Follow instructions to copy pyenv init to your shell's .zshrc or .bashrc
```

### 2. Install the version of python you want

```bash
pyenv install 3.8.10
```

### 3. Create a venv with the version of python you want

```bash
pyenv shell 3.8.10
python3 --version
```

You should now see `Python 3.8.10` as the version of python you are using. Now when you create your venv from running `bootstrap.sh` you should be able to get the correct version of python.