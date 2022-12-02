## Steps to use the dev environment

1. create a `.zshenv` file in your root and set your data dir (this is what you have in your .env files as `DATA_ROOT`):

```
RSTOOLS_DATA=/my/path/to/external/data
```

2. Do magic-p in vscode and choose: `Dev Containers: Rebuild and Reopen in Container`

3. Once you're in the container you'll need to set your existing `.env` files to point to the new mount: `DATA_ROOT=/data`