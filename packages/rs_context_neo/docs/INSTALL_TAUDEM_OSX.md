# Installing TauDEM and MPI on macOS

This guide explains how to install [TauDEM](https://hydrology.usu.edu/taudem/taudem5/) and a compatible MPI runtime (`mpiexec`) on macOS so that the `rs_context_neo` D8 hydrology pipeline can run successfully.

---

## What is needed and why

`rs_context_neo` calls TauDEM tools (`pitremove`, `d8flowdir`, `aread8`, `threshold`, `streamnet`) via MPI:

```
mpiexec -n <cores> pitremove -z dem_breach.tif -fel dem_filled.tif
```

Two things must be on your `$PATH`:

| Binary | Purpose |
|--------|---------|
| `mpiexec` | MPI process launcher (parallel execution across CPU cores) |
| `pitremove`, `d8flowdir`, `aread8`, `threshold`, `streamnet` | TauDEM hydrological analysis tools |

---

## Build TauDEM from source with Homebrew MPI

> **Note:** The `osgeo/osgeo4mac` Homebrew tap is currently broken on recent Homebrew versions (`undefined method 'cellar' for an instance of BottleSpecification`) and should not be used. Build from source instead.

### 1. Install dependencies

```bash
brew install open-mpi cmake gdal
```

### 2. Clone and build TauDEM

```bash
git clone https://github.com/dtarb/TauDEM.git
cd TauDEM/src

# Create a build directory (out-of-source build is recommended)
mkdir build && cd build

cmake .. \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_INSTALL_PREFIX=/usr/local

make -j$(sysctl -n hw.logicalcpu)
sudo make install
```

> **Apple Silicon note:** If `cmake` cannot find Open MPI headers, pass the Homebrew prefix explicitly:
>
> ```bash
> cmake .. \
>   -DCMAKE_BUILD_TYPE=Release \
>   -DCMAKE_INSTALL_PREFIX=/usr/local \
>   -DMPI_C_COMPILER=/opt/homebrew/bin/mpicc \
>   -DMPI_CXX_COMPILER=/opt/homebrew/bin/mpicxx
> ```

### 3. Verify

```bash
which pitremove
mpiexec -n 2 pitremove --help
```

---

## macOS-specific MPI workaround (important)

Open MPI's default network provider (`ofi`) tries to communicate over the primary network interface (`en0`).  On macOS this often fails with:

```
OFI poll failed (default nic=en0: Input/output error)
```

`rs_context_neo` automatically injects the correct environment variables before running any TauDEM subprocess (see `taudem.py → apply_mpi_env`), so **no manual action is required** during normal pipeline use.

However, if you run TauDEM commands manually from the shell, set these variables first:

```bash
# For Open MPI
export OMPI_MCA_btl=tcp,self

# For MPICH / libfabric
export FI_PROVIDER=tcp
```

You can add both lines to your shell profile (`~/.zshrc` or `~/.bash_profile`) so they apply to every terminal session:

```bash
echo 'export OMPI_MCA_btl=tcp,self' >> ~/.zshrc
echo 'export FI_PROVIDER=tcp'        >> ~/.zshrc
source ~/.zshrc
```

---

## Controlling the number of MPI cores

By default `rs_context_neo` uses **2 MPI ranks**.  You can change this without modifying code by setting an environment variable:

```bash
export TAUDEM_CORES=8    # use 8 parallel MPI ranks
```

A sensible value is the number of physical CPU cores on your machine:

```bash
# Print the physical core count on macOS
sysctl -n hw.physicalcpu
```

Set it persistently in your shell profile:

```bash
echo "export TAUDEM_CORES=$(sysctl -n hw.physicalcpu)" >> ~/.zshrc
source ~/.zshrc
```

You can also pass extra `mpiexec` flags via a second environment variable (space-separated):

```bash
# Allow MPI to oversubscribe (useful in containers or when cores > physical CPUs)
export TAUDEM_MPI_ARGS="--oversubscribe"
```

---

## Quick smoke test

Once everything is installed, run the following to confirm that MPI + TauDEM are working correctly together:

```bash
# Should print TauDEM usage without any MPI errors
OMPI_MCA_btl=tcp,self mpiexec -n 2 pitremove
```

Expected output is a usage/help message from TauDEM.  Any line containing `OFI poll failed` or `mpiexec: command not found` indicates a problem; refer to the troubleshooting section below.

---

## Troubleshooting

### `mpiexec: command not found`

MPI is not installed or is not on `$PATH`.

```bash
brew install open-mpi

# Apple Silicon — Homebrew may not be on PATH by default; add it:
echo 'eval "$(/opt/homebrew/bin/brew shellenv)"' >> ~/.zshrc
source ~/.zshrc
```

### `pitremove: command not found`

TauDEM binaries are not on `$PATH`.

Ensure the install prefix is on your `$PATH`:

```bash
export PATH="/usr/local/bin:$PATH"
```

### `osgeo/osgeo4mac/osgeo-taudem: undefined method 'cellar'`

The `osgeo4mac` Homebrew tap is broken on current Homebrew versions.  Build TauDEM from source instead (see above).

### `OFI poll failed` or `btl_tcp` warnings

Route MPI traffic over the loopback interface (see the [macOS-specific MPI workaround](#macos-specific-mpi-workaround-important) section above).

### GDAL not found during TauDEM build

```bash
brew install gdal
# Then re-run cmake, pointing it at the Homebrew prefix:
cmake .. -DGDAL_LIBRARY=$(brew --prefix gdal)/lib/libgdal.dylib \
         -DGDAL_INCLUDE_DIR=$(brew --prefix gdal)/include
```

### MPI processes crash or hang with large rasters

Reduce the core count so each rank has enough memory:

```bash
export TAUDEM_CORES=2
```

Or, if physical memory is not the issue, allow MPI to oversubscribe:

```bash
export TAUDEM_MPI_ARGS="--oversubscribe"
```

---

## Summary of environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `TAUDEM_CORES` | `2` | Number of MPI ranks for all TauDEM steps |
| `TAUDEM_MPI_ARGS` | *(empty)* | Extra flags inserted between `mpiexec` and the TauDEM command |
| `OMPI_MCA_btl` | *(unset)* | Set to `tcp,self` to avoid Open MPI OFI errors on macOS |
| `FI_PROVIDER` | *(unset)* | Set to `tcp` to avoid MPICH libfabric errors on macOS |

`rs_context_neo` sets `OMPI_MCA_btl` and `FI_PROVIDER` automatically for subprocess calls on macOS.  If you run TauDEM manually from the shell, set them yourself as shown above.
