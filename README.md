# OCO lite matchup

A program designed to find crossings between OCO-2 and -3 tracks or OCO-3 with itself.


## Copyright notice

Copyright (c) 2022, by the California Institute of Technology. ALL RIGHTS RESERVED. United States Government Sponsorship acknowledged. Any commercial use must be negotiated with the Office of Technology Transfer at the California Institute of Technology.
 
This software may be subject to U.S. export control laws. By accepting this software, the user agrees to comply with all applicable U.S. export laws and regulations. User has the responsibility to obtain export licenses, or other export authority as may be required before exporting such information to foreign countries or providing access to foreign persons.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
* Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
* Neither the name of Caltech nor its operating division, the Jet Propulsion Laboratory, nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

## Installation

### Rust

This program requires the Rust toolchain to build.
While there is not a formal minimum version of Rust, this has only been tested with v1.75, so 1.75 or later is recommended.
Check if you have a Rust toolchain installed by running the command `rustup show`.
If that produces output similar to:

```
Default host: x86_64-unknown-linux-gnu
rustup home:  /home/user/.rustup

installed toolchains
--------------------

stable-x86_64-unknown-linux-gnu (default)

active toolchain
----------------

stable-x86_64-unknown-linux-gnu (default)
rustc 1.76.0 (07dca489a 2024-02-04)
```

then you have a toolchain installed.
In this case, it is version 1.76.0, as seen in the "active toolchain" section.
If `rustup show` does not produce output, or returns `rustup: No such file or directory`, either:

- check if your computing system provides Rust as a module which needs loaded (this may be the case on HPCs for example), or
- install Rust to your user from https://rustup.rs/

### HDF5 and netCDF4 libraries

This program needs to read and write netCDF4 files, thus it depends on the netCDF4 library, which in turn relies on the HDF5 library.
There are two ways which these libraries can provided:

1. They can be built from their source code during the compilation of this program (requires `cmake` be available).
2. Existing libraries can be linked to the Rust program during compilation.

Building from source is simple, so long as you have a reasonably recent version of `cmake` on your path.
Anyone trying to build on Mac should note that some users with MacOS Sonoma have had issues with a required
tool (`m4`) is missing from the command line developer tools which causes the compilation from source to fail.
(During compilation of the `netcdf-src` dependency, the build will crash and MacOS will launch a popup saying
that `m4` is missing and requires command line developer tools to be installed.
Following the prompt to install does not actually install `m4`.)

To prepare for installation, do one of the following:

- Ensure that you have `cmake` v3.x installed and on your `PATH` (i.e. `cmake --version` returns a string like `cmake version 3.29.2`).
  If you do not, see "Installing cmake" in the "Tips" section below.
- Mac users affected by the `m4` bug or users who want faster compilation need to find your HDF5 and netCDF4 libraries.
  Specifically, you are looking for the paths that contain `libhdf5.*` and `libnetcdf*`. See "Finding HDF and netCDF libraries"
  in the "Tips" section below.

### Compiling oco-lite-matchup

To compile with HDF5 and netCDF4 from source, in this repo, run:

```bash
cargo build --release --features=static
```

To compile with existing HDF5 and netCDF4 libraries, take the paths from the "HDF5 and netCDF4 libraries" section and assign them
to environmental variables.
Assuming you found `libhdf5.*` and `libnetcdf.*` at:

- `/usr/lib/libhdf5.*`
- `/usr/lib/libnetcdf.*`

then assign the following environmental variables:

```bash
HDF5_DIR=/usr
NETCDF_DIR=/usr
RUSTFLAGS="-C link-args=-Wl,-rpath,${HDF5_DIR}/lib"
```

and, in this repo, run `cargo build --release`. Two notes:

1. Note that the paths given to `HDF5_DIR` and `NETCDF_DIR` are the paths to our `lib*` files without the "lib" subdirectory.
   On some systems, that may be "lib64" instead (e.g. `/usr/lib64/libnetcdf.*`); the `lib64` subdirectory should also be
   removed in the paths for the variables.
2. We've not tested cases where the `HDF5_DIR` and `NETCDF_DIR` are different paths. In such a case, the `RUSTFLAGS` value
   may need multiple `-rpath` values, e.g. `-rpath,${HDF5_DIR}/lib,-rpath,${NETCDF_DIR}/lib`.

Either method will produce two binaries: `target/release/oco-lite-matchup` and `target/release/make-oco-match-config`.
These can be left where they are or moved/copied somewhere more convenient.

**Special notes for mac users:** 

- You may need to set the environment variable `DYLD_FALLBACK_LIBRARY_PATH` to `${HDF5_DIR}/lib`
  as well, and this must be set *any time you call the programs from this repo*, not just during compilation.
  If you get errors about missing files with the `.dylib` extension when running the program, try setting this
  variable.
- The version of HDF5 (1.14.*) installed by Homebrew in at least some cases is not recognized by the current
  stable version of the `hdf5-sys` crate used by this repo. Once `hdf5-sys` is updated, this restriction should
  be eased.

## Usage

The program `oco-lite-matchup` identifies crossings between OCO-2 and OCO-3 or OCO-3 and itself.
It takes lite files from both instruments as input.

### Matching one file

The simplest use is to identify crossings between one OCO-2 and one or more OCO-3 files (or one OCO-3 file and several other OCO-3 files).
This uses the `one` subcommand.
For example, let's assume that you wanted to find crossings between the OCO-2 file `oco2_LtCO2_200101_B11014Ar_220902231034s.nc4` and the
OCO-3 file for the same day, `oco3_LtCO2_200101_B10400Br_220317235255s.nc4`.
We'll save the output in `oco2_oco3_matched_1Jan2020.nc4`.
The command to do this is:

```
oco-lite-matchup one oco2_oco3_matched_1Jan2020.nc4 \
    oco2_LtCO2_200101_B11014Ar_220902231034s.nc4 \
    oco3_LtCO2_200101_B10400Br_220317235255s.nc4
```

This will take at least a few minutes to run, but will output a file with all cases where OCO-2 crosses within 100 km
of OCO-3 within 12 hours.

However, we can imagine that data from the beginning of the day in the OCO-2 file might match with OCO-3 data on the
previous day, and likewise data at the end of the OCO-2 file might match with OCO-3 data from the next day.
This subcommand allows us to specify multiple OCO-3 files to match against, so we should include the previous and next
days' files:


```
oco-lite-matchup one oco2_oco3_matched_1Jan2020.nc4 \
    oco2_LtCO2_200101_B11014Ar_220902231034s.nc4 \
    oco3_LtCO2_191231_B10400Br_220317235246s.nc4 \
    oco3_LtCO2_200101_B10400Br_220317235255s.nc4 \
    oco3_LtCO2_200102_B10400Br_220317235308s.nc4
```

Note that the 3rd and 5th lines point to the OCO-3 files from the previous day (31 Dec 2019) and the next day (2 Jan 2020).

If instead we wanted to look for OCO-3 self crossings (and output to `oco3_self_matched_1Jan2020.nc4`), the command instead would be:


```
oco-lite-matchup one --oco3-self-cross \
    oco3_self_matched_1Jan2020.nc4 \
    oco3_LtCO2_200101_B10400Br_220317235255s.nc4 \
    oco3_LtCO2_191231_B10400Br_220317235246s.nc4 \
    oco3_LtCO2_200101_B10400Br_220317235255s.nc4 \
    oco3_LtCO2_200102_B10400Br_220317235308s.nc4
```

Note that we include the 1 Jan 2020 file as both the 2nd and 4th positional argument.
It must be specified as both as the "base" file for matches (2nd argument) and the "other" file to match against (4th argument here);
if it were only given as the second argument, matches within that file will **not** be identified.

There are some other command line options:

- `--flag0-only`: only calculates matches based on soundings for which `xco2_quality_flag == 0` (good quality data).
- `--save-full-matches-as`: outputs an intermediate file which maps exactly which sounding pairs meet the crossing criteria, rather than
  just groups. These files can easily be hundreds of megabytes, so only save them if you really need them. Really these are intended for
  devloper debugging to be passed in via the `--read-full-matches` flag rather than for users to derive information from.

### Matching multiple files

If you want to generate matches for a range of dates, use the `multi` subcommand instead.
This only takes one argument, which is a [TOML](https://toml.io/) configuration file.
An example of this file is:

```toml
[[matchups]]
output_file = "oco_lite_matches_20200101.nc4"
oco2_lite_file = "/oco2/2020/01/01/LtCO2/oco2_LtCO2_200101_B11014Ar_220902231034s.nc4"
oco3_lite_files = [
    "/data/2019/12/31/LtCO2/oco3_LtCO2_191231_B10400Br_220317235246s.nc4",
    "/data/2020/01/01/LtCO2/oco3_LtCO2_200101_B10400Br_220317235255s.nc4",
    "/data/2020/01/02/LtCO2/oco3_LtCO2_200102_B10400Br_220317235308s.nc4",
]
flag0_only = false
oco3_self_cross = false

[[matchups]]
output_file = "oco_lite_matches_20200102.nc4"
oco2_lite_file = "/oco2/2020/01/02/LtCO2/oco2_LtCO2_200102_B11014Ar_220902231109s.nc4"
oco3_lite_files = [
    "/data/2020/01/01/LtCO2/oco3_LtCO2_200101_B10400Br_220317235255s.nc4",
    "/data/2020/01/02/LtCO2/oco3_LtCO2_200102_B10400Br_220317235308s.nc4",
    "/data/2020/01/03/LtCO2/oco3_LtCO2_200103_B10400Br_220317235309s.nc4",
]
flag0_only = false
oco3_self_cross = false
```

Each set of files to find crossings among is one `[[matchup]]` section.
It should be fairly straightforward to understand how the options in each section map to the command line arguments
of `oco-lite-matchup one`.
One note, if you want to use OCO-3 self crossings, set `oco3_self_cross` to `true` and put the "base" OCO-3 file
as the value for `oco2_lite_file`.
Do *not* change the key; OCO-3 self crossings still use `oco2_lite_file` as the key in these files.

You can generate these TOML configuration files yourself or use the second program included in this repo, `make-oco-match-config`.
See the command line help of that program for how to use it.
Note that it currently requires that OCO-2 and -3 lite files be organized in a directory structure that has one file per
directory, e.g. `/data/<year>/<month>/<day>/<FILE>`.
If you have multiple lite files in the same directory, for now you will need to construct the TOML files yourself.

## Tips

### Installing cmake

If working on your personal computer, `cmake` should be available through your package manager (e.g. `apt` on Debian/Ubuntu,
`homebrew` or `macports` on Mac, etc.).
A quick search for "PACKAGE_MANAGER install cmake", e.g. "apt install cmake" should point you to the right commands to use.

If you are working on a shared system (e.g. an HPC or other cluster) where you cannot use the system package manager, then
first see if that system uses modules or another way of organizing available tools and libraries.
If so, there may be a module you can load to provide `cmake`.
Otherwise, you can use [conda](https://docs.conda.io/en/latest/), [mamba](https://mamba.readthedocs.io/en/latest/), or
[micromamba](https://mamba.readthedocs.io/en/latest/user_guide/micromamba.html#micromamba) to install it.
This is especially handy if you already have one of these tools installed, e.g. to manage Python environments.

If you use conda/mamba/micromamba to install `cmake`, we recommend doing so in an isolated environment to avoid breaking existing tools.
For this example, we will use `conda`, but you should be able to substitute `mamba` or `micromamba` for `conda` in any commands that follow.
First, create a new conda environment with `cmake` as a dependency:

```bash
conda create --name cmake-for-rust cmake
```

Next, activate this environment with:

```
conda activate cmake-for-rust
```

This will modify your PATH so that `cmake` can be found.
Running `cmake --version` should now display a version number.
To use this version of cmake when compiling `oco-lite-matchup`, do one of the following:

1. activate this environment before running any `cargo build` commands (note that it must be activated for each new shell),
2. set the environmental variable `CMAKE` to the executable in this environment, or
3. link the `cmake` executable to a directory on your PATH.

For options 2 and 3, with the `cmake-for-rust` environment active, run `which cmake` and note the path it returns,
e.g. `/home/user/anaconda3/envs/cmake-for-rust/bin/cmake`.
For option 2, set the environmental variable `CMAKE` to this path, e.g `export CMAKE=/home/user/anaconda3/envs/cmake-for-rust/bin/cmake`
for `bash` or `zsh` or `setenv CMAKE /home/user/anaconda3/envs/cmake-for-rust/bin/cmake` for C-shell type shells.
For option 3, look at the directories listed by `echo $PATH` and find one you own.
If you need to add a new directory to `PATH`, e.g. `/home/data/bin`, you can modify your `PATH` by resetting it
as an environmental variable in your shell's login file (e.g. `~/.bashrc`, `~/.bash_profile`, `~/.zshrc`, etc.) with:

```bash
# this is bash/zsh syntax
export PATH=${PATH}:/home/user/bin
```

replacing `/home/user/bin` with your desired path.

### Finding HDF and netCDF libraries

If you are working on an HPC or other shared system, as in "Installing cmake", check if there are modules (or other
software management approaches) that need loaded to make the system HDF5 and netCDF4 libraries available.
In any situation, start by running `nc-config --prefix`.
If this returns a path, then that is a good guess as the value for `NETCDF_DIR` above.
If `nc-config` is not on your PATH, then you will either need to dig around on your computer or install the libraries yourself.
For `HDF5_DIR`, there often isn't a comparable command you can run, but the HDF5 libraries will usually be in the same
path as the netCDF ones, or a nearby path.

Should you need to install these libraries yourself, you can do so with `conda` (or `mamba` or `micromamba`).
Create or activate the environment you wish to install into, then run:

```
conda install --override-channels --channel conda-forge hdf5=1.12.2 libnetcdf=4.9.1
```

(You can replace `conda` with `mamba` or `micromamba` here.)
While this environment is active, check the value of `$CONDA_PREFIX`.
That path will be the value to provide for `HDF5_DIR` and `NETCDF_DIR` in the compilation instructions above.
