## Problem

During Windows installation, `mountainsort4` may fail to install with an error similar to:

```text
Failed to build wheels for isosplit5
```

or:

```text
Failed building wheel for isosplit5
mountainsort4 did not install
```

This is not an issue with the MEA/NLX sorter GUI code. It occurs because `mountainsort4` depends on `isosplit5`, which may need to compile native C/C++ code during installation. On Windows, that compilation requires Microsoft C++ build tools.

## Environment where this occurred

- OS: Windows
- Python environment: conda environment for `mea_nlx_sorter`
- Package failing: `mountainsort4`
- Dependency failing: `isosplit5`

## Solution

Install Microsoft C++ build tools through Visual Studio Installer.

The working setup was:

```text
Visual Studio Community Insider 2026
Desktop development with C++
MSVC v143 - VS 2022 C++ x64/x86 build tools
Windows 11 SDK
C++ CMake tools for Windows
```

After installing these components, restart the computer.

Then reopen Anaconda Prompt and run:

```bat
conda activate mea_nlx_sorter

python -m pip install --upgrade pip setuptools wheel
python -m pip install pybind11
python -m pip install --no-cache-dir mountainsort4
```

Verify the installation:

```bat
python -c "import mountainsort4; print('MountainSort4 OK')"
```

Then verify that SpikeInterface detects MountainSort4:

```bat
python -c "import spikeinterface.sorters as ss; print(ss.installed_sorters())"
```

Expected result: `mountainsort4` should appear in the installed sorter list.

## Notes

The key required components are:

```text
Desktop development with C++
MSVC v143 - VS 2022 C++ x64/x86 build tools
Windows 10 or Windows 11 SDK
C++ CMake tools for Windows
```

The full Visual Studio IDE is not strictly required if using the standalone Build Tools installer, but the same components can also be installed through Visual Studio Community if available.

## Status

Resolved after installing the C++ build tools and rerunning the MountainSort4 installation.
