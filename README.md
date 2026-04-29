# MEA/NLX Spike Sorting GUI

Jupyter-based graphical workflow for **MEA `.h5` recordings** and **Neuralynx `.ncs` tetrode recordings** using SpikeInterface.

This repository provides an interactive GUI for channel-level QC, spike sorting, sorter comparison, quality metric generation, spike-time export, unit report generation, and two-pass unit curation.

---

## Overview

The GUI supports:

- MEA `.h5` recording input
- Neuralynx `.ncs` tetrode folder input
- Automatic tetrode grouping from files such as `TT1a.ncs`, `TT1b.ncs`, `TT1c.ncs`, and `TT1d.ncs`
- RMS-based channel QC
- Editable channel inclusion/exclusion
- Bandpass filtering, optional 60 Hz notch filtering, and optional common median reference
- Spike sorting with:
  - MountainSort4
  - Kilosort4
  - SpyKING Circus2
- Spike-time export
- SpikeInterface quality metric computation
- Unit report generation
- Pairwise sorter comparison
- Two-pass unit curation using Allen-style quality thresholds and nearest-neighbor hit-rate filtering

---

## Repository structure

```text
mea-nlx-spike-sorting-gui/
│
├── README.md
├── LICENSE
├── CITATION.cff
├── .gitignore
├── requirements-core.txt
├── requirements-optional.txt
│
├── src/
│   └── mea_nlx_sorter_core_v4.py
│   └── mea_nlx_sorter_core_v6.py
│
├── notebooks/
│   └── mea_nlx_clustering_ui_2026_04_27_v7.ipynb
│   └── mea_nlx_clustering_ui_v9.ipynb
│
├── docs/
│   ├── MEA_Neuralynx_Spike_Sorting_GUI_Researcher_Guide_UPDATED 4-29-26.pdf
│   └── troubleshoot.md

```

---

## Main files

| File | Purpose |
|---|---|
| `src/mea_nlx_sorter_core_v6.py` | Backend Python module containing loading, preprocessing, sorting, metrics, reporting, comparison, and curation functions. |
| `notebooks/mea_nlx_clustering_ui_v9.ipynb` | Jupyter GUI used by researchers. |
| `docs/MEA_Neuralynx_Spike_Sorting_GUI_Researcher_Guide.pdf` | Full researcher-facing guide. |
| `requirements-core.txt` | Required packages for the main GUI workflow. |
| `requirements-optional.txt` | Optional packages for Kilosort4, graph/network analysis, and broader downstream analysis. |

---

## Workflow

The active GUI is organized into three main tabs.

### Tab 1: Load & QC

Used to load the recording and inspect/select channels before sorting.

Functions include:

- Load MEA `.h5` files
- Load Neuralynx tetrode folders containing `.ncs` files
- Scan tetrode groups such as `TT1`, `TT2`, `TT3`, and `TT4`
- Apply bandpass filtering
- Optionally apply notch filtering
- Optionally apply common median reference
- Compute per-channel RMS QC
- Auto-flag noisy channels
- Manually include or exclude channels
- Plot flagged-channel traces
- Plot notch-filter effects
- Plot CMR effects

### Tab 2: Sorting & Metrics

Used to run the sorter and generate post-sorting outputs.

Supported sorters:

- MountainSort4
- Kilosort4
- SpyKING Circus2

Outputs include:

- SpikeInterface sorting output
- Waveform/analyzer folder
- `quality_metrics.xlsx`
- `units_spike_times.xlsx`
- `unit_reports/`
- Optional sorter-comparison outputs

### Tab 3: Unit Curation

Used to run two-pass automated unit curation.

The curation workflow applies:

1. Allen-style first-pass filters:
   - `amplitude_cutoff`
   - `presence_ratio`
   - ISI/contamination metric

2. Optional second-pass nearest-neighbor filter:
   - `nn_hit_rate`

Outputs include:

```text
curated_units_*.xlsx
```

with sheets for:

- Combined units
- Kept units only
- Flagged units only
- Curation summary
- Curation settings

---

## Input data

### MEA `.h5`

The MEA workflow expects an `.h5` recording file and, when available, a geometry CSV containing channel positions.

### Neuralynx `.ncs`

The Neuralynx workflow expects a folder containing continuous channel files named like:

```text
TT1a.ncs
TT1b.ncs
TT1c.ncs
TT1d.ncs
TT2a.ncs
TT2b.ncs
TT2c.ncs
TT2d.ncs
```

The GUI groups `TT1a-d` as `TT1`, `TT2a-d` as `TT2`, and so on.

`.ntt` files may be present in the folder, but the GUI sorts from the continuous `.ncs` files.

---

## Installation

A dedicated conda environment is recommended.

### Create environment

```bash
conda create -n mea_nlx_sorter python=3.10 -y
conda activate mea_nlx_sorter

python -m pip install --upgrade pip setuptools wheel
```

### Install core requirements

```bash
pip install -r requirements-core.txt
```

### Install optional requirements

```bash
pip install -r requirements-optional.txt
```

### Register the Jupyter kernel

```bash
python -m ipykernel install --user --name mea_nlx_sorter --display-name "Python (MEA/NLX Sorter)"
```

### Launch Jupyter

```bash
jupyter lab
```

or:

```bash
jupyter notebook
```

Then select the kernel:

```text
Python (MEA/NLX Sorter)
```

---

## macOS note

On macOS/zsh, packages with bracketed extras should be quoted.

For example:

```bash
pip install "spikeinterface[full,widgets]==0.103.0"
pip install "kilosort[gui]"
```

---

## Running the GUI

1. Open the notebook:

```text
notebooks/mea_nlx_clustering_ui_2026_04_27_v7.ipynb
```

2. Run the notebook cells.

3. In the GUI, load the core file:

```text
src/mea_nlx_sorter_core_v4.py
```

4. Select the input mode:

```text
MEA .h5
```

or:

```text
Neuralynx tetrode folder (.ncs)
```

5. Run Tab 1 QC.

6. Run sorting and output generation in Tab 2.

7. Run unit curation in Tab 3.

---

## Main outputs

Typical outputs include:

```text
si_sorting/
waveforms/
quality_metrics.xlsx
units_spike_times.xlsx
unit_reports/
curated_units_*.xlsx
sorter_comparison_* folders
```

These files are generated during analysis and should not be committed to GitHub.

---

## Sorter notes

### MountainSort4

MountainSort4 is generally practical for local testing and computational-core use. The GUI exposes parameters such as detection threshold, detection sign, adjacency radius, clip size, detection interval, and worker count.

### Kilosort4

Kilosort4 is installed as the Python package `kilosort`, but it appears in SpikeInterface as the sorter name `kilosort4`.

Kilosort4 performance depends strongly on the PyTorch/CUDA setup. It can be selected from the GUI, but on computers without CUDA GPU acceleration it may take longer, especially for larger recordings.

### SpyKING Circus2

SpyKING Circus2 is used through SpikeInterface as the internal sorter `spykingcircus2`.

---

## Data and output policy

Do not commit raw electrophysiology recordings or generated sorter outputs to this repository.

Do not commit:

```text
*.h5
*.ncs
*.ntt
*.nev
*.nse
*.nvt
si_sorting/
waveforms/
unit_reports/
quality_metrics.xlsx
units_spike_times.xlsx
curated_units_*.xlsx
sorter_comparison_*/
```

Raw electrophysiology data should remain on approved institutional storage or approved data-sharing platforms.

---

## Documentation

The full researcher guide is available in:

```text
docs/MEA_Neuralynx_Spike_Sorting_GUI_Researcher_Guide_UPDATED.pdf
```

Researchers should read the guide before using the GUI, especially the sections on:

- Running environment and hardware expectations
- Installation
- Tab 1 QC
- Sorter parameters
- Whitening and threshold interpretation
- Quality metrics
- Sorter comparison
- Unit curation

---

## Citation

If you use this software, please cite this repository using the information in `CITATION.cff`.

---

## License

This project is released under the BSD 3-Clause License. See `LICENSE`.

---

## Status

Current release:

```text
v1.0.0
```

Active code versions:

```text
Core: MEA-Nlx Sorter python core_v4.py
UI:   MEA-Nlx Clustering UI 04-27-26_v7.ipynb
Guide: MEA_Neuralynx_Spike_Sorting_GUI_Researcher_Guide
```
