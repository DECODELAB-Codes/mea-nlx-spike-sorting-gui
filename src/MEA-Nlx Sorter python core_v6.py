"""
MEA / Neuralynx Spike Sorting GUI Core
--------------------------------------
Purpose
=======

Backend core functions for the MEA / Neuralynx spike-sorting Jupyter GUI.
This module supports:
  1) Loading MCS / Harvard Apparatus MEA recordings exported as HDF5 (.h5).
  
  2) Loading Neuralynx continuous-channel tetrode recordings from .ncs folders.
     Neuralynx tetrodes are detected from files such as TT1a.ncs, TT1b.ncs,
     TT1c.ncs, and TT1d.ncs, then loaded and sorted as one tetrode bundle.
     
  3) Attaching recording geometry:
       - MEA recordings use a provided geometry CSV.
       - Neuralynx tetrode recordings use an auto-generated compact 4-contact
         tetrode geometry, with a default 25 um square layout.
         
  4) Preprocessing recordings with bandpass filtering, optional notch filtering,
     and optional common median reference.
     
  5) Computing channel-level RMS QC from the quietest baseline window and
     supporting channel inclusion/exclusion before sorting.
     
  6) Running spike sorting through SpikeInterface with:
       - MountainSort4
       - Kilosort4
       - SpyKING Circus2

  7) Handling cross-platform sorter temporary directories so the same workflow
     can run on Linux/HPC, Windows, and macOS without relying on Unix-only /tmp
     behavior.

  8) Applying sorter-specific compatibility handling for SpikeInterface 0.103,
     including validated SpyKING Circus2 parameter sanitization.

  9) Extracting waveforms, templates, spike amplitudes, noise levels, principal
     components, and quality metrics from sorted units.

10) Exporting unit spike times.

11) Building per-unit and global unit reports, including waveform summaries,
     ISI histograms, autocorrelograms, PCA scatter plots, amplitude summaries,
     and unit-statistics workbooks.

12) Running two-pass unit curation from quality_metrics.xlsx using configurable
     thresholds for amplitude cutoff, presence ratio, contamination/ISI metrics,
     and nearest-neighbor hit rate.
 
Active workflow
===============

This core is intended to be called by the Jupyter UI. The active researcher-facing
workflow is:

    Load recording
    -> Compute channel QC
    -> Apply included-channel selection
    -> Run sorter
    -> Extract waveforms/PCA
    -> Compute quality metrics
    -> Export spike times
    -> Build unit reports
    -> Run unit curation
 
Notes
=====

- This module no longer provides the active connectivity/null-model workflow.
  Functional connectivity and null testing should be maintained in a separate
  analysis repository or script.

- The legacy Tkinter class remains only for backward compatibility unless
  explicitly removed; the current supported interface is the Jupyter GUI.

- Recommended SpikeInterface version for this codebase: 0.103.0.

- Required core packages include:
    spikeinterface[full,widgets], mountainsort4, kilosort, probeinterface,
    neo, quantities, numpy, scipy, pandas, matplotlib, scikit-learn,
    openpyxl, xlsxwriter, h5py, ipywidgets, ipyfilechooser, and ipydatagrid.
"""
 

from __future__ import annotations

# Safer multiprocessing in notebooks/headless
import multiprocessing as _mp
try:
    _mp.set_start_method("spawn", force=True)
except RuntimeError:
    pass

import os
import sys
import json
import time
import traceback
import threading
import re
import shutil
from typing import Dict, List, Tuple, Optional
from datetime import datetime
from dataclasses import dataclass, field

import numpy as np

# ---- NumPy 2.x compatibility for legacy deps (spikeextractors etc.) ----
if not hasattr(np, "Inf"):
    np.Inf = np.inf
if not hasattr(np, "Infinity"):
    np.Infinity = np.inf
# cover both casings occasionally used in old code
if not hasattr(np, "NaN"):
    np.NaN = np.nan
if not hasattr(np, "Nan"):
    np.Nan = np.nan
# ------------------------------------------------------------------------

import pandas as pd

import tkinter as tk
from tkinter import ttk, filedialog, messagebox

import matplotlib
matplotlib.use("Agg")  # we render plots to files; GUI shows thumbnails/opens folder
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D  # (for 3D PCA)

# Excel writer
import xlsxwriter  

# SpikeInterface imports
try:
    import spikeinterface as si
    import spikeinterface.core as sicore
    import spikeinterface.extractors as se
    import spikeinterface.preprocessing as spre
    import spikeinterface.sorters as ss

    from spikeinterface.qualitymetrics import compute_quality_metrics

except Exception as e:
    si = None
    se = None
    ss = None
    spre = None
    compute_quality_metrics = None


import matplotlib.backends.backend_tkagg


def set_tempdir_for_session(path):
    """
    Set a valid temporary directory for sorters and compiled dependencies.
 
    This is cross-platform:
    - Windows: avoids invalid Unix-style /tmp paths.
    - Linux/HPC: uses the selected output folder rather than assuming /tmp.
    - macOS: also works through TMPDIR/TEMP/TMP.
 
    Several sorter dependencies use tempfile or environment variables
    such as TEMPDIR/TMP/TEMP/TMPDIR. Setting all of them makes the behavior
    consistent across platforms.
    """
    import os
    import tempfile
    from pathlib import Path
 
    tmp_path = Path(path).expanduser().resolve()
    tmp_path.mkdir(parents=True, exist_ok=True)
 
    # TEMPDIR is used by MountainSort4. TMP/TEMP/TMPDIR are used by Python/tempfile
    # and other libraries depending on OS.
    for key in ("TEMPDIR", "TMPDIR", "TMP", "TEMP"):
        os.environ[key] = str(tmp_path)
 
    tempfile.tempdir = str(tmp_path)
    return str(tmp_path)
 
 
def _prepare_sorter_tempdir(output_folder: str, sorter_name: str = "sorter") -> str:
    """
    Create and register a sorter-specific temporary directory outside the sorter
    output folder. 
    
    Important:
    Spikeinterface is called with remove_existing_folder=True. Therefore, the 
    temporary directory must not be inside output_folder, because Spikeinterface
    may delete output_folder before the sorter starts. Mountainsort4 then tries
    to create a temporary subfolder under TEMPDIR, and fails if TEMPDIR was deleted.
    
    This function creates a sibling temp folder under:
        <parent_of_sorter_output>/_sorter_tmp/<sorter_output_folder_name>/
    
    This works on windows/ Linux/HPC. and macOS.
    """
    from pathlib import Path
    import re
    import os
 
    output_dir = Path(output_folder).expanduser().resolve()

    # Do not place temp inside output_dir because ss.run_sorter(...,
    #remove_existing_folder=True) may delete output_dir.
    parent_dir = output_dir.parent
    parent_dir.mkdir(parents=True, exist_ok=True)
    
    safe_sorter = re.sub(r"[^A-Za-z0-9._-]+", "_", str(sorter_name)).strip("_") or "sorter"
    safe_run = re.sub(r"[^A-Za-z0-9._-]+", "_", output_dir.name).strip("_") or "run"
    
    tmp_dir = parent_dir / "_sorter_tmp" / f"{safe_sorter}_{safe_run}"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    
    tmp_str = set_tempdir_for_session(tmp_dir)
    
    #Sanity check: fall early if the temp directory is not usable.
    if not os.path.isdir(tmp_str):
        raise RuntimeError(f"{sorter_name} temp directory does not exist after creation: {tmp_str}")
    if not os.access(tmp_str, os.W_OK):
        raise RuntimeError(f"{sorter_name} temp directory is not writable: {tmp_str}")
    
    print(f"[info] {sorter_name} temp dir: {tmp_str}")
    return tmp_str
 

class NoisyChannelInspector(tk.Toplevel):
    def __init__(self, parent, traces_dict, fs, seconds=60):
        super().__init__(parent)
        self.title("Noisy Channel Visual Inspection")
        self.selected = {}

        # --- Scrollable frame setup ---
        container = ttk.Frame(self)
        container.pack(fill='both', expand=True)
        canvas = tk.Canvas(container)
        scrollbar = ttk.Scrollbar(container, orient="vertical", command=canvas.yview)
        canvas.configure(yscrollcommand=scrollbar.set)
        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        scrollable_frame = ttk.Frame(canvas)
        scrollable_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )
        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")

        # --- Add plots inside scrollable_frame ---
        self.check_vars = {}
        n = len(traces_dict)
        cols = 2
        rows = (n + 1) // 2

        for i, (ch, trace) in enumerate(traces_dict.items()):
            f = ttk.Frame(scrollable_frame)
            f.grid(row=i // cols, column=i % cols, padx=6, pady=6, sticky='nsew')
            fig, ax = plt.subplots(figsize=(5, 2))
            n_samples = min(len(trace), int(fs * seconds))
            t = np.arange(n_samples) / fs
            ax.plot(t, trace[:n_samples])
            ax.set_title(f"Ch {ch}")
            ax.set_xlabel("s")
            ax.set_ylabel("uV")
            fig.tight_layout()
            canvas_fig = matplotlib.backends.backend_tkagg.FigureCanvasTkAgg(fig, master=f)
            canvas_fig.draw()
            canvas_fig.get_tk_widget().pack()
            v = tk.BooleanVar(value=False)
            self.check_vars[ch] = v
            ttk.Checkbutton(f, text="Keep channel", variable=v).pack()
            plt.close(fig)

        # --- OK button at the bottom ---
        btn_frame = ttk.Frame(self)
        btn_frame.pack(fill='x', pady=8)
        ttk.Button(btn_frame, text="OK", command=self._on_ok).pack()

    def _on_ok(self):
        self.selected = {ch: v.get() for ch, v in self.check_vars.items()}
        self.destroy()

# ----------------------------
# Utility & data structures
# ----------------------------

@dataclass
class MEASettings:
    # IO
    input_h5: str = ""
    input_mode: str = "mcs_h5"
    input_path: str = ""
    input_folder: str = ""
    selected_tetrode: str = ""
    outdir: str = field(default_factory=os.getcwd)

    # Neuralynx tetrode geometry
    tetrode_spacing_um: float = 25.0
    tetrode_layout: str = "square"

    # QC
    baseline_t0_s: float = 0.0
    baseline_len_s: float = 15.0
    hp_min_hz: float = 300.0
    hp_max_hz: float = 6000.0
    use_cmr: bool = False

    # Sorting
    sorter_name: str = "mountainsort4"
    detect_threshold: float = 3.0  # ~3x RMS/noise
    detect_sign: int = 0           # -1 neg, 0 both, +1 pos
    adjacency_radius: float = -1   # -1 = auto/global
    clip_size: int = 50            # samples per clip (approx)
    chunk_duration_s: float = 60.0
    #n_jobs: int = max(1, os.cpu_count() // 2)
    n_jobs: int = 1

    # Unit Reports
    ms_before: float = 1.5  # ms
    ms_after: float = 2.5   # ms
    max_spikes_per_unit: int = 1000

    # Unit Report ACG (grid)
    include_acg_in_reports: bool = True   # toggle ACG generation
    acg_grid_cols: int = 10               # grid width (default 10x10 = 100/page)
    acg_grid_rows: int = 10               # grid height
    acg_max_lag_s: float = 0.1            # ±100 ms
    acg_bin_ms: float = 2.0               # 2 ms bins (~100 bins across 200 ms)
    acg_use_pdf: bool = True              # save ACG grids to a multi-page PDF
    acg_png_dpi: int = 300                # used only if not PDF

    # NEW: whether to compute PC-based cluster-separation metrics
    compute_pc_metrics: bool = False
    
    # Metadata columns
    animal: str = ""
    unique_id: str = ""
    group: str = ""
    cohort: str = ""
    
    # --- NEW: Notch filter toggle ---
    use_notch_60hz: bool = True
# ----------------------------
# Core functions
# ----------------------------

def ensure_spikeinterface_available():
    if si is None or se is None or ss is None or spre is None:
        raise RuntimeError(
            "SpikeInterface not found. Please install: \n"
            "  pip install 'spikeinterface[full,widgets]' mountainsort4\n"
            "Also install: numpy scipy pandas matplotlib xlsxwriter openpyxl joblib tqdm h5py\n"
        )


def load_mcs_h5(path: str):
    """Load MCS/Harvard Apparatus HDF5 via SpikeInterface extractor.

    Returns a RecordingExtractor.
    """
    ensure_spikeinterface_available()
    if not os.path.isfile(path):
        raise FileNotFoundError(path)
    try:
        rec = se.read_mcsh5(path)
    except Exception as e:
        raise RuntimeError(f"Failed to read MCS H5. Error: {e}")
    return rec


def load_neuralynx_folder(folder_path: str, stream_id: Optional[str] = None, stream_name: Optional[str] = None,
                          exclude_filename: Optional[List[str]] = None):
    """Load a Neuralynx folder containing continuous .ncs files via SpikeInterface."""
    ensure_spikeinterface_available()
    folder_path = os.path.abspath(folder_path)
    if not os.path.isdir(folder_path):
        raise NotADirectoryError(folder_path)
    kwargs = {}
    if stream_id is not None:
        kwargs["stream_id"] = stream_id
    if stream_name is not None:
        kwargs["stream_name"] = stream_name
    if exclude_filename is not None:
        kwargs["exclude_filename"] = exclude_filename
    try:
        rec = se.read_neuralynx(folder_path, **kwargs)
    except Exception as e:
        raise RuntimeError(f"Failed to read Neuralynx folder. Error: {e}")
    return rec


def _natural_sort_key(text):
    return [int(tok) if tok.isdigit() else tok.lower() for tok in re.split(r'(\d+)', str(text))]


def discover_neuralynx_tetrode_groups(folder_path: str, strict_four_channels: bool = False) -> Dict[str, List[str]]:
    """
    Scan a Neuralynx folder and group .ncs files into tetrodes like:
    TT1a.ncs, TT1b.ncs, TT1c.ncs, TT1d.ncs -> {"TT1": [..4 files..]}
    """
    folder_path = os.path.abspath(folder_path)
    if not os.path.isdir(folder_path):
        raise NotADirectoryError(folder_path)

    pat = re.compile(r'^(TT\d+)([A-Za-z])$')
    grouped = {}
    for entry in os.listdir(folder_path):
        if not entry.lower().endswith('.ncs'):
            continue
        stem = os.path.splitext(entry)[0]
        m = pat.match(stem)
        if not m:
            continue
        tetrode, suffix = m.group(1), m.group(2).lower()
        grouped.setdefault(tetrode, []).append((suffix, os.path.join(folder_path, entry)))

    ordered = {}
    for tetrode in sorted(grouped.keys(), key=_natural_sort_key):
        items = sorted(grouped[tetrode], key=lambda x: _natural_sort_key(x[0]))
        paths = [p for _, p in items]
        if strict_four_channels and len(paths) != 4:
            continue
        ordered[tetrode] = paths

    if not ordered:
        raise RuntimeError(
            "No tetrode-style .ncs groups were found. Expected names like TT1a.ncs, TT1b.ncs, TT1c.ncs, TT1d.ncs."
        )
    return ordered


def stage_neuralynx_tetrode_folder(folder_path: str, tetrode_name: str, staged_root: Optional[str] = None,
                                   copy_files: bool = False) -> Tuple[str, List[str]]:
    """
    Create a small staging folder containing only one tetrode's .ncs files.
    This avoids depending on how channel IDs are exposed by the Neuralynx extractor.
    """
    folder_path = os.path.abspath(folder_path)
    groups = discover_neuralynx_tetrode_groups(folder_path)
    if tetrode_name not in groups:
        raise KeyError(f"Tetrode '{tetrode_name}' not found. Available: {sorted(groups)}")

    if staged_root is None:
        staged_root = os.path.join(folder_path, '_si_tetrode_staging')
    tetrode_dir = os.path.join(staged_root, tetrode_name)
    if os.path.isdir(tetrode_dir):
        shutil.rmtree(tetrode_dir)
    os.makedirs(tetrode_dir, exist_ok=True)

    staged = []
    for src in groups[tetrode_name]:
        src = os.path.abspath(src)
        dst = os.path.join(tetrode_dir, os.path.basename(src))
        try:
            if copy_files:
                shutil.copy2(src, dst)
            else:
                os.symlink(src, dst)
        except Exception:
            shutil.copy2(src, dst)
        staged.append(dst)
    return tetrode_dir, staged


def make_tetrode_contact_locations(channel_ids: List, spacing_um: float = 25.0, layout: str = 'square') -> np.ndarray:
    """Return a compact 4-contact tetrode layout in micrometers."""
    ch_ids = list(channel_ids)
    n_ch = len(ch_ids)
    if layout not in {'square', 'diamond'}:
        layout = 'square'

    if layout == 'diamond':
        base = np.array([
            [0.0, spacing_um / 2.0],
            [spacing_um / 2.0, 0.0],
            [spacing_um, spacing_um / 2.0],
            [spacing_um / 2.0, spacing_um],
        ], dtype=float)
    else:
        base = np.array([
            [0.0, 0.0],
            [spacing_um, 0.0],
            [0.0, spacing_um],
            [spacing_um, spacing_um],
        ], dtype=float)

    if n_ch <= 4:
        return base[:n_ch].copy()

    # Fallback grid if more than four channels are present for any reason
    side = int(np.ceil(np.sqrt(n_ch)))
    coords = []
    for idx in range(n_ch):
        rr = idx // side
        cc = idx % side
        coords.append([cc * spacing_um, rr * spacing_um])
    return np.asarray(coords, dtype=float)


def attach_tetrode_probe_to_recording(recording, spacing_um: float = 25.0, layout: str = 'square',
                                      electrode_diameter_um: float = 30.0):
    ch_ids = get_channel_ids_compat(recording)
    locs = make_tetrode_contact_locations(ch_ids, spacing_um=spacing_um, layout=layout)
    return attach_probe_to_recording(
        recording,
        locations_um=locs,
        channel_ids=ch_ids,
        electrode_diameter_um=electrode_diameter_um,
        ied_um=spacing_um,
    )


def load_neuralynx_tetrode_recording(folder_path: str, tetrode_name: str, staged_root: Optional[str] = None,
                                     spacing_um: float = 25.0, layout: str = 'square',
                                     electrode_diameter_um: float = 30.0, stream_id: Optional[str] = None,
                                     stream_name: Optional[str] = None, exclude_filename: Optional[List[str]] = None,
                                     copy_files: bool = False):
    """
    Load one tetrode (e.g. TT1a-d) as its own 4-channel SpikeInterface recording and attach a compact probe geometry.
    """
    folder_path = os.path.abspath(folder_path)
    tetrode_dir, _ = stage_neuralynx_tetrode_folder(
        folder_path, tetrode_name, staged_root=staged_root, copy_files=copy_files
    )
    rec = load_neuralynx_folder(
        tetrode_dir,
        stream_id=stream_id,
        stream_name=stream_name,
        exclude_filename=exclude_filename,
    )
    rec = attach_tetrode_probe_to_recording(
        rec,
        spacing_um=spacing_um,
        layout=layout,
        electrode_diameter_um=electrode_diameter_um,
    )
    return rec

# --- Probe helpers ---
def attach_probe_to_recording(recording, locations_um, channel_ids,
                              electrode_diameter_um=30.0, ied_um=200.0):
    """
    locations_um : array-like (n_channels, 2) in micrometers
    channel_ids  : list in the same order as 'locations_um'
    """
    from probeinterface import Probe
    import numpy as np

    n_ch = len(channel_ids)
    locations_um = np.asarray(locations_um, dtype=float)
    assert locations_um.shape[0] == n_ch, "locations_um must match number of channels"

    prb = Probe(ndim=2)
    prb.set_contacts(
        positions=locations_um,
        shapes='circle',
        shape_params={'radius': float(electrode_diameter_um) / 2.0},
    )

    # Contact IDs as strings (to mirror SI’s common pattern)
    try:
        prb.set_contact_ids([str(ch) for ch in channel_ids])
    except Exception:
        pass

    # *** IMPORTANT for SI 0.103+ ***
    # Map each probe contact to the *index* of the channel in the current recording.
    # Here we assume the contact order == recording channel order passed in.
    prb.set_device_channel_indices(np.arange(n_ch, dtype=int))

    # Optional annotations
    prb.annotations['manufacturer'] = 'MCS'
    prb.annotations['interelectrode_distance_um'] = float(ied_um)
    prb.annotations['electrode_diameter_um'] = float(electrode_diameter_um)

    # Attach probe (preferred API on SI 0.103)
    return recording.set_probe(prb, in_place=False)

def ensure_probe_on(recording, reference_rec):
    try:
        probe = reference_rec.get_probe()
    except Exception:
        probe = None
    if probe is None:
        return recording

    # Rebuild a probe aligned to recording’s channel order
    from probeinterface import Probe
    import numpy as np

    rec_ch_ids = list(get_channel_ids_compat(recording))
    ref_ch_ids = [str(cid) for cid in getattr(probe, "contact_ids", rec_ch_ids)]
    # map contact_id -> position index
    id_to_pos = {str(cid): i for i, cid in enumerate(ref_ch_ids)}
    pos = []
    for ch in rec_ch_ids:
        i = id_to_pos.get(str(ch), None)
        if i is None:
            pos.append([np.nan, np.nan])
        else:
            pos.append(probe.contact_positions[i])

    prb2 = Probe(ndim=2)
    
    # Try to reuse the original contact radius if available; else default to 15µm
    radius = 15.0
    try:
        csp = getattr(probe, "contact_shape_params", None)  # <- singular "shape"
        if csp and isinstance(csp, (list, tuple)) and len(csp) > 0 and "radius" in csp[0]:
            radius = float(csp[0]["radius"])
    except Exception:
        pass
    
    prb2.set_contacts(
        positions=np.asarray(pos, float),
        shapes='circle',
        shape_params={'radius': radius}
    )
    prb2.set_contact_ids([str(c) for c in rec_ch_ids])
    prb2.set_device_channel_indices(np.arange(len(rec_ch_ids), dtype=int))

    return recording.set_probe(prb2, in_place=False)

def preprocess_recording(recording, settings: MEASettings):
    """Bandpass, optional 60 Hz notch, and optional common-median reference. Returns preprocessed RecordingExtractor."""
    rec = spre.bandpass_filter(recording, freq_min=settings.hp_min_hz, freq_max=settings.hp_max_hz)
    if getattr(settings, "use_notch_60hz", True):
        notch_hz = float(
            getattr(settings, "hp_notch_hz", getattr(settings, "notch_freq_hz", 60.0))
        )
        rec = spre.notch_filter(rec, freq=notch_hz, q=30)
    if settings.use_cmr:
        rec = spre.common_reference(rec, operator="median")
    return rec


def _match_channel_id(recording, channel_id):
    """Resolve channel_id against the recording's channel IDs with tolerant string/int matching."""
    ch_ids = list(get_channel_ids_compat(recording))
    candidates = [channel_id, str(channel_id)]
    try:
        candidates.append(int(channel_id))
    except Exception:
        pass
    try:
        candidates.append(f"Ch{int(channel_id)}")
    except Exception:
        candidates.append(f"Ch{channel_id}")

    for cand in candidates:
        if cand in ch_ids:
            return cand

    key = str(channel_id).lstrip("Ch").lstrip("ch")
    for cid in ch_ids:
        if str(cid).lstrip("Ch").lstrip("ch") == key:
            return cid

    raise ValueError(f"Channel {channel_id} not found in recording.")


def _get_single_channel_trace(recording, channel_id, seconds=60):
    """Return (t, trace_uV, matched_channel_id) for one channel from a SpikeInterface recording."""
    import numpy as np

    ch_key = _match_channel_id(recording, channel_id)
    fs = float(recording.get_sampling_frequency())
    n_frames = int(min(recording.get_num_frames(segment_index=0), fs * float(seconds)))
    try:
        trace = recording.get_traces(
            start_frame=0,
            end_frame=n_frames,
            channel_ids=[ch_key],
            segment_index=0,
            return_in_uV=True,
        ).ravel()
    except TypeError:
        try:
            trace = recording.get_traces(
                start_frame=0,
                end_frame=n_frames,
                channel_ids=[ch_key],
                segment_index=0,
                return_scaled=True,
            ).ravel()
        except TypeError:
            trace = recording.get_traces(
                start_frame=0,
                end_frame=n_frames,
                channel_ids=[ch_key],
                segment_index=0,
            ).ravel()

    t = np.arange(trace.size, dtype=float) / fs
    return t, trace, ch_key


def _make_bandpass_only_recording(recording, settings: MEASettings):
    return spre.bandpass_filter(recording, freq_min=settings.hp_min_hz, freq_max=settings.hp_max_hz)


def _make_bandpass_notch_recording(recording, settings: MEASettings):
    rec = _make_bandpass_only_recording(recording, settings)
    if getattr(settings, "use_notch_60hz", True):
        notch_hz = float(
            getattr(settings, "hp_notch_hz", getattr(settings, "notch_freq_hz", 60.0))
        )
        rec = spre.notch_filter(rec, freq=notch_hz, q=30)
    return rec


def get_notch_effect_trace(recording, settings: MEASettings, channel_id, seconds=60):
    """
    Return (t, before_notch, after_notch, matched_channel_id).

    "Before notch" is the bandpassed trace before notch filtering.
    "After notch" is the same bandpassed trace after notch filtering.
    """
    rec_pre = _make_bandpass_only_recording(recording, settings)
    rec_post = _make_bandpass_notch_recording(recording, settings)

    t, tr_pre, ch_key = _get_single_channel_trace(rec_pre, channel_id, seconds=seconds)
    _t2, tr_post, _ = _get_single_channel_trace(rec_post, ch_key, seconds=seconds)
    return t, tr_pre, tr_post, ch_key


def get_cmr_effect_trace(recording, settings: MEASettings, channel_id, seconds=60):
    """
    Return (t, before_cmr, after_cmr, matched_channel_id).

    The comparison is done *after* bandpass + notch filtering, so:
      - before_cmr = post-notch trace before common median reference
      - after_cmr  = post-notch trace after common median reference
    If CMR is disabled, before_cmr and after_cmr are identical.
    """
    rec_pre_cmr = _make_bandpass_notch_recording(recording, settings)
    if settings.use_cmr:
        rec_post_cmr = spre.common_reference(rec_pre_cmr, operator="median")
    else:
        rec_post_cmr = rec_pre_cmr

    t, tr_pre, ch_key = _get_single_channel_trace(rec_pre_cmr, channel_id, seconds=seconds)
    _t2, tr_post, _ = _get_single_channel_trace(rec_post_cmr, ch_key, seconds=seconds)
    return t, tr_pre, tr_post, ch_key

def get_channel_ids_compat(recording) -> List:
    """Return channel IDs as a plain list across SI versions."""
    ch = getattr(recording, "channel_ids", None)
    if ch is None:
        ch = recording.get_channel_ids()
    return list(ch)

def compute_channel_rms_quietest(recording,
                                 scan_start_s: float,
                                 scan_end_s: float,
                                 window_s: float) -> pd.DataFrame:
    """
    For each channel, find the QUIETEST window_s within [scan_start_s, scan_end_s],
    using SpikeInterface-scaled traces (µV).

    Returns DataFrame columns:
      channel_id | rms | threshold_3x | include | is_noisy | quiet_start_s | quiet_end_s
    """
    import numpy as np
    import pandas as pd

    fs = float(recording.get_sampling_frequency())
    ch_ids = get_channel_ids_compat(recording)
    n_ch = len(ch_ids)

    total_frames = int(recording.get_num_frames())

    # sanitize time bounds
    scan_start_s = max(0.0, float(scan_start_s))
    scan_end_s   = max(scan_start_s, float(scan_end_s))
    window_s     = max(1.0 / fs, float(window_s))  # at least one sample long

    start_fr = int(round(scan_start_s * fs))
    end_fr   = int(round(scan_end_s   * fs))
    end_fr   = max(start_fr + 1, min(end_fr, total_frames))  # guard

    win_len  = max(1, int(round(window_s * fs)))
    scan_len = max(0, end_fr - start_fr)

    # Pull µV-scaled traces
    try:
        traces = recording.get_traces(start_frame=start_fr, end_frame=end_fr, return_in_uV=True)
    except TypeError:
        try:
            traces = recording.get_traces(start_frame=start_fr, end_frame=end_fr, return_scaled=True)
        except TypeError:
            traces = recording.get_traces(start_frame=start_fr, end_frame=end_fr)

    # Ensure (channels, frames)
    if traces.shape[0] == n_ch:
        pass
    elif traces.shape[1] == n_ch:
        traces = traces.T
    else:
        raise ValueError(f"Unexpected trace shape {traces.shape} vs n_ch {n_ch}")

    traces = np.asarray(traces, dtype=np.float64, order="C")

    # If the scan window is shorter than the baseline window, fall back to whole scan slice
    if scan_len <= win_len:
        # RMS over the available segment
        energy = np.sum(traces * traces, axis=1)
        rms = np.sqrt(energy / max(1, scan_len))
        quiet_start_s = np.full(n_ch, scan_start_s, dtype=float)
        quiet_end_s   = np.full(n_ch, scan_start_s + scan_len / fs, dtype=float)
    else:
        # Cumulative sum of squares to get sliding window energy fast
        ssq = np.cumsum(traces * traces, axis=1)
        # pad a zero column on the left to allow E[t]=ssq[t+win]-ssq[t]
        ssq_pad = np.concatenate([np.zeros((n_ch, 1), dtype=ssq.dtype), ssq], axis=1)
        # all valid start positions in the scan slice (0-based within slice)
        starts = np.arange(0, scan_len - win_len + 1, dtype=int)
        # energy for each start, shape (n_ch, n_starts)
        window_energy = ssq_pad[:, starts + win_len] - ssq_pad[:, starts]
        # argmin per channel
        best_starts = np.argmin(window_energy, axis=1)
        best_energy = window_energy[np.arange(n_ch), best_starts]
        rms = np.sqrt(best_energy / float(win_len))
        # convert best_starts (slice coordinates) to absolute seconds
        quiet_start_s = (start_fr + best_starts) / fs
        quiet_end_s   = quiet_start_s + (win_len / fs)

    df = pd.DataFrame({
        "channel_id": ch_ids,
        "rms": rms,                           # µV
        "threshold_3x": 3.0 * rms,            # µV
        "include": [True] * n_ch,
        "quiet_start_s": quiet_start_s,
        "quiet_end_s": quiet_end_s,
    })

    # flag noisy channels: rms > mean + 2*SD (over channels)
    valid = np.isfinite(df["rms"].values)
    mu = float(np.mean(df.loc[valid, "rms"])) if np.any(valid) else np.nan
    sd = float(np.std(df.loc[valid, "rms"], ddof=1)) if np.sum(valid) > 1 else np.nan
    df["is_noisy"] = False
    if np.isfinite(mu) and np.isfinite(sd) and sd > 0:
        df.loc[:, "is_noisy"] = df["rms"] > (mu + 2.0 * sd)

    return df

# --- UTILITY: Smart extension data loader ---
def load_extension_data(folder, ext_name, base_names, allow_csv=True, force_npy=True):
    """
    Loads extension data (metrics, PCs, etc.) from modern SI extension subfolders.
    Tries candidates like .npy, .csv, and handles both new and legacy names.
    Returns: np.ndarray or pd.DataFrame as appropriate.
    """
    import numpy as np
    import pandas as pd
    import os

    # First: Try modern extension folder (preferred in SI >= 0.100)
    ext_path = os.path.join(folder, "extensions", ext_name)
    for base in base_names:
        # Try .npy
        npy_path = os.path.join(ext_path, base + ".npy")
        if os.path.exists(npy_path):
            return np.load(npy_path, allow_pickle=True)
        # Try .csv
        csv_path = os.path.join(ext_path, base + ".csv")
        if allow_csv and os.path.exists(csv_path):
            df = pd.read_csv(csv_path, index_col=0)
            if force_npy:
                np.save(npy_path, df.values)
            return df
    # Fallback: look for alternative SI file names
    # For example: 'average.npy' for templates, 'pca_projection.npy' for PCs
    alt_npy = find_si_extension_file(folder, ext_name, ["average.npy", "pca_projection.npy", "mean.npy"])
    if alt_npy:
        return np.load(alt_npy, allow_pickle=True)
    # Top-level fallback for legacy (very rare)
    for base in base_names:
        path_npy = os.path.join(folder, base + ".npy")
        if os.path.exists(path_npy):
            return np.load(path_npy, allow_pickle=True)
    raise FileNotFoundError(f"Could not find any of {base_names} in extension '{ext_name}' of {folder}")

def _channel_slice_compat(recording, channel_ids):
    """
    Version-agnostic channel subset for SpikeInterface.
    Tries (in order):
      - recording.channel_slice(...)
      - spikeinterface.channel_slice(...)
      - spikeinterface.core.channel_slice(...)
      - SubRecordingExtractor (new home)
      - SubRecordingExtractor (legacy 'spikeextractors' pkg)
      - recording.select_channels(...)    # rare name in some builds
    """
    # no-op if same set
    try:
        cur = list(recording.get_channel_ids())
        if set(map(str, cur)) == set(map(str, channel_ids)):
            return recording
    except Exception:
        pass

    # 1) instance method
    try:
        if hasattr(recording, "channel_slice"):
            return recording.channel_slice(channel_ids=channel_ids)
    except Exception:
        pass

    # 2) top-level si.channel_slice
    try:
        import spikeinterface as si
        if hasattr(si, "channel_slice"):
            return si.channel_slice(recording, channel_ids=channel_ids)
    except Exception:
        pass

    # 3) spikeinterface.core.channel_slice
    try:
        from spikeinterface.core import channel_slice as _cs
        return _cs(recording, channel_ids=channel_ids)
    except Exception:
        pass

    # 4) SubRecordingExtractor (new)
    try:
        from spikeinterface.extractors import SubRecordingExtractor as _SIE_Sub
        return _SIE_Sub(parent_recording=recording, channel_ids=channel_ids)
    except Exception:
        pass

    # 5) SubRecordingExtractor (legacy)
    try:
        from spikeextractors import SubRecordingExtractor as _SE_Sub
        return _SE_Sub(parent_recording=recording, channel_ids=channel_ids)
    except Exception:
        pass

    # 6) alt method name found in some builds
    try:
        if hasattr(recording, "select_channels"):
            return recording.select_channels(channel_ids=channel_ids)
    except Exception:
        pass

    raise AttributeError(
        "Could not slice channels: no compatible channel-slice API found "
        "(tried .channel_slice, si.channel_slice, spikeinterface.core.channel_slice, "
        "SubRecordingExtractor (SIE/SE), select_channels)."
    )

def print_si_slicing_diagnostics(recording=None):
    """
    Prints what's available in this environment for channel slicing.
    Call it after QC so `recording` is a real RecordingExtractor.
    """
    import sys, inspect
    try:
        import spikeinterface as si
        siv = getattr(si, "__version__", "unknown")
        print(f"[SI] version={siv} file={getattr(si, '__file__', '?')}")
        print(f"[SI] has top-level si.channel_slice? {hasattr(si, 'channel_slice')}")
    except Exception as e:
        print(f"[SI] import failed: {e}")
        return

    # spikeinterface.core
    try:
        import spikeinterface.core as sicore
        print(f"[SI.core] module file={getattr(sicore, '__file__','?')}")
        print(f"[SI.core] has channel_slice? {hasattr(sicore, 'channel_slice')}")
        if hasattr(sicore, "channel_slice"):
            print(f"[SI.core] channel_slice obj={sicore.channel_slice}")
    except Exception as e:
        print(f"[SI.core] import failed: {e}")

    # SubRecordingExtractor in both possible homes
    try:
        from spikeinterface.extractors import SubRecordingExtractor as SIE_Sub
        print("[SIE] SubRecordingExtractor available (spikeinterface.extractors)")
    except Exception as e:
        print(f"[SIE] SubRecordingExtractor missing: {e}")

    try:
        from spikeextractors import SubRecordingExtractor as SE_Sub  # legacy pkg
        print("[SE ] SubRecordingExtractor available (spikeextractors)")
    except Exception as e:
        print(f"[SE ] SubRecordingExtractor missing: {e}")

    if recording is None:
        print("[probe] No recording provided; skipping live tests.")
        return

    try:
        ch_ids = list(recording.get_channel_ids())
        print(f"[rec] type={type(recording).__name__}  n_channels={len(ch_ids)}")
        print(f"[rec] has recording.channel_slice? {hasattr(recording, 'channel_slice')}")
        subset = ch_ids[:min(4, len(ch_ids))]
        print(f"[rec] trying subset: {subset}")
    except Exception as e:
        print(f"[rec] cannot introspect channel ids: {e}")
        return

    # 1) instance method
    try:
        if hasattr(recording, "channel_slice"):
            r1 = recording.channel_slice(channel_ids=subset)
            print("[test] recording.channel_slice(...)  OK")
        else:
            print("[test] recording.channel_slice(...)  not present")
    except Exception as e:
        print(f"[test] recording.channel_slice(...)  failed: {e}")

    # 2) top-level si.channel_slice
    try:
        if hasattr(si, "channel_slice"):
            r2 = si.channel_slice(recording, channel_ids=subset)
            print("[test] si.channel_slice(...)         OK")
        else:
            print("[test] si.channel_slice(...)         not present")
    except Exception as e:
        print(f"[test] si.channel_slice(...)         failed: {e}")

    # 3) spikeinterface.core.channel_slice
    try:
        import spikeinterface.core as sicore
        if hasattr(sicore, "channel_slice"):
            r3 = sicore.channel_slice(recording, channel_ids=subset)
            print("[test] sicore.channel_slice(...)     OK")
        else:
            print("[test] sicore.channel_slice(...)     not present")
    except Exception as e:
        print(f"[test] sicore.channel_slice(...)     failed: {e}")

    # 4) SubRecordingExtractor (new home)
    try:
        from spikeinterface.extractors import SubRecordingExtractor as SIE_Sub
        r4 = SIE_Sub(parent_recording=recording, channel_ids=subset)
        print("[test] SIE.SubRecordingExtractor(...)  OK")
    except Exception as e:
        print(f"[test] SIE.SubRecordingExtractor(...)  failed: {e}")

    # 5) SubRecordingExtractor (legacy pkg)
    try:
        from spikeextractors import SubRecordingExtractor as SE_Sub
        r5 = SE_Sub(parent_recording=recording, channel_ids=subset)
        print("[test] SE.SubRecordingExtractor(...)   OK")
    except Exception as e:
        print(f"[test] SE.SubRecordingExtractor(...)   failed: {e}")

    # 6) any other method name
    try:
        if hasattr(recording, "select_channels"):
            r6 = recording.select_channels(channel_ids=subset)
            print("[test] recording.select_channels(...) OK")
        else:
            print("[test] recording.select_channels(...) not present")
    except Exception as e:
        print(f"[test] recording.select_channels(...) failed: {e}")

def subset_recording_channels(recording, channel_ids):
    rec = _channel_slice_compat(recording, channel_ids)
    # keep probe metadata from parent
    try:
        return ensure_probe_on(rec, recording)
    except Exception:
        return rec

def _filter_sorter_kwargs(sorter_name: str, params: dict):
    """
    Keep only keys accepted by this sorter in the current SpikeInterface version.
 
    SpikeInterface 0.103 uses get_default_sorter_params().
    Older code used get_default_params(), which is not available in SI 0.103.
    This helper tries both APIs and removes invalid keys when defaults are available.
    """
    try:
        import spikeinterface.sorters as ss
 
        defaults = None
        for fn_name in ("get_default_sorter_params", "get_default_params"):
            if hasattr(ss, fn_name):
                try:
                    defaults = getattr(ss, fn_name)(sorter_name)
                    break
                except Exception:
                    pass
 
        if defaults is None:
            print(f"[warn] Could not fetch default params for {sorter_name}; passing params unchanged.")
            return params
 
        allowed = set(defaults.keys())
        clean = {k: v for k, v in params.items() if k in allowed}
        extra = [k for k in params.keys() if k not in allowed]
 
        if extra:
            print(f"[warn] Ignoring {sorter_name} unknown params: {extra}")
 
        return clean
 
    except Exception as e:
        print(f"[warn] Parameter filtering failed for {sorter_name}: {e}")
        return params

def run_mountainsort4(recording, settings: MEASettings, output_folder: str):
    """MountainSort4: no re-filtering upstream-preprocessed data; keep MS4 internal whitening."""
    import spikeinterface.sorters as ss
 
    # Cross-platform temp directory.
    # Prevents MS4 from falling back to '/tmp' on Windows.
    _prepare_sorter_tempdir(output_folder, sorter_name="MountainSort4")
 
    sorting = ss.run_sorter(
        sorter_name="mountainsort4",
        recording=recording,
        folder=output_folder,
        remove_existing_folder=True,
        verbose=True,
 
        # MS4 params from UI
        detect_threshold=settings.detect_threshold,
        detect_sign=settings.detect_sign,
        adjacency_radius=settings.adjacency_radius,
        clip_size=settings.clip_size,
        detect_interval=getattr(settings, "detect_interval", 10),
 
        # Preprocessing policy
        filter=False,        # bandpass/notch/CMR done upstream in Tab 1
        whiten=True,         # let MS4 whiten internally
        num_workers=settings.n_jobs,
    )
    return sorting

def run_kilosort4(recording, settings: MEASettings, output_folder: str):
    import json
    import spikeinterface.sorters as ss
 
    # Cross-platform temp directory.
    # Also helps keep temporary files near the KS4 run folder.
    _prepare_sorter_tempdir(output_folder, sorter_name="Kilosort4")
 
    # Baseline defaults; UI JSON can override.
    params = dict(
        do_CAR=True,
        skip_kilosort_preprocessing=False,
        torch_device="auto",
    )
 
    js = getattr(settings, "ks4_params_json", "") or ""
    if js:
        try:
            user = json.loads(js)
            if isinstance(user, dict):
                params.update(user)
        except Exception as e:
            print(f"[warn] Could not parse KS4 params JSON; using defaults/valid existing params. Error: {e}")
 
    # Filter to only keys accepted by this SpikeInterface version.
    params = _filter_sorter_kwargs("kilosort4", params)
 
    sorting = ss.run_sorter(
        sorter_name="kilosort4",
        recording=recording,
        folder=output_folder,
        remove_existing_folder=True,
        verbose=True,
        **params,
    )
    return sorting

def _sanitize_spykingcircus2_params(params: dict) -> dict:
    """
    Sanitize SpyKING Circus2 params for SpikeInterface 0.103 compatibility.
 
    Handles known SI 0.103 SC2 pitfalls:
    - Removes unsupported top-level matched_filtering.
    - Removes unsupported whitening.neighbors.
    - Converts local whitening presets to global whitening.
    - Converts cache_preprocessing.memory_limit from strings like "2G" or "0.25"
      to a valid fractional memory limit.
    - Converts clustering method aliases such as graph_clustering to graph-clustering.
    - Forces templates_from_svd=False to avoid SI 0.103 SVD template-length mismatch.
    - Sanitizes the merging block so only merging.max_distance_um is passed.
    """
    import copy
 
    params = copy.deepcopy(params) if isinstance(params, dict) else {}
    # The GUI already passes a Tab 1 preprocessed recording to SC2.
    # Force SC2 not to perform an additional internal filtering/preprocessing pass.
    params["apply_preprocessing"] = False
 
    # Remove filtering block because it is irrelevant when apply_preprocessing=False
    # and older preset keys may not match the SI 0.103 SC2 nested schema.
    params.pop("filtering", None)
 
    # 1) Remove legacy unsupported top-level key.
    params.pop("matched_filtering", None)
 
    # 2) Disable SVD-derived templates for robustness.
    # In 32 kHz tetrode data, SI 0.103 SC2 produced a 96-sample SVD reconstruction
    # for a 128-sample template window, causing a broadcast error.
    params["templates_from_svd"] = False
 
    # 3) Sanitize whitening nested params.
    whitening = params.get("whitening", None)
    if isinstance(whitening, dict):
        # In SI 0.103, this nested key can be forwarded incorrectly to random chunk selection.
        whitening.pop("neighbors", None)
 
        # Use global whitening for compatibility with this GUI's SC2 presets.
        if whitening.get("mode", None) == "local":
            whitening["mode"] = "global"
 
        params["whitening"] = whitening
 
    # 4) Sanitize cache_preprocessing.memory_limit.
    cache = params.get("cache_preprocessing", None)
    if isinstance(cache, dict):
        ml = cache.get("memory_limit", None)
 
        # SpikeInterface SC2 expects a float fraction in ]0, 1[, not "2G" or "0.25".
        if isinstance(ml, str) or ml is None:
            cache["memory_limit"] = 0.25
        else:
            try:
                ml_float = float(ml)
                if not (0.0 < ml_float < 1.0):
                    ml_float = 0.25
                cache["memory_limit"] = ml_float
            except Exception:
                cache["memory_limit"] = 0.25
 
        params["cache_preprocessing"] = cache
 
    # 5) Sanitize clustering method aliases.
    clustering = params.get("clustering", None)
    if isinstance(clustering, dict):
        method = clustering.get("method", None)
 
        aliases = {
            "graph_clustering": "graph-clustering",
            "tdc_clustering": "tdc-clustering",
            "circus_clustering": "circus-clustering",
        }
 
        if method in aliases:
            clustering["method"] = aliases[method]
        elif isinstance(method, str) and "_" in method:
            # General fallback: SI clustering methods commonly use hyphens.
            clustering["method"] = method.replace("_", "-")
 
        params["clustering"] = clustering
 
    # 6) Sanitize merging block.
    # SI 0.103 SC2 default schema exposes only merging.max_distance_um.
    # Legacy keys such as auto_merge_units, acg_threshold, and ccg_threshold
    # can be forwarded into unit-location computation and crash.
    merging = params.get("merging", None)
 
    if isinstance(merging, dict):
        old_keys = set(merging.keys())
 
        md = merging.get("max_distance_um", None)
 
        if md is None:
            general = params.get("general", {})
            if isinstance(general, dict):
                md = general.get("radius_um", 80.0)
            else:
                md = 80.0
 
        try:
            md = float(md)
        except Exception:
            md = 80.0
 
        params["merging"] = {"max_distance_um": md}
 
        removed = sorted(old_keys - {"max_distance_um"})
        if removed:
            print(f"[warn] Removed unsupported SC2 merging params: {removed}. Using max_distance_um={md}.")
    elif merging is None:
        general = params.get("general", {})
        if isinstance(general, dict):
            md = general.get("radius_um", 80.0)
        else:
            md = 80.0
 
        try:
            md = float(md)
        except Exception:
            md = 80.0
 
        params["merging"] = {"max_distance_um": md}
 
    return params

def run_spykingcircus2(recording, settings: MEASettings, output_folder: str):
    import json
    import spikeinterface.sorters as ss
 
    # Cross-platform temp directory.
    _prepare_sorter_tempdir(output_folder, sorter_name="SpyKING Circus2")
 
    # SpikeInterface 0.103 spykingcircus2 expects nested parameter groups.
    # Do NOT pass legacy top-level keys such as:
    #   filter, whiten, num_workers, matched_filtering
    #
    # The GUI already passes the Tab 1 preprocessed recording to SC2, so
    # apply_preprocessing=False prevents duplicate preprocessing unless the
    # researcher explicitly changes it in the JSON.
    params = dict(
        apply_preprocessing=False,
        job_kwargs={"n_jobs": int(getattr(settings, "n_jobs", 1))},
    )
 
    js = getattr(settings, "sc2_params_json", "") or ""
    if js:
        try:
            user = json.loads(js)
            if isinstance(user, dict):
                params.update(user)
        except Exception as e:
            print(f"[warn] Could not parse SC2 params JSON; using defaults/valid existing params. Error: {e}")
 
    # Sanitize nested SC2 params before top-level filtering.
    params = _sanitize_spykingcircus2_params(params)
    
    print(f"[info] SC2 templates_from_svd={params.get('templates_from_svd')}")
    print(f"[info] SC2 merging params={params.get('merging')}")
 
    params = _filter_sorter_kwargs("spykingcircus2", params)
 
    sorting = ss.run_sorter(
        sorter_name="spykingcircus2",
        recording=recording,
        folder=output_folder,
        remove_existing_folder=True,
        verbose=True,
        **params,
    )
    return sorting

def extract_waveforms_and_pcs(recording, sorting, settings: MEASettings, waveforms_folder: str):
    """
    Build a SortingAnalyzer and compute waveforms/templates/PCs.
    - On SI 0.103.0, there is no si.save_extractor; if the Recording exposes .save(...),
      we persist to a binary folder for mmap-friendly workers; otherwise we proceed in-memory.
    - Adds safer multiprocessing hints for Jupyter/headless (spawn, modest process cap).
    """
    import os
    import spikeinterface as si
 
    if os.path.exists(waveforms_folder):
        raise RuntimeError(
            f"Waveforms output folder already exists: {waveforms_folder}\n"
            "Please choose a different output path, or manually delete the folder."
        )
 
    # --- Try to persist the preprocessed recording using the instance API (preferred on 0.103) ---
    rec_for_analysis = recording
    try:
        if hasattr(recording, "save"):
            rec_folder = os.path.join(waveforms_folder, "preproc_recording")
            # Most extractors accept format="binary_folder" in 0.103
            recording.save(folder=rec_folder, format="binary_folder")
            # reload with SpikeInterface loader (top-level load_extractor exists on 0.103)
            rec_for_analysis = si.load_extractor(rec_folder)
    except Exception as _e:
        # If saving fails for any reason, just fall back to in-memory recording
        rec_for_analysis = recording
 
    # --- Create analyzer on the (possibly persisted) recording ---
    analyzer = si.create_sorting_analyzer(
        sorting=sorting,
        recording=rec_for_analysis,
        format="binary_folder",
        folder=waveforms_folder,
        overwrite=True,
    )
    
 
    # --- Compute pipeline (with conservative parallelism & spawn context) ---
    # Cap waveform workers even on big machines to avoid oversubscription
    n_wave_jobs = max(1, min(int(getattr(settings, "n_jobs", 8)), 8))
 
    analyzer.compute("random_spikes", max_spikes_per_unit=settings.max_spikes_per_unit, n_jobs=1)
 
    # Waveforms
    try:
        analyzer.compute(
            "waveforms",
            ms_before=settings.ms_before,
            ms_after=settings.ms_after,
            n_jobs=n_wave_jobs,
            mp_context="spawn",   # safer in notebooks/headless
        )
    except TypeError:
        analyzer.compute(
            "waveforms",
            ms_before=settings.ms_before,
            ms_after=settings.ms_after,
            n_jobs=n_wave_jobs,
        )
 
    # Templates
    analyzer.compute("templates", n_jobs=1)
 
    # PCA
    try:
        analyzer.compute(
            "principal_components",
            n_components=3,
            mode="by_channel_global",
            n_jobs=n_wave_jobs,
            mp_context="spawn",
        )
    except TypeError:
        analyzer.compute(
            "principal_components",
            n_components=3,
            mode="by_channel_global",
            n_jobs=n_wave_jobs,
        )
 
    return analyzer

def compute_unit_quality_metrics(analyzer, include_pc_metrics: bool) -> pd.DataFrame:
    metric_names = [
        'firing_rate',
        'isi_violations_ratio',
        'snr',
        'amplitude_cutoff',
        'presence_ratio',
        'isolation_distance',
        'l_ratio',
    ]
    qm = si.qualitymetrics.compute_quality_metrics(
        analyzer,
        metric_names=metric_names,
        skip_pc_metrics=not include_pc_metrics,
        delete_existing_metrics=False,
    )
    return qm

def curate_units(metrics_df: pd.DataFrame,
                 isi_viol_thresh: float = 0.005,
                 amp_cutoff_thresh: float = 0.1,
                 snr_thresh: float = 5.0,
                 presence_ratio_thresh: float = 0.8) -> List[int]:
    """Return list of unit_ids to keep based on thresholds."""
    keep_mask = pd.Series(True, index=metrics_df.index)
    if 'isi_violations_ratio' in metrics_df.columns:
        keep_mask &= metrics_df['isi_violations_ratio'] <= isi_viol_thresh
    if 'amplitude_cutoff' in metrics_df.columns:
        keep_mask &= metrics_df['amplitude_cutoff'] <= amp_cutoff_thresh
    if 'snr' in metrics_df.columns:
        keep_mask &= metrics_df['snr'] >= snr_thresh
    if 'presence_ratio' in metrics_df.columns:
        keep_mask &= metrics_df['presence_ratio'] >= presence_ratio_thresh
    return metrics_df.index[keep_mask].tolist()


def _read_quality_metrics_table(metrics_path: str) -> pd.DataFrame:
    """Read a quality-metrics table and normalize the unit identifier column."""
    path = os.path.abspath(str(metrics_path))
    if not os.path.exists(path):
        raise FileNotFoundError(path)

    ext = os.path.splitext(path)[1].lower()
    if ext in {".xlsx", ".xls"}:
        df = pd.read_excel(path)
    elif ext in {".csv", ".txt", ".tsv"}:
        sep = "\t" if ext in {".txt", ".tsv"} else ","
        df = pd.read_csv(path, sep=sep)
    else:
        raise ValueError(f"Unsupported quality-metrics file type: {ext}")

    cols_lower = {str(c).strip().lower(): c for c in df.columns}
    if "unit_id" in cols_lower:
        unit_col = cols_lower["unit_id"]
        if unit_col != "unit_id":
            df = df.rename(columns={unit_col: "unit_id"})
    else:
        first_col = df.columns[0] if len(df.columns) else None
        if first_col is not None and str(first_col).lower().startswith("unnamed"):
            df = df.rename(columns={first_col: "unit_id"})
        elif first_col is not None and str(first_col).strip().lower() in {"index", "id", "unit", "unit_ids"}:
            df = df.rename(columns={first_col: "unit_id"})
        else:
            df.insert(0, "unit_id", np.arange(1, len(df) + 1))

    return df


def _numeric_series(df: pd.DataFrame, col: str, default=np.nan) -> pd.Series:
    if col in df.columns:
        return pd.to_numeric(df[col], errors="coerce")
    return pd.Series([default] * len(df), index=df.index, dtype="float64")


def _find_first_column(df: pd.DataFrame, aliases: List[str]) -> Optional[str]:
    lower = {str(c).strip().lower(): c for c in df.columns}
    for a in aliases:
        if a.strip().lower() in lower:
            return lower[a.strip().lower()]
    return None


def _choose_contamination_column(df: pd.DataFrame) -> Tuple[pd.Series, str, str]:
    """Choose a contamination/ISI column robustly using the user's historical preference order."""
    if "isi_violations" in df.columns:
        return pd.to_numeric(df["isi_violations"], errors="coerce"), "isi_violations", "allen_like"
    if "isi_violations_ratio" in df.columns:
        vals = pd.to_numeric(df["isi_violations_ratio"], errors="coerce")
        mx = float(np.nanmax(vals)) if np.any(np.isfinite(vals)) else np.nan
        mode = "allen_like" if (np.isfinite(mx) and mx > 1.0) else "ratio_or_fraction"
        return vals, "isi_violations_ratio", mode
    if "rp_contamination" in df.columns:
        return pd.to_numeric(df["rp_contamination"], errors="coerce"), "rp_contamination", "fallback"
    return pd.Series([np.nan] * len(df), index=df.index, dtype="float64"), "MISSING", "missing"


def curate_quality_metrics_two_pass(
    metrics_path: str,
    output_path: Optional[str] = None,
    amp_cutoff_max: float = 0.10,
    presence_min: float = 0.90,
    contamination_max: float = 0.50,
    nn_hit_rate_min: float = 0.70,
    apply_nn_gate: bool = True,
    treat_amp_nan_as_keep: bool = True,
    treat_missing_nn_as_keep: bool = True,
    require_nn_column: bool = False,
) -> Tuple[str, Dict[str, object]]:
    """Run the user's two-pass unit curation in one step and write one curated workbook."""
    metrics_path = os.path.abspath(str(metrics_path))
    df = _read_quality_metrics_table(metrics_path)
    if df.empty:
        raise ValueError("Quality-metrics table is empty.")

    out = df.copy()
    if "unit_id" not in out.columns:
        out.insert(0, "unit_id", np.arange(1, len(out) + 1))
    else:
        cols = ["unit_id"] + [c for c in out.columns if c != "unit_id"]
        out = out[cols]

    amp = _numeric_series(out, "amplitude_cutoff")
    pres = _numeric_series(out, "presence_ratio")
    contam, contam_col, contam_mode = _choose_contamination_column(out)

    amp_present = "amplitude_cutoff" in out.columns
    pres_present = "presence_ratio" in out.columns
    contam_present = contam_col != "MISSING"

    if amp_present:
        amp_ok = amp <= float(amp_cutoff_max)
        if treat_amp_nan_as_keep:
            amp_ok = amp_ok | amp.isna()
    else:
        amp_ok = pd.Series([False] * len(out), index=out.index)
    pres_ok = pres >= float(presence_min) if pres_present else pd.Series([False] * len(out), index=out.index)
    contam_ok = contam <= float(contamination_max) if contam_present else pd.Series([False] * len(out), index=out.index)

    out["cur_amp_cutoff_ok"] = amp_ok.astype(bool)
    out["cur_presence_ok"] = pres_ok.astype(bool)
    out["cur_contam_ok"] = contam_ok.astype(bool)
    out["cur_amp_cutoff_is_nan"] = amp.isna()
    out["cur_contamination_column"] = contam_col
    out["cur_contamination_interpretation"] = contam_mode
    out["cur_pass1_keep"] = out[["cur_amp_cutoff_ok", "cur_presence_ok", "cur_contam_ok"]].all(axis=1)

    nn_aliases = ["nn_hit_rate", "nn_isolation", "nearest_neighbors_hit_rate", "nn_hitrate"]
    nn_col = _find_first_column(out, nn_aliases)
    nn_available = nn_col is not None
    if apply_nn_gate:
        if nn_available:
            nn = pd.to_numeric(out[nn_col], errors="coerce")
            out["nn_hit_rate"] = nn
            nn_ok = nn >= float(nn_hit_rate_min)
            if treat_missing_nn_as_keep:
                nn_ok = nn_ok | nn.isna()
            out["cur_nn_hit_rate_ok"] = nn_ok.astype(bool)
            out["cur_nn_gate_applied"] = True
            out["cur_nn_column"] = nn_col
        else:
            if require_nn_column:
                raise ValueError(
                    "NN gate is enabled and require_nn_column=True, but no nn_hit_rate-like column was found. "
                    f"Looked for: {nn_aliases}"
                )
            out["nn_hit_rate"] = np.nan
            out["cur_nn_hit_rate_ok"] = True
            out["cur_nn_gate_applied"] = False
            out["cur_nn_column"] = "MISSING"
    else:
        out["cur_nn_hit_rate_ok"] = True
        out["cur_nn_gate_applied"] = False
        out["cur_nn_column"] = nn_col if nn_available else "MISSING"

    out["final_keep"] = out["cur_pass1_keep"].astype(bool) & out["cur_nn_hit_rate_ok"].astype(bool)
    out["curation_status"] = np.where(out["final_keep"], "kept", "flagged")

    def _drop_reasons(row):
        reasons = []
        if not bool(row["cur_amp_cutoff_ok"]):
            reasons.append("amplitude_cutoff")
        if not bool(row["cur_presence_ok"]):
            reasons.append("presence_ratio")
        if not bool(row["cur_contam_ok"]):
            reasons.append(str(contam_col))
        if not bool(row["cur_nn_hit_rate_ok"]):
            reasons.append("nn_hit_rate")
        return ",".join(reasons)

    out["final_drop_reasons"] = out.apply(_drop_reasons, axis=1)
    kept = out.loc[out["final_keep"]].copy()
    flagged = out.loc[~out["final_keep"]].copy()

    if output_path is None or str(output_path).strip() == "":
        output_path = os.path.join(os.path.dirname(metrics_path), "curated_units_two_pass.xlsx")
    output_path = os.path.abspath(str(output_path))
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    n_total = int(len(out))
    n_keep = int(out["final_keep"].sum())
    n_flag = int((~out["final_keep"]).sum())
    summary_df = pd.DataFrame([
        {"metric": "total_units", "value": n_total},
        {"metric": "kept_units", "value": n_keep},
        {"metric": "flagged_units", "value": n_flag},
        {"metric": "kept_percent", "value": (100.0 * n_keep / n_total) if n_total else np.nan},
        {"metric": "amp_cutoff_ok_percent", "value": float(out["cur_amp_cutoff_ok"].mean() * 100.0)},
        {"metric": "presence_ok_percent", "value": float(out["cur_presence_ok"].mean() * 100.0)},
        {"metric": "contam_ok_percent", "value": float(out["cur_contam_ok"].mean() * 100.0)},
        {"metric": "nn_hit_rate_ok_percent", "value": float(out["cur_nn_hit_rate_ok"].mean() * 100.0)},
        {"metric": "contamination_column", "value": contam_col},
        {"metric": "contamination_interpretation", "value": contam_mode},
        {"metric": "nn_column", "value": nn_col if nn_available else "MISSING"},
        {"metric": "nn_gate_applied", "value": bool(apply_nn_gate and nn_available)},
    ])
    settings_df = pd.DataFrame([
        {"parameter": "metrics_path", "value": metrics_path},
        {"parameter": "output_path", "value": output_path},
        {"parameter": "amp_cutoff_max", "value": amp_cutoff_max},
        {"parameter": "presence_min", "value": presence_min},
        {"parameter": "contamination_max", "value": contamination_max},
        {"parameter": "nn_hit_rate_min", "value": nn_hit_rate_min},
        {"parameter": "apply_nn_gate", "value": apply_nn_gate},
        {"parameter": "treat_amp_nan_as_keep", "value": treat_amp_nan_as_keep},
        {"parameter": "treat_missing_nn_as_keep", "value": treat_missing_nn_as_keep},
        {"parameter": "require_nn_column", "value": require_nn_column},
    ])

    with pd.ExcelWriter(output_path, engine="xlsxwriter") as xl:
        out.to_excel(xl, sheet_name="combined_units", index=False)
        kept.to_excel(xl, sheet_name="kept_only", index=False)
        flagged.to_excel(xl, sheet_name="flagged_only", index=False)
        summary_df.to_excel(xl, sheet_name="curation_summary", index=False)
        settings_df.to_excel(xl, sheet_name="curation_settings", index=False)
        try:
            for sheet_name, df_sheet in {
                "combined_units": out,
                "kept_only": kept,
                "flagged_only": flagged,
                "curation_summary": summary_df,
                "curation_settings": settings_df,
            }.items():
                ws = xl.sheets[sheet_name]
                ws.freeze_panes(1, 0)
                for idx, col in enumerate(df_sheet.columns):
                    max_len = int(df_sheet[col].astype(str).str.len().max()) if len(df_sheet) else 0
                    width = max(12, min(40, max(len(str(col)), max_len) + 2))
                    ws.set_column(idx, idx, width)
        except Exception:
            pass

    summary = {
        "output_path": output_path,
        "total_units": n_total,
        "kept_units": n_keep,
        "flagged_units": n_flag,
        "kept_percent": (100.0 * n_keep / n_total) if n_total else np.nan,
        "amp_cutoff_ok_percent": float(out["cur_amp_cutoff_ok"].mean() * 100.0),
        "presence_ok_percent": float(out["cur_presence_ok"].mean() * 100.0),
        "contam_ok_percent": float(out["cur_contam_ok"].mean() * 100.0),
        "nn_hit_rate_ok_percent": float(out["cur_nn_hit_rate_ok"].mean() * 100.0),
        "contamination_column": contam_col,
        "contamination_interpretation": contam_mode,
        "nn_column": nn_col if nn_available else "MISSING",
        "nn_gate_applied": bool(apply_nn_gate and nn_available),
    }
    return output_path, summary

def export_unit_spike_times(sorting, fs: float, out_xlsx: str):
    """Export spike times with one column per unit (wide format).
    Writes a SINGLE sheet 'unit_times' when rows <= Excel's limit.
    If any unit exceeds the row limit, rows are split across multiple sheets.
    """
    MAX_ROWS = 1_048_000  # safe under Excel's 1,048,576
    unit_ids = list(sorting.get_unit_ids())

    # Collect spike-time arrays per unit and track the maximum length
    cols = {}
    max_len = 0
    for uid in unit_ids:
        st = sorting.get_unit_spike_train(unit_id=uid)
        times = (st / fs).astype(float) if len(st) else np.array([], dtype=float)
        name = f"unit_{uid}"
        cols[name] = times
        if times.size > max_len:
            max_len = times.size

    with pd.ExcelWriter(out_xlsx, engine='xlsxwriter') as writer:
        if max_len == 0:
            # no spikes at all
            pd.DataFrame({}).to_excel(writer, sheet_name="unit_times", index=False)
            return

        if max_len <= MAX_ROWS:
            # Single-sheet case (preferred layout)
            data = {}
            for name, arr in cols.items():
                if arr.size < max_len:
                    pad = np.full((max_len - arr.size,), np.nan, dtype=float)
                    data[name] = np.concatenate([arr, pad])
                else:
                    data[name] = arr
            df = pd.DataFrame(data)
            df.to_excel(writer, sheet_name="unit_times", index=False)
        else:
            # Very rare: split by rows across multiple sheets to avoid truncation
            for start in range(0, max_len, MAX_ROWS):
                end = min(start + MAX_ROWS, max_len)
                slice_len = end - start
                data = {}
                for name, arr in cols.items():
                    if arr.size >= end:
                        data[name] = arr[start:end]
                    elif arr.size > start:
                        sl = arr[start:arr.size]
                        pad = np.full((slice_len - sl.size,), np.nan, dtype=float)
                        data[name] = np.concatenate([sl, pad])
                    else:
                        data[name] = np.full((slice_len,), np.nan, dtype=float)
                df = pd.DataFrame(data)
                sheet = f"unit_times_{start // MAX_ROWS + 1}"[:31]
                df.to_excel(writer, sheet_name=sheet, index=False)

def _acg_hist(spikes_s: np.ndarray, max_lag_s=0.1, bin_ms=2.0):
    """Memory-safe ACG: returns (lag_centers_ms, counts)."""
    if len(spikes_s) < 2:
        return np.array([]), np.array([])
    spikes = np.asarray(spikes_s, dtype=float)
    bin_w = bin_ms / 1000.0
    nbins = int(round(2 * max_lag_s / bin_w))
    edges = np.linspace(-max_lag_s, max_lag_s, nbins + 1)
    counts = np.zeros(nbins, dtype=int)
    j = 0
    for i in range(len(spikes)):
        while j < len(spikes) and spikes[j] < spikes[i] - max_lag_s:
            j += 1
        k = j
        while k < len(spikes) and spikes[k] <= spikes[i] + max_lag_s:
            if k != i:
                lag = spikes[k] - spikes[i]
                idx = int((lag + max_lag_s) // bin_w)
                if 0 <= idx < nbins:
                    counts[idx] += 1
            k += 1
    centers_ms = (edges[:-1] + edges[1:]) * 500.0  # s→ms and center
    return centers_ms, counts

def _get_pca_for_unit(analyzer, uid, max_points=5000):
    """
    Return PCA projections for one unit as an (n_spikes, n_components) array.
    Works on SI >= 0.103 (sparse principal_components). Uses API first, then
    falls back to slicing pca_projection.npy with random_spikes_indices.npy.
    """
    import os
    import numpy as np

    # Try modern API first
    try:
        ext = analyzer.get_extension("principal_components")
    except Exception:
        ext = None

    if ext is not None:
        # Newer SI often returns (X, labels); we request only this uid
        try:
            out = ext.get_some_projections(unit_ids=[uid])
            if isinstance(out, (tuple, list)) and len(out) == 2:
                X, labels = out
                if X is not None and X.ndim == 2 and X.size:
                    # When unit_ids=[uid], some builds already filter X;
                    # if labels provided, mask them; else take X as-is.
                    if labels is not None and np.size(labels):
                        mask = (labels == uid)
                        X = X[mask]
                    if X.shape[0] > max_points:
                        idx = np.linspace(0, X.shape[0] - 1, max_points, dtype=int)
                        X = X[idx]
                    return X[:, :3] if X.shape[1] >= 3 else X
            elif out is not None:
                X = out
                if X.ndim == 2 and X.size:
                    if X.shape[0] > max_points:
                        idx = np.linspace(0, X.shape[0] - 1, max_points, dtype=int)
                        X = X[idx]
                    return X[:, :3] if X.shape[1] >= 3 else X
        except Exception:
            pass

        # Older API variant
        try:
            if hasattr(ext, "get_projections_one_unit"):
                X = ext.get_projections_one_unit(uid)
                if X is not None and X.ndim == 2 and X.size:
                    if X.shape[0] > max_points:
                        idx = np.linspace(0, X.shape[0] - 1, max_points, dtype=int)
                        X = X[idx]
                    return X[:, :3] if X.shape[1] >= 3 else X
        except Exception:
            pass

    # Disk fallback: slice by cumulative lengths from random_spikes_indices.npy
    try:
        base = analyzer.folder
        pca_path = os.path.join(base, "extensions", "principal_components", "pca_projection.npy")
        rs_path = os.path.join(base, "extensions", "random_spikes", "random_spikes_indices.npy")
        if os.path.exists(pca_path) and os.path.exists(rs_path):
            Xall = np.load(pca_path, allow_pickle=True)
            rs = np.load(rs_path, allow_pickle=True)
            unit_ids = list(analyzer.sorting.get_unit_ids())

            # rs can be an object array of arrays (or lists-of-arrays)
            lengths = []
            for arr in rs:
                if isinstance(arr, (list, tuple)):
                    c = sum(len(np.asarray(a)) for a in arr if np.asarray(a).size)
                else:
                    a = np.asarray(arr)
                    c = len(a) if a.size else 0
                lengths.append(int(c))
            offsets = np.concatenate(([0], np.cumsum(lengths)))

            uid_to_idx = {u: i for i, u in enumerate(unit_ids)}
            if uid not in uid_to_idx:
                return None
            i = uid_to_idx[uid]
            sl = slice(int(offsets[i]), int(offsets[i + 1]))
            X = Xall[sl]
            if X is None or X.ndim != 2 or X.size == 0:
                return None
            if X.shape[0] > max_points:
                idx = np.linspace(0, X.shape[0] - 1, max_points, dtype=int)
                X = X[idx]
            return X[:, :3] if X.shape[1] >= 3 else X
    except Exception:
        pass

    return None

# --- Waveform feature helpers (polarity-standardized) ---
def _interp_crossing_time(x, y, y_level):
    """
    Linear interpolation to find x where y crosses y_level.
    Returns None if no crossing is found in the given segment.
    """
    diff = y - y_level
    # look for a sign change between consecutive points
    cross_idx = np.where(np.sign(diff[:-1]) * np.sign(diff[1:]) <= 0)[0]
    if cross_idx.size == 0:
        return None
    i = int(cross_idx[0])
    x0, x1, y0, y1 = float(x[i]), float(x[i+1]), float(y[i]), float(y[i+1])
    if y1 == y0:
        return x0
    t = (y_level - y0) / (y1 - y0)
    return x0 + t * (x1 - x0)

def _compute_waveform_features(w, fs_hz, flip_to_negative_first=True):
    """
    Compute template features on a single-channel waveform 'w' (µV).
    Returns a dict with: peak_uV, trough_uV, ptp_uV, peak_abs_uV, ttp_ms, ap_halfwidth_ms.
    If flip_to_negative_first=True: if the largest-magnitude extremum is positive,
    invert the waveform so main event is negative-first (standardizes TTP).
    """
    # baseline center
    w0 = w - np.median(w)

    # Standardize polarity (recommended)
    if flip_to_negative_first and abs(np.max(w0)) > abs(np.min(w0)):
        w0 = -w0

    # trough (most negative) then subsequent peak
    i_tr = int(np.argmin(w0))
    if i_tr < len(w0) - 2 and (len_w1 := len(w0) - (i_tr + 1)) > 0:
        seg = w0[i_tr + 1:]
        i_pk = i_tr + 1 + int(np.argmax(seg)) if seg.size else int(np.argmax(w0))
    else:
        i_pk = int(np.argmax(w0))

    # amplitudes
    peak_uV   = float(w0[i_pk])
    trough_uV = float(w0[i_tr])
    ptp_uV    = float(np.max(w0) - np.min(w0))
    peak_abs  = float(np.max(np.abs(w0)))

    # times
    t_ms  = np.arange(len(w0), dtype=float) * 1000.0 / float(fs_hz)
    ttp_ms = float((i_pk - i_tr) * 1000.0 / float(fs_hz))

    # AP half-width at 0.5*(trough→peak) around the event
    half_level = (w0[i_tr] + w0[i_pk]) / 2.0
    left_idx  = slice(max(0, i_tr - 200), i_tr + 1)                         
    right_idx = slice(i_tr, min(len(w0), i_pk + 200))
    t_left  = _interp_crossing_time(t_ms[left_idx],  w0[left_idx],  half_level)
    t_right = _interp_crossing_time(t_ms[right_idx], w0[right_idx], half_level)
    ap_halfwidth_ms = float(t_right - t_left) if (t_left is not None and t_right is not None and t_right >= t_left) else float("nan")

    return dict(
        peak_uV=peak_uV,
        trough_uV=trough_uV,
        ptp_uV=ptp_uV,
        peak_abs_uV=peak_abs,
        ttp_ms=ttp_ms,
        ap_halfwidth_ms=ap_halfwidth_ms,
    )

def make_unit_report_figures(
    analyzer, output_dir: str,
    settings: MEASettings,
    metrics_df: Optional[pd.DataFrame] = None
):
    """
    Generate per-unit PNGs: waveform, ISI histogram, ACG, PCA 2D/3D scatter, amplitude vs time.
    Uses the 'templates' extension for average waveforms; falls back to PCA projections if needed.
    """
    import os
    import numpy as np
    import pandas as pd
    import matplotlib.pyplot as plt

    os.makedirs(output_dir, exist_ok=True)
    sorting = analyzer.sorting
    recording = analyzer.recording
    fs = float(recording.get_sampling_frequency())
    unit_ids = sorting.get_unit_ids()
    if len(unit_ids) == 0:
        pd.DataFrame(columns=["unit_id"]).to_excel(os.path.join(output_dir, "unit_stats.xlsx"), index=False)
        return

    # --- Get SI extension objects ---
    ext_pca = analyzer.get_extension("principal_components")

    # Prefer the 'templates' extension (computed in extract_waveforms_and_pcs)
    templates = None
    uid_to_idx = None
    try:
        ext_templates = analyzer.get_extension("templates")
        templates = ext_templates.get_data()  # (n_units, n_samples, n_channels)
        # Map unit_id -> row index in templates
        unit_ids_list = list(unit_ids)
        uid_to_idx = {u: i for i, u in enumerate(unit_ids_list)}
    except Exception as e:
        templates = None
        print(f"No 'templates' extension available: {e}")

    # Helper: get spike times in seconds for each unit
    unit_spikes_s: Dict[int, np.ndarray] = {}
    for uid in unit_ids:
        st = sorting.get_unit_spike_train(unit_id=uid)
        unit_spikes_s[uid] = (st / fs).astype(float)

    # Collect ACGs for grid export
    acg_plots: List[Tuple[int, np.ndarray, np.ndarray]] = []

    # Collect per-unit waveforms (best channel) for batch PDFs
    waveform_plots = []  # list[(unit_id, waveform_1d)]
    best_ch_map = {}     # unit_id -> best channel (for titles)

    if templates is not None:
        for uid in unit_ids:
            try:
                T = templates[uid_to_idx[uid], :, :]  
                if T.ndim == 1:
                    T = T[:, None]
                ptp = np.ptp(T, axis=0)
                best_ch = int(np.argmax(ptp))
                wf = T[:, best_ch]
                waveform_plots.append((uid, wf))
                best_ch_map[uid] = best_ch
            except Exception as e:
                print(f"Template not available for unit {uid} from 'templates' ext: {e}")

    # Fallback: use waveforms extension’s template accessor if nothing collected
    if not waveform_plots:
        try:
            ext_waveforms = analyzer.get_extension("waveforms")
            for uid in unit_ids:
                try:
                    T = ext_waveforms.get_template(uid)  # (samples, channels) or None
                    if T is None:
                        continue
                    if T.ndim == 1:
                        T = T[:, None]
                    ptp = np.ptp(T, axis=0)
                    best_ch = int(np.argmax(ptp))
                    wf = T[:, best_ch]
                    waveform_plots.append((uid, wf))
                    best_ch_map[uid] = best_ch
                except Exception:
                    # ignore per-unit failures and continue
                    pass
        except Exception as e:
            print(f"No usable templates from 'waveforms' extension either: {e}")

    if not waveform_plots:
        print("No templates available from 'templates' or 'waveforms' — skipping waveform PDF grids.")
    else:
        # --- Waveform grid PDF exports (two versions) ---
        from matplotlib.backends.backend_pdf import PdfPages
        rows, cols = 5, 5
        per_page = rows * cols

        # 1) GLOBAL y-limits PDF (all panels share ymin/ymax)
        all_wf_values = np.concatenate([wf for uid, wf in waveform_plots])
        min_y = float(np.min(all_wf_values))
        max_y = float(np.max(all_wf_values))
        yticks = np.linspace(min_y, max_y, 3)

        out_global = os.path.join(output_dir, "waveforms_global.pdf")
        with PdfPages(out_global) as pdf:
            for page_start in range(0, len(waveform_plots), per_page):
                chunk = waveform_plots[page_start:page_start + per_page]
                fig, axes = plt.subplots(rows, cols, figsize=(cols * 1.8, rows * 1.2))
                axes = axes.flatten()
                for idx, (uid, wf) in enumerate(chunk):
                    ax = axes[idx]
                    ax.plot(wf)
                    ax.set_ylim(min_y, max_y)
                    # Show best channel if known
                    ch = best_ch_map.get(uid, None)
                    title = f"Unit {uid}" if ch is None else f"Unit {uid} (ch {ch})"
                    ax.set_title(title, fontsize=7)
                    ax.set_xticks([])
                    ax.set_yticks(yticks)
                    if idx % cols == 0:
                        ax.set_ylabel("uV")
                    else:
                        ax.set_yticklabels([])
                    for spine in ax.spines.values():
                        spine.set_linewidth(0.3)
                for k in range(len(chunk), len(axes)):
                    axes[k].axis('off')
                fig.suptitle(f"Units {page_start + 1}-{page_start + len(chunk)} of {len(waveform_plots)}", fontsize=10)
                fig.tight_layout(rect=[0, 0, 1, 0.95])
                pdf.savefig(fig)
                plt.close(fig)

        # 2) PER-UNIT y-limits PDF (each panel auto-scales) — show per-panel y ticks
        out_per = os.path.join(output_dir, "waveforms_per_unit.pdf")
        with PdfPages(out_per) as pdf:
            for page_start in range(0, len(waveform_plots), per_page):
                chunk = waveform_plots[page_start:page_start + per_page]
                fig, axes = plt.subplots(rows, cols, figsize=(cols * 1.8, rows * 1.2))
                axes = axes.flatten()
                for idx, (uid, wf) in enumerate(chunk):
                    ax = axes[idx]
                    ax.plot(wf, linewidth=0.8)

                    # Per-panel limits with headroom (±1 µV or 5% span, whichever is larger)
                    ymin_raw = float(np.min(wf))
                    ymax_raw = float(np.max(wf))
                    span = max(ymax_raw - ymin_raw, 1e-12)
                    pad = max(1.0, 0.05 * span)
                    ylo = ymin_raw - pad
                    yhi = ymax_raw + pad
                    ax.set_ylim(ylo, yhi)

                    # Per-panel ticks: min/mid/max (with labels)
                    mid = (ylo + yhi) / 2.0
                    ax.set_yticks([ylo, mid, yhi])
                    ax.set_yticklabels([f"{ylo:.1f}", f"{mid:.1f}", f"{yhi:.1f}"])
                    ax.tick_params(axis='y', labelsize=6)

                    ch = best_ch_map.get(uid, None)
                    title = f"Unit {uid}" if ch is None else f"Unit {uid} (ch {ch})"
                    ax.set_title(title, fontsize=7)
                    ax.set_xticks([])

                    # Only first column gets the ylabel to avoid clutter
                    if (idx % cols) == 0:
                        ax.set_ylabel("uV", fontsize=7)

                    for spine in ax.spines.values():
                        spine.set_linewidth(0.3)

                for k in range(len(chunk), len(axes)):
                    axes[k].axis('off')
                fig.suptitle(f"Units {page_start + 1}-{page_start + len(chunk)} of {len(waveform_plots)}", fontsize=10)
                fig.tight_layout(rect=[0, 0, 1, 0.95])
                pdf.savefig(fig)
                plt.close(fig)

    # --- ISI histograms (one per unit) ---
    for uid in unit_ids:
        spikes = np.sort(unit_spikes_s[uid])
        isis = np.diff(spikes)
        fig, ax = plt.subplots(figsize=(6, 3))
        spikes_s = np.asarray(unit_spikes_s[uid], dtype=float)
        spikes_s = np.sort(spikes_s[np.isfinite(spikes_s)])
        isis = np.diff(spikes_s) if spikes_s.size > 1 else np.array([], dtype=float)

        if len(isis) > 0:
            finite_isis = isis[np.isfinite(isis)]
            finite_isis = finite_isis[finite_isis >= 0]

            # Fixed-bin histogram prevents slowdowns from bins="auto"
            # and makes ISI plots directly comparable across units.
            isi_hist_max_s = float(getattr(settings, "isi_hist_max_s", 1.0))
            isi_hist_bins = int(getattr(settings, "isi_hist_bins", 100))

            ax.hist(
                finite_isis,
                bins=isi_hist_bins,
                range=(0.0, isi_hist_max_s),
                alpha=0.9,
            )

            n_total = int(finite_isis.size)
            n_gt = int(np.sum(finite_isis > isi_hist_max_s))
            frac_gt = (100.0 * n_gt / n_total) if n_total > 0 else 0.0

            ax.text(
                0.98,
                0.95,
                f"n={n_total}\n>{isi_hist_max_s:g}s={frac_gt:.1f}%",
                transform=ax.transAxes,
                ha="right",
                va="top",
                fontsize=8,
            )

        ax.set_title(f"Unit {uid} — ISI histogram")
        ax.set_xlabel("ISI (s)")
        ax.set_ylabel("Count")
        fig.tight_layout()
        fig.savefig(os.path.join(output_dir, f"unit_{int(uid):03d}_isi_hist.png"), dpi=200)
        plt.close(fig)

        # Collect ACG data if requested
        if settings.include_acg_in_reports:
            lags_ms, counts = _acg_hist(
                spikes,
                max_lag_s=settings.acg_max_lag_s,
                bin_ms=settings.acg_bin_ms
            )
            acg_plots.append((uid, lags_ms, counts))

        # --- Per-unit PCA plotting with robust fallback ---
        X = _get_pca_for_unit(analyzer, uid, max_points=5000)
        if X is not None and X.ndim == 2 and X.shape[0] > 0:
            # 2D PCA
            fig, ax = plt.subplots(figsize=(4.5, 4))
            ax.scatter(X[:, 0], X[:, 1], s=2, alpha=0.5)
            ax.set_title(f"Unit {uid} — PCA 2D")
            ax.set_xlabel("PC1")
            ax.set_ylabel("PC2")
            fig.tight_layout()
            fig.savefig(os.path.join(output_dir, f"unit_{uid:03d}_pca2d.png"), dpi=200)
            plt.close(fig)
        
            # 3D PCA if available
            if X.shape[1] > 2:
                fig = plt.figure(figsize=(5, 4))
                ax = fig.add_subplot(111, projection='3d')
                ax.scatter(X[:, 0], X[:, 1], X[:, 2], s=2, alpha=0.5)
                ax.set_title(f"Unit {uid} — PCA 3D")
                ax.set_xlabel("PC1")
                ax.set_ylabel("PC2")
                ax.set_zlabel("PC3")
                fig.tight_layout()
                fig.savefig(os.path.join(output_dir, f"unit_{uid:03d}_pca3d.png"), dpi=200)
                plt.close(fig)

    # --- Export combined Excel with unit stats (enhanced: amplitudes + best_channel + TTP + half-width) ---
    wf_lookup = {u: w for u, w in waveform_plots}
    stats_rows = []
    
    for uid in unit_ids:
        spikes = unit_spikes_s[uid]
        fr = len(spikes) / max(1e-9, (spikes.max() - spikes.min())) if len(spikes) > 1 else 0.0
        isis = np.diff(np.sort(spikes)) if len(spikes) > 1 else np.array([])
    
        # Pull best-channel waveform (if any) and compute standardized features
        wf = wf_lookup.get(uid, None)
        best_ch = best_ch_map.get(uid, np.nan)
        if wf is None:
            # no template found for this unit
            ttp_ms = np.nan
            ap_halfwidth_ms = np.nan
            template_peak_abs_uV = np.nan
            template_ptp_uV = np.nan
        else:
            feats = _compute_waveform_features(wf, fs_hz=fs, flip_to_negative_first=True)
            ttp_ms = feats["ttp_ms"]
            ap_halfwidth_ms = feats["ap_halfwidth_ms"]
            template_peak_abs_uV = feats["peak_abs_uV"]
            template_ptp_uV = feats["ptp_uV"]
    
        row = dict(
            unit_id=int(uid),
            n_spikes=int(len(spikes)),
            mean_fr_hz=float(fr),
            mean_isi_s=float(np.mean(isis)) if isis.size else np.nan,
            median_isi_s=float(np.median(isis)) if isis.size else np.nan,
            cv_isi=float(np.std(isis) / np.mean(isis)) if isis.size and np.mean(isis) > 0 else np.nan,
    
            # NEW columns:
            best_channel=int(best_ch) if np.isfinite(best_ch) else np.nan,
            template_peak_abs_uV=float(template_peak_abs_uV),
            template_ptp_uV=float(template_ptp_uV),
            ttp_ms=float(ttp_ms),
            ap_halfwidth_ms=float(ap_halfwidth_ms),
    
            # Legacy name kept for compatibility; equal to TTP by design:
            ap_width_ms=float(ttp_ms),
        )
    
        if metrics_df is not None and uid in metrics_df.index:
            md = metrics_df.loc[uid]
            for c in metrics_df.columns:
                row[c] = md.get(c, np.nan)
    
        stats_rows.append(row)
    
    if stats_rows:
        df = pd.DataFrame(stats_rows)
    
        # ---------- Optional: reorder to place the new waveform features right after amplitude_median ----------
        def _first_existing(cols, options):
            """Return the first name from options that exists in cols, else None."""
            for x in options:
                if x in cols:
                    return x
            return None
    
        cols = list(df.columns)
    
        # Choose the anchor column (after which we insert); fallback to end if absent
        anchor = _first_existing(cols, ["amplitude_median"])
        insert_at = cols.index(anchor) + 1 if anchor else len(cols)
    
        # Build the list of new/feature columns in the order preferred
        # (Handles either template_* or legacy names if those are present.)
        to_insert = []
        for name_or_alternates in [
            ("best_channel",),
            ("template_peak_abs_uV", "peak_abs_uV"),
            ("template_ptp_uV", "p2p_uV"),
            ("ttp_ms",),
            ("ap_halfwidth_ms",),
            ("ap_width_ms",),
        ]:
            # pick the first that exists in the DataFrame
            picked = _first_existing(cols, name_or_alternates)
            if picked and picked not in to_insert:
                to_insert.append(picked)
    
        # Remove any of these from current order so we can reinsert as a block
        for c in to_insert:
            if c in cols:
                cols.remove(c)
    
        # Insert the block right after the anchor (or at the end)
        cols[insert_at:insert_at] = to_insert
    
        # Reorder and write
        df = df[cols]
        df.to_excel(os.path.join(output_dir, "unit_stats.xlsx"), index=False)


    # --- ACG grid export ---
    if settings.include_acg_in_reports and acg_plots:
        cols = max(1, int(settings.acg_grid_cols))
        rows = max(1, int(settings.acg_grid_rows))
        per_page = cols * rows
        if settings.acg_use_pdf:
            from matplotlib.backends.backend_pdf import PdfPages
            pdf_path = os.path.join(output_dir, "acg_grids.pdf")
            with PdfPages(pdf_path) as pdf:
                for page_start in range(0, len(acg_plots), per_page):
                    chunk = acg_plots[page_start:page_start + per_page]
                    n = len(chunk)
                    r, c = rows, cols
                    fig, axes = plt.subplots(r, c, figsize=(c * 1.0, r * 0.9))
                    if r == 1 and c == 1:
                        axes = np.array([[axes]])
                    elif r == 1:
                        axes = np.array([axes])
                    elif c == 1:
                        axes = axes.reshape(r, 1)
                    for idx, (uid, lags_ms, counts) in enumerate(chunk):
                        rr, cc = divmod(idx, c)
                        ax = axes[rr, cc]
                        if counts.size:
                            ax.plot(lags_ms, counts, linewidth=0.6)
                            ax.set_xlim(-settings.acg_max_lag_s * 1000, settings.acg_max_lag_s * 1000)
                        ax.set_title(str(uid), fontsize=6)
                        ax.set_xticks([])
                        ax.set_yticks([])
                        for spine in ax.spines.values():
                            spine.set_linewidth(0.3)
                    # hide unused axes
                    for k in range(n, r * c):
                        rr, cc = divmod(k, c)
                        axes[rr, cc].axis('off')
                    fig.suptitle(
                        f"Unit ACGs (±{int(settings.acg_max_lag_s*1000)} ms)  "
                        f"[{page_start + 1}–{page_start + n} of {len(acg_plots)}]",
                        fontsize=10)
                    fig.tight_layout(rect=[0, 0, 1, 0.95])
                    pdf.savefig(fig)
                    plt.close(fig)
        else:
            for page_start in range(0, len(acg_plots), per_page):
                chunk = acg_plots[page_start:page_start + per_page]
                n = len(chunk)
                r, c = rows, cols
                fig, axes = plt.subplots(r, c, figsize=(c * 1.2, r * 1.0), dpi=settings.acg_png_dpi)
                if r == 1 and c == 1:
                    axes = np.array([[axes]])
                elif r == 1:
                    axes = np.array([axes])
                elif c == 1:
                    axes = axes.reshape(r, 1)
                for idx, (uid, lags_ms, counts) in enumerate(chunk):
                    rr, cc = divmod(idx, c)
                    ax = axes[rr, cc]
                    if counts.size:
                        ax.plot(lags_ms, counts, linewidth=0.6)
                        ax.set_xlim(-settings.acg_max_lag_s * 1000, settings.acg_max_lag_s * 1000)
                    ax.set_title(str(uid), fontsize=6)
                    ax.set_xticks([])
                    ax.set_yticks([])
                    for spine in ax.spines.values():
                        spine.set_linewidth(0.3)
                for k in range(n, r * c):
                    rr, cc = divmod(k, c)
                    axes[rr, cc].axis('off')
                fig.suptitle(
                    f"Unit ACGs (±{int(settings.acg_max_lag_s*1000)} ms)  "
                    f"[{page_start + 1}–{page_start + n} of {len(acg_plots)}]",
                    fontsize=10)
                fig.tight_layout(rect=[0, 0, 1, 0.95])
                out_path = os.path.join(output_dir, f"acg_grid_{page_start // per_page + 1:03d}.png")
                fig.savefig(out_path, dpi=settings.acg_png_dpi)
                plt.close(fig)

    # --- Joint PCA plot for all units (robust) ---
    try:
        unit_ids_list = list(unit_ids)
        all_X = []
        all_labels = []
        for uid in unit_ids_list:
            X = _get_pca_for_unit(analyzer, uid, max_points=1000)
            if X is not None and X.ndim == 2 and X.size:
                all_X.append(X)
                all_labels.extend([uid] * X.shape[0])
        if all_X:
            all_X = np.vstack(all_X)
            all_labels = np.array(all_labels)
            import matplotlib.cm as cm
            fig, ax = plt.subplots(figsize=(7, 6))
            unique_units = np.unique(all_labels)
            colors = cm.rainbow(np.linspace(0, 1, len(unique_units)))
            for i, uid in enumerate(unique_units):
                idx = all_labels == uid
                ax.scatter(all_X[idx, 0], all_X[idx, 1], s=2, color=colors[i], label=str(uid), alpha=0.5)
            ax.set_xlabel("PC1"); ax.set_ylabel("PC2")
            ax.set_title("All units: PCA feature scatter")
            ax.legend(markerscale=5, loc='upper right', fontsize=7, bbox_to_anchor=(1.15, 1))
            fig.tight_layout()
            fig.savefig(os.path.join(output_dir, "all_units_pca_scatter.png"), dpi=200)
            plt.close(fig)
        else:
            print("No PCA projections available for joint plot.")
    except Exception as e:
        print(f"Failed to make cluster PCA plot: {e}")
        
import glob

def find_si_extension_file(folder, extension, candidates):
    """
    Searches for any of the candidate files in the modern SI extension subfolders.
    Args:
        folder: The main output directory (e.g., 'waveforms_2025-08-17_23-50-27')
        extension: The SI extension (e.g., 'templates', 'waveforms', 'principal_components', etc.)
        candidates: List of possible file names (e.g., ['average.npy', 'mean.npy'])
    Returns:
        The full path to the found file, or None.
    """
    ext_dir = os.path.join(folder, "extensions", extension)
    for cand in candidates:
        fpath = os.path.join(ext_dir, cand)
        if os.path.exists(fpath):
            return fpath
    # Optionally, do a glob search for .npy/.csv fallback
    pattern = os.path.join(ext_dir, "*.npy")
    for fpath in glob.glob(pattern):
        if os.path.basename(fpath) in candidates:
            return fpath
    return None
# ----------------------------
# GUI
# ----------------------------

class MEAGUI(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("MEA/Nlx Spike Sorting & Curation (SpikeInterface)")
        self.geometry("1120x780")
        self.settings = MEASettings()
        self.recording = None
        self.raw_recording = None        # keep a raw handle for KS4 routing
        self.preproc_recording = None
        self.qc_df: Optional[pd.DataFrame] = None
        self.include_channel_ids: List[int] = []
        self.sorting = None
        self.waveforms = None
        self.metrics_df: Optional[pd.DataFrame] = None
        self.unit_spikes_s: Dict[int, np.ndarray] = {}
        self.seed = self.settings.random_seed
        np.random.seed(self.seed)

        self._build_ui()

    # --- UI construction
    def _build_ui(self):
        nb = ttk.Notebook(self)
        self.tab_load = ttk.Frame(nb)
        self.tab_sort = ttk.Frame(nb)
        nb.add(self.tab_load, text="1) Load & QC")
        nb.add(self.tab_sort, text="2) Sorting & Metrics")
        nb.pack(fill='both', expand=True)

        self._build_tab_load()
        self._build_tab_sort()

        # Status box
        self.status = tk.Text(self, height=6, wrap=tk.WORD)
        self.status.pack(fill='x')

    def _log_si_probe(self, rec, label):
        import io, contextlib
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            print_si_slicing_diagnostics(rec)
        self.log(f"[SI slicing probe: {label}]\n{buf.getvalue().rstrip()}")

    def browse_geom_csv(self):
        path = filedialog.askopenfilename(title="Select MEA geometry CSV", filetypes=[("CSV files", "*.csv"), ("All files", "*.*")])
        if path:
            self.var_geom_csv.set(path)
    
    def _set_busy(self, busy: bool):
        self.config(cursor="watch" if busy else "")
        def _toggle(widget):
            # Don't disable the status Text so logging keeps working
            if widget is getattr(self, "status", None):
                return
            try:
                widget.configure(state="disabled" if busy else "normal")
            except tk.TclError:
                pass
            for ch in getattr(widget, "winfo_children", lambda: [])():
                _toggle(ch)
        _toggle(self)
        self.update_idletasks()
    
    def _run_bg(self, fn, *args, **kwargs):
        def _wrap():
            try:
                fn(*args, **kwargs)
            finally:
                self.after(0, lambda: self._set_busy(False))
        self._set_busy(True)
        threading.Thread(target=_wrap, daemon=True).start()
    
    # make logging thread-safe (always marshal to Tk thread)
    def log(self, msg: str):
        def _append():
            ts = time.strftime('%H:%M:%S')
            self.status.insert(tk.END, f"[{ts}] {msg}\n")
            self.status.see(tk.END)
        self.after(0, _append)

    def pick_outdir(self):
        d = filedialog.askdirectory(title="Select output folder")
        if d:
            self.var_out.set(d)
            self.settings.outdir = d  # keep GUI and backend in sync!
            temp_dir = os.path.join(d, "temp")
            set_tempdir_for_session(temp_dir)
            self.log(f"Temp dir set to: {temp_dir}")
            print("Temp dir set to:", temp_dir)

    def open_outdir(self):
        out = self.var_out.get()
        if not out:
            return
        try:
            if sys.platform.startswith('darwin'):
                os.system(f'open "{out}"')
            elif os.name == 'nt':
                os.startfile(out)
            else:
                os.system(f'xdg-open "{out}"')
        except Exception:
            pass

    # --- Tab 1: Load & QC
    def _build_tab_load(self):
        f = self.tab_load
        r = 0
        ttk.Label(f, text="Input .h5 file:").grid(row=r, column=0, sticky='e')
        self.var_h5 = tk.StringVar()
        ttk.Entry(f, textvariable=self.var_h5, width=70).grid(row=r, column=1, sticky='w')
        ttk.Button(f, text="Browse", command=self.browse_h5).grid(row=r, column=2, padx=5)
        r += 1
        # --- NEW: Geometry CSV field ---
        ttk.Label(f, text="Geometry CSV:").grid(row=r, column=0, sticky='e')
        self.var_geom_csv = tk.StringVar()
        ttk.Entry(f, textvariable=self.var_geom_csv, width=70).grid(row=r, column=1, sticky='w')
        ttk.Button(f, text="Browse", command=self.browse_geom_csv).grid(row=r, column=2, padx=5)
        r += 1
        ttk.Label(f, text="Output folder:").grid(row=r, column=0, sticky='e')
        self.var_out = tk.StringVar(value=self.settings.outdir)
        ttk.Entry(f, textvariable=self.var_out, width=70).grid(row=r, column=1, sticky='w')
        ttk.Button(f, text="Set", command=self.pick_outdir).grid(row=r, column=2, padx=5)
        ttk.Button(f, text="Open", command=self.open_outdir).grid(row=r, column=3, padx=5)
        r += 1
        # QC params
        qc_frame = ttk.LabelFrame(f, text="QC (RMS)")
        qc_frame.grid(row=r, column=0, columnspan=4, sticky='we', pady=6)
        
        # NEW: start / end / quiet length
        default_start = getattr(self.settings, "baseline_t0_s", 0.0)
        default_end   = default_start + getattr(self.settings, "baseline_len_s", 15.0)
        self.var_qc_start = tk.DoubleVar(value=default_start)
        self.var_qc_end   = tk.DoubleVar(value=default_end)
        self.var_qc_win   = tk.DoubleVar(value=1.0)
        
        ttk.Label(qc_frame, text="Baseline start (s)").grid(row=0, column=0, sticky='e')
        ttk.Entry(qc_frame, textvariable=self.var_qc_start, width=8).grid(row=0, column=1, sticky='w')
        
        ttk.Label(qc_frame, text="Baseline end (s)").grid(row=0, column=2, sticky='e')
        ttk.Entry(qc_frame, textvariable=self.var_qc_end, width=8).grid(row=0, column=3, sticky='w')
        
        ttk.Label(qc_frame, text="Quiet length (s)").grid(row=0, column=4, sticky='e')
        ttk.Entry(qc_frame, textvariable=self.var_qc_win, width=8).grid(row=0, column=5, sticky='w')
        
        ttk.Label(qc_frame, text="High-pass (Hz)").grid(row=0, column=6, sticky='e')
        self.var_hpmin = tk.DoubleVar(value=self.settings.hp_min_hz)
        ttk.Entry(qc_frame, textvariable=self.var_hpmin, width=8).grid(row=0, column=7, sticky='w')
        
        ttk.Label(qc_frame, text="Low-pass (Hz)").grid(row=0, column=8, sticky='e')
        self.var_hpmax = tk.DoubleVar(value=self.settings.hp_max_hz)
        ttk.Entry(qc_frame, textvariable=self.var_hpmax, width=8).grid(row=0, column=9, sticky='w')
        
        self.var_use_cmr = tk.BooleanVar(value=self.settings.use_cmr)
        ttk.Checkbutton(qc_frame, text="Common median reference (CMR)", variable=self.var_use_cmr).grid(row=0, column=10, padx=10)

        # --- NEW: Notch filter 60Hz checkbox (default True) ---
        self.var_use_notch = tk.BooleanVar(value=self.settings.use_notch_60hz if hasattr(self.settings, "use_notch_60hz") else True)
        ttk.Checkbutton(qc_frame, text="Apply 60 Hz Notch Filter", variable=self.var_use_notch).grid(row=0, column=12, padx=10)
        
        ttk.Button(qc_frame, text="Compute QC", command=self.run_qc).grid(row=0, column=11, padx=10)

        r += 1
        # QC table
        self.qc_tree = ttk.Treeview(f, columns=("ch", "rms", "thr", "include", "q_start", "q_end"),
                                    show='headings', height=18)
        for c, w in zip(["ch", "rms", "thr", "include", "q_start", "q_end"],
                        [120, 120, 120, 100, 120, 120]):
            self.qc_tree.heading(c, text=c)
            self.qc_tree.column(c, width=w, anchor='center')
        self.qc_tree.grid(row=r, column=0, columnspan=4, sticky='nsew', pady=4)
        self.qc_tree.bind("<Double-1>", self._on_qc_toggle)  # NEW
        f.grid_rowconfigure(r, weight=1)
        f.grid_columnconfigure(1, weight=1)
        r += 1
        btns = ttk.Frame(f)
        btns.grid(row=r, column=0, columnspan=4, sticky='w')
        ttk.Button(btns, text="Select all", command=lambda: self._qc_set_all(True)).pack(side='left', padx=4)
        ttk.Button(btns, text="Deselect all", command=lambda: self._qc_set_all(False)).pack(side='left', padx=4)
        ttk.Button(btns, text="Export QC to Excel", command=self.export_qc_excel).pack(side='left', padx=8)

    def _on_qc_toggle(self, event):
        iid = self.qc_tree.focus()
        if not iid:
            return
        vals = list(self.qc_tree.item(iid, 'values'))
        vals[3] = 'False' if vals[3] == 'True' else 'True'
        self.qc_tree.item(iid, values=vals)
        raw = str(vals[0])            # e.g., "23 (noisy)" or "23"
        ch_str = raw.split()[0]       # "23"
        mask = self.qc_df['channel_id'].astype(str) == ch_str
        self.qc_df.loc[mask, 'include'] = (vals[3] == 'True')

    def browse_h5(self):
        path = filedialog.askopenfilename(
            title="Select MCS HDF5 (.h5)", 
            filetypes=[("HDF5", "*.h5"), ("All", "*.*")]
        )
        if path:
            self.var_h5.set(path)
    # (removed duplicate pick_outdir/open_outdir — the earlier versions already sync self.settings.outdir)

    def _qc_set_all(self, val: bool):
        if self.qc_df is None:
            return
        self.qc_df['include'] = val
        for iid in self.qc_tree.get_children():
            vals = list(self.qc_tree.item(iid, 'values'))
            vals[3] = str(val)
            self.qc_tree.item(iid, values=vals)

    def run_qc(self):
        try:
            ensure_spikeinterface_available()
            path = self.var_h5.get()
            if not path:
                messagebox.showwarning("Missing", "Please select an input .h5 file")
                return
    
            # IO setup
            self.settings.input_h5 = path
            self.settings.outdir = self.var_out.get() or os.getcwd()
            os.makedirs(self.settings.outdir, exist_ok=True)
    
            # Load recording
            self.log("Loading recording…")
            rec = load_mcs_h5(path)
            self.recording = rec
            self.log(f"Loaded: {rec}")
    
            # --- Assign geometry to RAW recording BEFORE filtering ---
            geom_csv = self.var_geom_csv.get().strip()
            if geom_csv:
                try:
                    geom_df = pd.read_csv(geom_csv)
    
                    # Validate columns
                    cols = {c.lower() for c in geom_df.columns}
                    if not (("hwid" in cols or "channel" in cols) and "x_um" in cols and "y_um" in cols):
                        raise ValueError("Geometry CSV must contain either 'hwid' (1-based) or 'channel' (0/1-based), and 'x_um','y_um'.")
    
                    # Normalize column names
                    rename = {c: c.lower() for c in geom_df.columns}
                    geom_df = geom_df.rename(columns=rename)
    
                    # Build zero-based channel index and "Ch{n}" strings we will try to match against
                    if "hwid" in geom_df.columns:
                        geom_df["hwid_zero"] = geom_df["hwid"].astype(int) - 1   # MCS: hwid is 1-based
                    elif "channel" in geom_df.columns:
                        geom_df["hwid_zero"] = geom_df["channel"].astype(int)
                    geom_df["hwid_str"] = geom_df["hwid_zero"].apply(lambda n: f"Ch{int(n)}")
    
                    # Two lookup maps: "Ch{n}" and "{n}"
                    geom_map_str = dict(zip(geom_df["hwid_str"], zip(geom_df["x_um"], geom_df["y_um"])))
                    geom_map_num = dict(zip(geom_df["hwid_zero"].astype(str), zip(geom_df["x_um"], geom_df["y_um"])))
    
                    # Apply to actual recording channel order
                    channel_ids = self.recording.get_channel_ids()
                    locs, matched, key_used = [], [], []
                    hits = 0
                    for ch in channel_ids:
                        key = str(ch)  # could be "Ch37" or "37"
                        if key in geom_map_str:
                            xy = geom_map_str[key]; hits += 1; matched.append(True); key_used.append(key)
                        elif key in geom_map_num:
                            xy = geom_map_num[key]; hits += 1; matched.append(True); key_used.append(key)
                        else:
                            # Fallback cross-try (swap "Ch" prefix)
                            if isinstance(ch, str) and ch.startswith("Ch"):
                                alt = ch[2:]
                                xy = geom_map_num.get(alt, (np.nan, np.nan))
                                if alt in geom_map_num:
                                    hits += 1; matched.append(True); key_used.append(f"(alt){alt}")
                                else:
                                    matched.append(False); key_used.append("")
                            else:
                                alt = f"Ch{ch}"
                                xy = geom_map_str.get(alt, (np.nan, np.nan))
                                if alt in geom_map_str:
                                    hits += 1; matched.append(True); key_used.append(f"(alt){alt}")
                                else:
                                    matched.append(False); key_used.append("")
                        locs.append(xy)
    
                    locations = np.asarray(locs, dtype=np.float32)

                    # Attach a real Probe (keeps geometry through the pipeline)
                    rec2 = attach_probe_to_recording(
                        self.recording,
                        locations_um=locations,
                        channel_ids=self.recording.get_channel_ids(),
                        electrode_diameter_um=30.0,   # your electrodes diameter
                        ied_um=200.0                  # your inter-electrode distance
                    )
                    
                    # NEW: probe sanity check (logs to the GUI). This helps catch
                    # "Probe must have device_channel_indices" early and visibly.
                    try:
                        prb = rec2.get_probe()
                        dci = getattr(prb, "device_channel_indices", None)
                        self.log(
                            f"Probe attached: class={type(prb).__name__}; "
                            f"device_channel_indices={'OK' if dci is not None else 'MISSING'}"
                        )
                    except Exception as e:
                        self.log(f"Probe check failed: {e}")
                    
                    # Continue with the probe-attached recording
                    self.recording = rec2
                    # NEW: keep an explicit raw handle for sorter routing
                    self.raw_recording = self.recording
                    
                    # Persist a check file so we can eyeball what happened
                    check_df = pd.DataFrame({
                        "channel_id": channel_ids,
                        "matched": matched,
                        "key_used": key_used,
                        "x_um": locations[:, 0],
                        "y_um": locations[:, 1],
                    })
                    out_dir = self.var_out.get() or os.getcwd()
                    check_path = os.path.join(out_dir, "geometry_assignment_check.csv")
                    check_df.to_csv(check_path, index=False)
                    
                    self.log(f"Geometry assigned to {hits}/{len(channel_ids)} channels from CSV. Check: {check_path}")

                    if hits < len(channel_ids):
                        messagebox.showwarning(
                            "Geometry warning",
                            f"Geometry matched {hits}/{len(channel_ids)} channels.\n"
                            f"See geometry_assignment_check.csv in the output folder."
                        )
                except Exception as e:
                    self.log(f"Geometry assignment error: {e}")
                    messagebox.showwarning("Geometry error", f"Could not assign geometry: {e}")
    
            # Preprocess **after** geometry is set (only once)
            self.settings.hp_min_hz = float(self.var_hpmin.get())
            self.settings.hp_max_hz = float(self.var_hpmax.get())
            self.settings.use_cmr = bool(self.var_use_cmr.get())
            self.settings.use_notch_60hz = bool(self.var_use_notch.get())   # <--- NEW LINE
            
            # IMPORTANT: preprocess the probe-attached recording, then ensure probe persists
            self.preproc_recording = preprocess_recording(self.recording, self.settings)
            self.preproc_recording = ensure_probe_on(self.preproc_recording, self.recording)
            # One-shot diagnostics (helpful for version/compat visibility)
            self._log_si_probe(self.raw_recording, "RAW")
            self._log_si_probe(self.preproc_recording, "PREPROC")

            # QC with quietest window within [start, end]
            scan_start = float(self.var_qc_start.get())
            scan_end   = float(self.var_qc_end.get())
            win_s      = float(self.var_qc_win.get())
            
            # basic validation / auto-fix
            if scan_end <= scan_start:
                self.log("Baseline end must be > start. Adjusting end = start + 1.0 s.")
                scan_end = scan_start + 1.0
            if win_s <= 0:
                self.log("Quiet length must be > 0. Setting to 1.0 s.")
                win_s = 1.0
            if (scan_end - scan_start) < win_s:
                self.log("Scan window shorter than quiet length. Expanding end to fit.")
                scan_end = scan_start + win_s
            
            self.log(f"Computing RMS per channel (quietest {win_s:.3g}s in [{scan_start:.3g}, {scan_end:.3g}]s)…")
            self.qc_df = compute_channel_rms_quietest(
                self.preproc_recording,
                scan_start_s=scan_start,
                scan_end_s=scan_end,
                window_s=win_s
            )

            # Optional: visual inspection of outliers
            noisy_chans = self.qc_df[self.qc_df['is_noisy']]['channel_id'].tolist()
            if noisy_chans:
                fs = float(self.preproc_recording.get_sampling_frequency())
                total_frames = self.preproc_recording.get_num_frames()
                n_samples = int(min(fs * 60, total_frames))
                # Try to get µV for the viewer; fall back silently
                try:
                    traces = self.preproc_recording.get_traces(start_frame=0, end_frame=n_samples, return_in_uV=True)
                except TypeError:
                    try:
                        traces = self.preproc_recording.get_traces(start_frame=0, end_frame=n_samples, return_scaled=True)
                    except TypeError:
                        traces = self.preproc_recording.get_traces(start_frame=0, end_frame=n_samples)
                ch_ids = self.qc_df['channel_id'].tolist()
                # traces shape is (frames, channels)
                traces_dict = {ch: traces[:, ch_ids.index(ch)] for ch in noisy_chans}
                dlg = NoisyChannelInspector(self, traces_dict, fs, seconds=60)
                self.wait_window(dlg)
                selected = getattr(dlg, 'selected', {})
                for ch, keep in selected.items():
                    self.qc_df.loc[self.qc_df['channel_id'] == ch, 'include'] = keep
    
            # Populate table
            for iid in self.qc_tree.get_children():
                self.qc_tree.delete(iid)
            for _, row in self.qc_df.iterrows():
                ch_disp = f"{row['channel_id']}"
                if row.get('is_noisy', False):
                    ch_disp += " (noisy)"
                self.qc_tree.insert('', tk.END,
                    values=(ch_disp,
                            f"{row['rms']:.4f}",
                            f"{row['threshold_3x']:.4f}",
                            str(row['include']),
                            f"{row.get('quiet_start_s', float('nan')):.3f}",
                            f"{row.get('quiet_end_s', float('nan')):.3f}"))
            self.include_channel_ids = list(self.qc_df[self.qc_df['include']].channel_id.values)
            self.log(f"QC done. Channels: {len(self.qc_df)}; Included: {len(self.include_channel_ids)}")
    
        except Exception as e:
            self.log(f"QC error: {e}")
            traceback.print_exc()
            messagebox.showerror("QC error", str(e))

    def export_qc_excel(self):
        if self.qc_df is None:
            messagebox.showwarning("No QC", "Run QC first")
            return
        out = os.path.join(self.var_out.get() or os.getcwd(), "qc_channels.xlsx")
        self.qc_df.to_excel(out, index=False)
        self.log(f"QC saved: {out}")

    # --- Tab 2: Sorting & Metrics
    def _build_tab_sort(self):
        f = self.tab_sort
        r = 0
        # Sorting params
        # --- Sorting frame (supports MountainSort4 and Kilosort4) ---
        sort_frame = ttk.LabelFrame(f, text="Sorting")
        sort_frame.grid(row=r, column=0, columnspan=4, sticky='we', pady=6)
        
        # sorter selector
        ttk.Label(sort_frame, text="Sorter").grid(row=0, column=0, sticky='e')
        self.var_sorter = tk.StringVar(value=self.settings.sorter_name)  # uses MEASettings.sorter_name
        cmb = ttk.Combobox(
            sort_frame,
            textvariable=self.var_sorter,
            state="readonly",
            values=("mountainsort4", "kilosort4", "spykingcircus2"),
            width=15
        )
        cmb.grid(row=0, column=1, sticky='w', padx=4)
        
        # MS4-only params (left in place; we'll enable/disable with _on_sorter_changed)
        ttk.Label(sort_frame, text="detect_threshold").grid(row=0, column=2, sticky='e')
        self.var_det_thr = tk.DoubleVar(value=self.settings.detect_threshold)
        ent_thr = ttk.Entry(sort_frame, textvariable=self.var_det_thr, width=6)
        ent_thr.grid(row=0, column=3, sticky='w')
        
        ttk.Label(sort_frame, text="detect_sign (-1 neg, 0 both, +1 pos)").grid(row=0, column=4, sticky='e')
        self.var_det_sign = tk.IntVar(value=self.settings.detect_sign)
        ent_sign = ttk.Entry(sort_frame, textvariable=self.var_det_sign, width=6)
        ent_sign.grid(row=0, column=5, sticky='w')
        
        ttk.Label(sort_frame, text="adjacency_radius (-1 global)").grid(row=0, column=6, sticky='e')
        self.var_adj = tk.DoubleVar(value=self.settings.adjacency_radius)
        ent_adj = ttk.Entry(sort_frame, textvariable=self.var_adj, width=6)
        ent_adj.grid(row=0, column=7, sticky='w')
        
        ttk.Label(sort_frame, text="clip_size (samples)").grid(row=0, column=8, sticky='e')
        self.var_clip = tk.IntVar(value=self.settings.clip_size)
        ent_clip = ttk.Entry(sort_frame, textvariable=self.var_clip, width=6)
        ent_clip.grid(row=0, column=9, sticky='w')
        
        # actions
        ttk.Button(sort_frame, text="Run sorting",
                   command=lambda: self._run_bg(self.run_sorting)).grid(row=0, column=10, padx=10)
        
        ttk.Button(sort_frame, text="Check KS4 GPU & deps",
                   command=lambda: self._run_bg(self._check_ks4_readiness)).grid(row=0, column=11, padx=6)
        
        # keep references so we can enable/disable when switching sorter
        self._ms4_param_widgets = [ent_thr, ent_sign, ent_adj, ent_clip]
        def _on_sorter_changed(evt=None):
            use_ms4 = (self.var_sorter.get().lower() == "mountainsort4")
            state = "normal" if use_ms4 else "disabled"
            for w in self._ms4_param_widgets:
                w.configure(state=state)
        cmb.bind("<<ComboboxSelected>>", _on_sorter_changed)
        _on_sorter_changed()

        r += 1
        met_frame = ttk.LabelFrame(f, text="Quality metrics & Reports")
        met_frame.grid(row=r, column=0, columnspan=4, sticky='we', pady=6)
        ttk.Button(met_frame, text="Compute metrics", command=lambda: self._run_bg(self.compute_metrics)).grid(row=0, column=0, padx=5)
        ttk.Button(met_frame, text="Compare MS4 vs KS4", command=lambda: self._run_bg(self.compare_ms4_vs_ks4)).grid(row=0, column=5, padx=8)
        ttk.Button(met_frame, text="Compare MS4 vs SC2", command=lambda: self._run_bg(self.compare_ms4_vs_sc2)).grid(row=0, column=6, padx=8)
        ttk.Button(met_frame, text="Compare KS4 vs SC2", command=lambda: self._run_bg(self.compare_ks4_vs_sc2)).grid(row=0, column=7, padx=8)
        ttk.Button(met_frame, text="Auto-curate (preview)", command=self.preview_curation).grid(row=0, column=1, padx=5)
        ttk.Button(met_frame, text="Export unit spike times", command=self.export_unit_spikes_xlsx).grid(row=0, column=2, padx=5)
        ttk.Button(met_frame, text="Build unit reports", command=lambda: self._run_bg(self.build_unit_reports)).grid(row=0, column=3, padx=5)
        
        # NEW: ACG toggles
        self.var_include_acg = tk.BooleanVar(value=self.settings.include_acg_in_reports)
        ttk.Checkbutton(met_frame,
                        text="Include ACGs (grid)",
                        variable=self.var_include_acg).grid(row=0, column=4, padx=8)
    
        self.var_acg_pdf = tk.BooleanVar(value=self.settings.acg_use_pdf)
        ttk.Checkbutton(met_frame,
                        text="Save ACGs as multi-page PDF",
                        variable=self.var_acg_pdf).grid(row=0, column=5, padx=8)
    
        # NEW: PC-metrics toggle
        self.var_pc_metrics = tk.BooleanVar(value=self.settings.compute_pc_metrics)
        ttk.Checkbutton(met_frame,
                        text="Include cluster-separation metrics (PC-based)",
                        variable=self.var_pc_metrics).grid(row=0, column=6, padx=8)

    def _build_sorter_inputs(self):
        """
        Returns (rec_for_sort, rec_for_waveforms) based on the selected sorter
        and the QC-selected channel subset. Waveforms/metrics always use the
        filtered QC view for consistent visuals; KS4 gets the raw subset so it
        can apply its own filtering/whitening.
        """
        import spikeinterface as si
    
        # You already maintain these:
        # - self.raw_recording: the raw, probe-attached Recording (no bandpass/notch)
        # - self.preproc_recording: QC/filtered Recording (bandpass/notch/CMR applied)
        # - self.include_channel_ids: list of channels kept after RMS-based QC (or None for all)
        if getattr(self, "raw_recording", None) is None or getattr(self, "preproc_recording", None) is None:
            raise RuntimeError("Missing raw or preprocessed recording. Load data and run QC first.")
    
        # Apply the same channel selection to both raw and QC recordings
        if self.include_channel_ids:
            rec_qc  = subset_recording_channels(self.preproc_recording, self.include_channel_ids)
            rec_raw = subset_recording_channels(self.raw_recording,      self.include_channel_ids)
        else:
            rec_qc  = self.preproc_recording
            rec_raw = self.raw_recording

        sorter = str(self.var_sorter.get()).lower()
    
        if sorter == "kilosort4":
            # KS4: let it do its own high-pass + whitening on the RAW subset
            rec_for_sort = rec_raw
            # For visuals/metrics, keep the filtered QC subset for parity across sorters
            rec_for_wave = rec_qc
        else:
            # MS4 / SC2: we already filtered upstream; give them the filtered subset
            rec_for_sort = rec_qc
            rec_for_wave = rec_qc
    
        # Preserve probe metadata (no-op if already present)
        rec_for_sort = ensure_probe_on(rec_for_sort, self.preproc_recording)
        rec_for_wave = ensure_probe_on(rec_for_wave, self.preproc_recording)
    
        return rec_for_sort, rec_for_wave

    def run_sorting(self):
        try:
            if self.preproc_recording is None or self.raw_recording is None:
                messagebox.showwarning("Missing", "Load data and run QC to prepare recordings first")
                return
    
            # Update included channels from the QC table (user may have toggled)
            if self.qc_df is not None and "include" in self.qc_df.columns:
                self.include_channel_ids = list(self.qc_df.loc[self.qc_df["include"], "channel_id"].values)
    
            # Build sorter-specific inputs (raw vs filtered) while keeping the same channel subset
            rec_for_sort, rec_for_wave = self._build_sorter_inputs()
    
            # Sorter selection + sorter-specific params
            self.settings.sorter_name = str(self.var_sorter.get()).lower()
    
            if self.settings.sorter_name == "mountainsort4":
                self.settings.detect_threshold = float(self.var_det_thr.get())
                self.settings.detect_sign = int(self.var_det_sign.get())
                self.settings.adjacency_radius = float(self.var_adj.get())
                self.settings.clip_size = int(self.var_clip.get())
    
            # Output dir depends on sorter
            tag_map = {"mountainsort4": "ms4", "kilosort4": "ks4", "spykingcircus2": "sc2"}
            sorter_tag = tag_map.get(self.settings.sorter_name, "sort")
            out_sort = os.path.join(self.var_out.get() or os.getcwd(), f"si_sorting_{sorter_tag}")
            os.makedirs(out_sort, exist_ok=True)
            if not os.access(out_sort, os.W_OK):
                self.log(f"WARNING: Cannot write to {out_sort}")
    
            # Run selected sorter on rec_for_sort
            if self.settings.sorter_name == "mountainsort4":
                self.log("Running MountainSort4…")
                self.sorting = run_mountainsort4(rec_for_sort, self.settings, out_sort)
            elif self.settings.sorter_name == "kilosort4":
                self.log("Running Kilosort4 (internal HPF + whitening)…")
                self.sorting = run_kilosort4(rec_for_sort, self.settings, out_sort)
            elif self.settings.sorter_name == "spykingcircus2":
                self.log("Running SpykingCircus2…")
                self.sorting = run_spykingcircus2(rec_for_sort, self.settings, out_sort)
            else:
                raise ValueError(f"Unknown sorter: {self.settings.sorter_name}")
    
            uids = self.sorting.get_unit_ids()
            self.log(f"Sorting done. Units found: {len(uids)}")
    
            # Extract waveforms + PCA from the filtered QC view for consistent visuals
            self.log("Extracting waveforms & PCA…")
            timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            wf_dir = os.path.join(self.var_out.get() or os.getcwd(), f"waveforms_{timestamp}")
            self.waveforms = extract_waveforms_and_pcs(rec_for_wave, self.sorting, self.settings, wf_dir)
            self.log("Waveforms/PCA ready.")
    
        except Exception as e:
            self.log(f"Sorting error: {e}")
            traceback.print_exc()
            messagebox.showerror("Sorting error", str(e))

    def _check_ks4_readiness(self):
        """
        Logs whether Kilosort4 is importable, SpikeInterface exposes the ks4 wrapper,
        and whether a CUDA GPU is visible.
        """
        try:
            import importlib, spikeinterface.sorters as ss
            try:
                import kilosort
                ks4_ver = getattr(kilosort, "__version__", "unknown")
            except Exception as e:
                self.log(f"KS4 import failed: {e}")
                return
    
            try:
                import torch
                has_cuda = torch.cuda.is_available()
                gpu_name = torch.cuda.get_device_name(0) if has_cuda else "None"
            except Exception:
                has_cuda, gpu_name = False, "Unknown"
    
            has_wrapper = ("kilosort4" in ss.installed_sorters()) if hasattr(ss, "installed_sorters") else True
            self.log(f"Kilosort4 version: {ks4_ver}")
            self.log(f"SpikeInterface has ks4 wrapper: {has_wrapper}")
            self.log(f"CUDA available: {has_cuda}; GPU: {gpu_name}")
    
            # probe default params (will raise if wrapper missing)
            try:
                _ = ss.get_default_sorter_params("kilosort4")
                self.log("KS4 default params retrieved OK")
            except Exception as e:
                self.log(f"KS4 params unavailable via SpikeInterface: {e}")
    
        except Exception as e:
            self.log(f"KS4 readiness check error: {e}")
    
    def _compare_sorters_generic(self, name_a: str, name_b: str):
        """
        Run two sorters head-to-head on the current preprocessed+subset recording,
        save matches CSVs and a quick agreement heatmap.
        """
        import spikeinterface.comparison as sicomp
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import numpy as np
        import pandas as pd
        import os
    
        if self.preproc_recording is None:
            messagebox.showwarning("Missing", "Run QC to prepare preprocessed recording")
            return
    
        # Build both RAW and QC subsets once (respect channel include mask)
        import spikeinterface as si
        
        if self.raw_recording is None or self.preproc_recording is None:
            messagebox.showwarning("Missing", "Run QC first")
            return
        
        if self.include_channel_ids:
            rec_qc  = subset_recording_channels(self.preproc_recording, self.include_channel_ids)
            rec_raw = subset_recording_channels(self.raw_recording,      self.include_channel_ids)
        else:
            rec_qc  = self.preproc_recording
            rec_raw = self.raw_recording
        
        # Preserve probe on both
        rec_qc  = ensure_probe_on(rec_qc,  self.preproc_recording)
        rec_raw = ensure_probe_on(rec_raw, self.preproc_recording)
        
        base_out = (self.var_out.get() or os.getcwd())
        tag_map = {"mountainsort4": "ms4", "kilosort4": "ks4", "spykingcircus2": "sc2"}
        out_a = os.path.join(base_out, f"si_sorting_{tag_map[name_a]}")
        out_b = os.path.join(base_out, f"si_sorting_{tag_map[name_b]}")
        
        def _pick_rec(name):
            # KS4 expects RAW (it will HPF/whiten itself); MS4/SC2 use your filtered QC stream
            return rec_raw if name == "kilosort4" else rec_qc
        
        # Run both sorters on their appropriate inputs
        self.log(f"Comparing {name_a.upper()} vs {name_b.upper()}: running {name_a}…")
        if name_a == "mountainsort4":
            sort_a = run_mountainsort4(_pick_rec(name_a), self.settings, out_a)
        elif name_a == "kilosort4":
            sort_a = run_kilosort4(_pick_rec(name_a), self.settings, out_a)
        elif name_a == "spykingcircus2":
            sort_a = run_spykingcircus2(_pick_rec(name_a), self.settings, out_a)
        else:
            raise ValueError(f"Unknown sorter A: {name_a}")
        
        self.log(f"Comparing {name_a.upper()} vs {name_b.upper()}: running {name_b}…")
        if name_b == "mountainsort4":
            sort_b = run_mountainsort4(_pick_rec(name_b), self.settings, out_b)
        elif name_b == "kilosort4":
            sort_b = run_kilosort4(_pick_rec(name_b), self.settings, out_b)
        elif name_b == "spykingcircus2":
            sort_b = run_spykingcircus2(_pick_rec(name_b), self.settings, out_b)
        else:
            raise ValueError(f"Unknown sorter B: {name_b}")
        
        # Pairwise comparison
        self.log("Computing unit matching (Hungarian) and agreement scores…")
        comp = sicomp.compare_two_sorters(
            sorting1=sort_a, sorting2=sort_b,
            sorting1_name=name_a.upper(), sorting2_name=name_b.upper()
        )
        
        # Export matches CSV (robust to different score column names)
        df_pairs = comp.get_matching().copy()
        df_pairs = df_pairs.rename(columns={
            "unit_id1": f"{tag_map[name_a]}_unit_id",
            "unit_id2": f"{tag_map[name_b]}_unit_id",
        })
        # Normalize score column to "agreement"
        if "match_score" in df_pairs.columns:
            score_col = "match_score"
        elif "agreement_score" in df_pairs.columns:
            score_col = "agreement_score"
        else:
            # fallback: try generic "score"
            score_col = "score" if "score" in df_pairs.columns else None
        if score_col is None:
            df_pairs["agreement"] = np.nan
        else:
            df_pairs = df_pairs.rename(columns={score_col: "agreement"})
        
        csv_pairs = os.path.join(base_out, f"{tag_map[name_a]}_vs_{tag_map[name_b]}_matches.csv")
        df_pairs.to_csv(csv_pairs, index=False)
        
        # Unmatched CSVs
        unmatched_a = comp.get_unmatched1()
        unmatched_b = comp.get_unmatched2()
        pd.DataFrame({"unit_id": list(unmatched_a)}).to_csv(os.path.join(base_out, f"unmatched_{tag_map[name_a]}.csv"), index=False)
        pd.DataFrame({"unit_id": list(unmatched_b)}).to_csv(os.path.join(base_out, f"unmatched_{tag_map[name_b]}.csv"), index=False)
        
        # --- Quick agreement heatmap ---
        units_a = sort_a.get_unit_ids()
        units_b = sort_b.get_unit_ids()
        idx_a = {u: i for i, u in enumerate(units_a)}
        idx_b = {u: i for i, u in enumerate(units_b)}
        
        M = np.zeros((len(units_a), len(units_b)), dtype=float)
        if "agreement" in df_pairs.columns:
            for _, row in df_pairs.iterrows():
                ua = row[f"{tag_map[name_a]}_unit_id"]
                ub = row[f"{tag_map[name_b]}_unit_id"]
                if ua in idx_a and ub in idx_b:
                    M[idx_a[ua], idx_b[ub]] = row["agreement"]
    
        fig, ax = plt.subplots(figsize=(max(4, len(units_b) * 0.25), max(3, len(units_a) * 0.25)), dpi=150)
        im = ax.imshow(M, aspect="auto", interpolation="nearest")
        ax.set_title(f"Agreement heatmap: {name_a.upper()} vs {name_b.upper()}")
        ax.set_xlabel(f"{name_b.upper()} units")
        ax.set_ylabel(f"{name_a.upper()} units")
        fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04, label="Agreement (0–1)")
    
        # Compact tick labels (only if small)
        if len(units_a) <= 50:
            ax.set_yticks(range(len(units_a)))
            ax.set_yticklabels([str(u) for u in units_a], fontsize=6)
        else:
            ax.set_yticks([])
        if len(units_b) <= 50:
            ax.set_xticks(range(len(units_b)))
            ax.set_xticklabels([str(u) for u in units_b], fontsize=6, rotation=90)
        else:
            ax.set_xticks([])
    
        fig.tight_layout()
        heat_path = os.path.join(base_out, f"agreement_{tag_map[name_a]}_vs_{tag_map[name_b]}.png")
        fig.savefig(heat_path)
        plt.close(fig)
    
        self.log(f"Matches CSV: {csv_pairs}")
        self.log(f"Agreement heatmap: {heat_path}")
        messagebox.showinfo("Compare done", f"Matches: {os.path.basename(csv_pairs)}\nHeatmap: {os.path.basename(heat_path)}")
    
    def compare_ms4_vs_ks4(self):
        return self._compare_sorters_generic("mountainsort4", "kilosort4")
    
    def compare_ms4_vs_sc2(self):
        return self._compare_sorters_generic("mountainsort4", "spykingcircus2")
    
    def compare_ks4_vs_sc2(self):
        return self._compare_sorters_generic("kilosort4", "spykingcircus2")

    def compute_metrics(self):
        try:
            if self.waveforms is None:
                messagebox.showwarning("Missing", "Run sorting first to extract waveforms")
                return
    
            self.log("Computing quality metrics…")
    
            include_pc = bool(self.var_pc_metrics.get()) if hasattr(self, "var_pc_metrics") else self.settings.compute_pc_metrics
            self.settings.compute_pc_metrics = include_pc
    
            # Ensure required deps; SI skips if already computed
            self.log("Ensuring 'templates' extension…")
            self.waveforms.compute("templates", n_jobs=self.settings.n_jobs)
    
            self.log("Ensuring 'spike_amplitudes' extension…")
            self.waveforms.compute("spike_amplitudes", n_jobs=self.settings.n_jobs)
    
            self.log("Ensuring 'noise_levels' extension…")
            # DO NOT pass return_in_uV/return_scaled on SI 0.103
            self.waveforms.compute("noise_levels", method="mad")
    
            if include_pc:
                self.log("Ensuring 'principal_components' extension…")
                self.waveforms.compute("principal_components", n_components=3, mode="by_channel_global")
    
            # ---- Choose metric names that exist on this SI build ----
            want_base = ["firing_rate", "isi_violations_ratio", "snr", "amplitude_cutoff", "presence_ratio"]
            want_pc   = ["isolation_distance", "l_ratio"] if include_pc else []
    
            aliases = {
                "isi_violations_ratio": ["isi_violation", "isi_violation_ratio"],
                "presence_ratio": ["presence_ratio"],
                "snr": ["snr"],
                "amplitude_cutoff": ["amplitude_cutoff"],
                "firing_rate": ["firing_rate"],
                "isolation_distance": ["isolation_distance"],
                "l_ratio": ["l_ratio"],
            }
    
            try:
                from spikeinterface.qualitymetrics.quality_metric_calculator import (
                    _misc_metric_name_to_func, _possible_pc_metric_names
                )
                supported_nonpc = set(_misc_metric_name_to_func.keys())
                supported_pc    = set(_possible_pc_metric_names)
            except Exception:
                supported_nonpc = {"firing_rate","isi_violation","snr","amplitude_cutoff","presence_ratio"}
                supported_pc    = {"isolation_distance","l_ratio"}
    
            def resolve_names(wants, supported):
                picked = []
                for w in wants:
                    for cand in aliases.get(w, [w]):
                        if cand in supported:
                            picked.append(cand)
                            break
                    else:
                        self.log(f"Skipping unsupported metric on this SI: {w}")
                return picked
    
            metric_names = resolve_names(want_base, supported_nonpc) + resolve_names(want_pc, supported_pc)
            self.log(f"Using metric names: {metric_names}")
    
            # Compute as an analyzer extension (lets SI enforce deps)
            self.waveforms.compute(
                "quality_metrics",
                metric_names=metric_names,
                skip_pc_metrics=not include_pc,
                delete_existing_metrics=False,
                verbose=True,
            )
    
            # Read back as DataFrame and save
            from spikeinterface import qualitymetrics as qim
            self.metrics_df = qim.compute_quality_metrics(self.waveforms, metric_names=None)
            self.metrics_df.index.name = "unit_id"
    
            # Back-compat column for code expecting 'isi_violations_ratio'
            if "isi_violation" in self.metrics_df.columns and "isi_violations_ratio" not in self.metrics_df.columns:
                self.metrics_df["isi_violations_ratio"] = self.metrics_df["isi_violation"]
    
            out = os.path.join(self.var_out.get() or os.getcwd(), "quality_metrics.xlsx")
            self.metrics_df.to_excel(out)
            self.log(f"Quality metrics written to: {out}")
    
        except Exception as e:
            self.log(f"Metrics error: {e}")
            traceback.print_exc()
            messagebox.showerror("Metrics error", str(e))
   
    def preview_curation(self):
        if self.metrics_df is None:
            messagebox.showwarning("No metrics", "Compute metrics first")
            return
        kept = curate_units(self.metrics_df)
        msg = f"Units kept with defaults:\n{len(kept)} / {self.metrics_df.shape[0]}\n\nIDs: {kept[:50]}{'…' if len(kept)>50 else ''}"
        messagebox.showinfo("Curation preview", msg)

    def export_unit_spikes_xlsx(self):
        try:
            if self.sorting is None or self.waveforms is None:
                messagebox.showwarning("Missing", "Run sorting first")
                return
            fs = float(self.waveforms.recording.get_sampling_frequency())
            out = os.path.join(self.var_out.get() or os.getcwd(), "units_spike_times.xlsx")
            export_unit_spike_times(self.sorting, fs, out)
            self.log(f"Spike times exported: {out}")
        except Exception as e:
            self.log(f"Export error: {e}")
            traceback.print_exc()
            messagebox.showerror("Export error", str(e))

    def build_unit_reports(self):
        try:
            if self.waveforms is None:
                messagebox.showwarning("Missing", "Run sorting first")
                return
            rep_dir = os.path.join(self.var_out.get() or os.getcwd(), "unit_reports")
            
            # NEW: sync UI toggles into settings
            if hasattr(self, "var_include_acg"):
                self.settings.include_acg_in_reports = bool(self.var_include_acg.get())
            if hasattr(self, "var_acg_pdf"):
                self.settings.acg_use_pdf = bool(self.var_acg_pdf.get())
            
            make_unit_report_figures(self.waveforms, rep_dir, self.settings, self.metrics_df)
            self.log(f"Unit reports saved under: {rep_dir}")
        except Exception as e:
            self.log(f"Report error: {e}")
            traceback.print_exc()
            messagebox.showerror("Report error", str(e))


if __name__ == "__main__":
    app = MEAGUI()
    app.mainloop()
