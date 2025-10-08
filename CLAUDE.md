# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

WUMaCat is an astronomical data processing pipeline for W Ursae Majoris (W UMa) contact binary stars and eclipsing binaries. The project processes TESS (Transiting Exoplanet Survey Satellite) light curves, cross-matches astronomical catalogs, and prepares data for analysis with ELISA (Eclipsing Light Curve Software).

## Environment Setup

```bash
# Activate virtual environment
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

## Key Data Files

The repository contains three main astronomical catalogs in `data/`:
- **WUMaCat.csv**: W UMa contact binary catalog
- **debcat.txt**: Detached/semi-detached eclipsing binary catalog
- **only_SP_objects.csv**: Short-period objects subset

Processed light curves are stored in:
- `data/wuma_lcs/`: W UMa light curves (CSV format with phase-folded data)
- `data/deb_lcs/`: Eclipsing binary light curves

## Data Processing Pipeline

The standard workflow follows this sequence:

1. **Coordinate Retrieval** (`scripts/retrieve_coord_from_simbad.py`)
   - Queries SIMBAD for RA/Dec coordinates based on object names
   - Input: CSV/TXT file with 'name' column
   - Output: CSV with added 'ra' and 'dec' columns

2. **Gaia Cross-matching** (`scripts/cross_with_gaia.py`)
   - Cross-matches objects with Gaia DR3 catalog
   - Requires environment variables: `GAIA_USERNAME` and `GAIA_PASSWORD`
   - Uploads table to Gaia archive, performs cross-match, retrieves results
   - Output: ECSV file with Gaia parameters

3. **Epoch Retrieval** (`scripts/take_epoch_from_vsx.py`)
   - Queries VSX (AAVSO Variable Star Index) for minimum light epochs (JD_min)
   - Converts HJD to JD-2450000 format
   - Output: ECSV with 'jd_min' column

4. **Light Curve Processing** (`scripts/create_lc_for_elisa.py`)
   - Finds eclipse center (JD_min) via Gaussian fitting if not provided
   - Folds light curves with orbital period
   - Normalizes flux to maximum value
   - Output: CSV files with columns: `jd`, `flux`, `phase`, `normalized_flux`
   - Metadata (jd_min, period) stored in table metadata

## Script Usage Examples

### Retrieve coordinates from SIMBAD
```python
from scripts.retrieve_coord_from_simbad import retrieve_coord_from_simbad
df = retrieve_coord_from_simbad("./data/WUMaCat.csv", output_filename="./data/WUMaCat_with_coord.csv")
```

### Cross-match with Gaia
```python
from scripts.cross_with_gaia import gaia_cross_match
# Set environment variables first: GAIA_USERNAME, GAIA_PASSWORD
results = gaia_cross_match("./data/WUMaCat_with_coord.csv", table_name='wumacat',
                          radius=1, output_filename="./data/WUMaCat_cross_with_gaia.ecsv")
```

### Add VSX epochs
```python
from scripts.take_epoch_from_vsx import take_epoch_from_vsx
table = take_epoch_from_vsx("./data/SP_cross_with_gaia.ecsv",
                           output_filename="./data/sp_final_with_jd_min.ecsv")
```

### Process light curves
```python
from scripts.create_lc_for_elisa import fold_and_normalize_lightcurves
processed = fold_and_normalize_lightcurves("./data/WUMaCat_with_coord.csv",
                                          "./only_one_obj/", period_column='P')
```

## Notebook Usage

**plot_folded_tess_lc.ipynb**: Visualizes light curves
- Plots raw ECSV light curves (JD vs flux)
- Plots folded CSV light curves (phase vs normalized_flux)

```python
from notebooks.plot_folded_tess_lc import plot_lightcurve
plot_lightcurve('../data/wuma_lcs/tess_lc_AC Boo_tess_jdmin.csv')
```

## Important Notes

- All scripts expect case-insensitive 'name' column in input tables
- RA/Dec can be in sexagesimal (HH:MM:SS) or decimal degree format
- The Gaia cross-match requires authentication and performs asynchronous jobs
- JD_min values are stored as JD - 2450000 for compact storage
- Light curve files use consistent naming: `tess_lc_{object_name}_tess_jdmin.csv`
- Period column name is configurable (default: 'P')

## Astronomy-Specific Conventions

- **JD (Julian Date)**: Standard astronomical time format
- **Phase**: Orbital phase from 0 to 1, calculated as `((JD - JD_min) / Period) % 1`
- **Normalized flux**: Flux divided by maximum flux value
- **W UMa systems**: Contact binary stars with periods typically < 1 day
- **Eclipse fitting**: Uses Gaussian model to find primary minimum center
