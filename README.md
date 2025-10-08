# WUMaCat & DEBCat: Binary Star Analysis Pipeline

A comprehensive astronomical data processing pipeline for analyzing **W Ursae Majoris (W UMa) contact binary stars** (WUMaCat) and **detached/semi-detached eclipsing binaries** (DEBCat) using TESS (Transiting Exoplanet Survey Satellite) light curves and Gaia DR3 data.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Installation](#installation)
- [Project Structure](#project-structure)
- [Data Catalogs](#data-catalogs)
- [Workflows](#workflows)
  - [1. Catalog Preparation Workflow](#1-catalog-preparation-workflow)
  - [2. Light Curve Processing Workflow](#2-light-curve-processing-workflow)
  - [3. Gaia Teff Analysis Workflow](#3-gaia-teff-analysis-workflow)
- [Script Documentation](#script-documentation)
- [Output Files](#output-files)
- [Usage Examples](#usage-examples)
- [Important Notes](#important-notes)
- [Requirements](#requirements)

## Overview

This pipeline processes two complementary binary star catalogs:

**WUMaCat** (~600 systems): W Ursae Majoris contact binaries
- Short periods (typically < 1 day)
- Cool stars (Teff typically < 10,000K)
- Both components in contact, sharing common envelope

**DEBCat** (~300 systems): Detached and semi-detached eclipsing binaries
- Wider range of periods
- Includes hot stars (some Teff > 10,000K)
- Components separated or in semi-contact

The pipeline is designed to:
- Cross-match both catalogs with SIMBAD and Gaia DR3
- Process and phase-fold TESS light curves for all binary star systems
- Compare catalog effective temperatures with Gaia GSP-Phot measurements
- Develop polynomial corrections for Gaia temperature systematics (especially important for hot stars in DEBCat)
- Prepare data for analysis with ELISA (Eclipsing Light Curve Software)

## Features

- **Automated catalog cross-matching** with SIMBAD and Gaia DR3
- **VSX epoch retrieval** for variable stars
- **Light curve processing**: Gaussian fitting for eclipse centers, phase folding, normalization
- **Teff analysis**: Compare catalog and Gaia effective temperatures with statistical analysis
- **Polynomial corrections**: Fit and apply corrections for Gaia Teff systematic errors
- **Flexible filtering**: Analyze specific temperature ranges
- **Organized outputs**: All results saved to structured directories

## Installation

### Prerequisites

- Python 3.8+
- Virtual environment (recommended)

### Setup

1. Clone the repository:
```bash
cd WUMaCat
```

2. Create and activate virtual environment:
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Set up Gaia credentials (for cross-matching):
```bash
export GAIA_USERNAME="your_username"
export GAIA_PASSWORD="your_password"
```

## Project Structure

```
WUMaCat/
├── data/                          # Input catalogs and processed data
│   ├── WUMaCat.csv               # W UMa contact binary catalog
│   ├── debcat.txt                # Detached/semi-detached binary catalog
│   ├── only_SP_objects.csv       # Short-period objects
│   ├── WUMaCat_cross_with_gaia.ecsv  # Gaia cross-matched catalog
│   ├── debcat_cross_with_gaia.ecsv   # Gaia cross-matched catalog
│   ├── wuma_lcs/                 # Processed W UMa light curves
│   └── deb_lcs/                  # Processed eclipsing binary light curves
├── scripts/                       # Python analysis scripts
│   ├── retrieve_coord_from_simbad.py
│   ├── cross_with_gaia.py
│   ├── take_epoch_from_vsx.py
│   ├── create_lc_for_elisa.py
│   ├── compare_teff.py
│   ├── fit_teff_correction.py
│   └── compare_teff_corrected.py
├── notebooks/                     # Jupyter notebooks
│   ├── plot_folded_tess_lc.ipynb
│   └── find_a_minima.ipynb
├── results/                       # Output directory (auto-created)
│   ├── teff_comparison/          # Teff comparison plots
│   └── teff_correction/          # Correction polynomials
├── requirements.txt              # Python dependencies
├── CLAUDE.md                     # Claude Code guidance
└── README.md                     # This file
```

## Data Catalogs

### Input Catalogs

1. **WUMaCat.csv**: W Ursae Majoris contact binary catalog
   - Type: Contact binaries (both components share common envelope)
   - Columns: `name`, `P` (period), `T1`, `T2` (temperatures in Kelvin), `M1`, `M2` (masses), etc.
   - Temperature range: Typically 3,000-8,000K (cool stars)
   - Period range: Typically 0.2-1.0 days (short periods)
   - ~600+ systems

2. **DEBCat** (debcat.txt): Detached and semi-detached eclipsing binary catalog
   - Type: Detached/semi-detached binaries (components separated or touching)
   - Columns: `name`, `P` (period), `logT1`, `logT2` (log temperatures), `logM1`, `logM2` (log masses), etc.
   - Temperature units: **log10(Kelvin)** - must convert with 10^logT
   - Temperature range: 3,000-30,000K (includes hot stars)
   - Period range: Wide range from hours to hundreds of days
   - ~300+ systems
   - **Note**: This catalog contains the hot stars (Teff > 10,000K) that require Gaia Teff corrections

3. **only_SP_objects.csv**: Short-period variable stars subset

### Cross-matched Catalogs (ECSV format)

After running the catalog preparation workflow:
- **WUMaCat_cross_with_gaia.ecsv**: WUMaCat + Gaia DR3 parameters (~600 systems)
- **debcat_cross_with_gaia.ecsv**: DEBCat + Gaia DR3 parameters (~300 systems)

Both catalogs include Gaia columns:
- `teff_gspphot`: Effective temperature from Gaia GSP-Phot
- `logg_gspphot`: Surface gravity
- `mh_gspphot`: Metallicity
- `parallax`, `pmra`, `pmdec`: Astrometric parameters
- Photometry: `phot_g_mean_mag`, `phot_bp_mean_mag`, `phot_rp_mean_mag`

## Workflows

### 1. Catalog Preparation Workflow

**Purpose**: Enrich catalog with coordinates and Gaia DR3 parameters

**Step 1: Retrieve coordinates from SIMBAD**
```bash
python scripts/retrieve_coord_from_simbad.py
```
- Queries SIMBAD for RA/Dec coordinates
- Input: `data/WUMaCat.csv` or `data/debcat.txt`
- Output: CSV with `ra` and `dec` columns

**Step 2: Cross-match with Gaia DR3**
```bash
export GAIA_USERNAME="your_username"
export GAIA_PASSWORD="your_password"
python scripts/cross_with_gaia.py
```
- Uploads catalog to Gaia archive
- Performs 1 arcsec cross-match with Gaia DR3
- Output: ECSV file with Gaia parameters

**Step 3: Retrieve VSX epochs (optional)**
```bash
python scripts/take_epoch_from_vsx.py
```
- Queries VSX (AAVSO Variable Star Index) for JD_min epochs
- Adds `jd_min` column for primary minimum times

### 2. Light Curve Processing Workflow

**Purpose**: Prepare phase-folded, normalized light curves for ELISA analysis

**Process light curves**
```bash
python scripts/create_lc_for_elisa.py
```

**What it does:**
1. Reads catalog with periods and object names
2. Finds corresponding TESS light curve files
3. For each object:
   - Fits Gaussian to find eclipse center (JD_min)
   - Folds light curve with orbital period
   - Normalizes flux to maximum value
   - Saves to `data/wuma_lcs/` or `data/deb_lcs/`

**Output format (CSV):**
- `jd`: Julian Date
- `flux`: Observed flux
- `phase`: Orbital phase (0-1)
- `normalized_flux`: Flux normalized to maximum

**Metadata stored:**
- `jd_min`: Eclipse center time
- `period`: Orbital period (days)

### 3. Gaia Teff Analysis Workflow

**Purpose**: Compare catalog and Gaia effective temperatures, develop corrections

#### Step 1: Compare Teff measurements

**Compare all temperatures:**
```bash
python scripts/compare_teff.py
```

**Compare specific temperature ranges:**
```bash
# Cool stars only (< 10000K)
python scripts/compare_teff.py --teff-max 10000

# Hot stars only (>= 10000K)
python scripts/compare_teff.py --teff-min 10000

# Custom range (5000-8000K)
python scripts/compare_teff.py --teff-min 5000 --teff-max 8000
```

**Outputs** (saved to `results/teff_comparison/`):
- `teff_comparison_T1.png`: Primary star comparison
- `teff_comparison_T2.png`: Secondary star comparison
- `teff_comparison_avg.png`: Average temperature comparison
- `teff_comparison_distributions.png`: Difference histograms (T1, T2)
- `teff_comparison_distributions_avg.png`: Difference histogram (average)
- Statistical summary printed to console

#### Step 2: Fit polynomial correction (for hot stars)

Gaia GSP-Phot systematically underestimates temperatures for hot stars (Teff > 10000K). Fit a polynomial correction:

```bash
# Linear correction
python scripts/fit_teff_correction.py --degree 1

# Quadratic correction (recommended)
python scripts/fit_teff_correction.py --degree 2

# Cubic correction
python scripts/fit_teff_correction.py --degree 3

# Custom threshold
python scripts/fit_teff_correction.py --degree 2 --threshold 12000
```

**How it works:**
1. Selects binary systems with average Teff ≥ threshold (default 10000K)
2. Computes average catalog Teff: `(T1 + T2) / 2`
3. Fits polynomial: `Catalog_Teff = f(Gaia_Teff)`
4. Saves polynomial coefficients and diagnostic plots

**Outputs** (saved to `results/teff_correction/`):
- `teff_correction_coeffs_deg{N}.pkl`: Polynomial coefficients
- `teff_correction_fit_deg{N}.png`: Fit visualization and residuals

**Example results (32 hot systems, Teff ≥ 10000K):**
- Linear (deg 1): RMS = 3247K
- Quadratic (deg 2): RMS = 3209K ✓ recommended
- Cubic (deg 3): RMS = 2951K

#### Step 3: Apply correction

```bash
# Apply quadratic correction
python scripts/compare_teff_corrected.py --correction results/teff_correction/teff_correction_coeffs_deg2.pkl

# Apply other corrections
python scripts/compare_teff_corrected.py --correction results/teff_correction/teff_correction_coeffs_deg3.pkl
```

**Important**: Correction is only applied to stars **above** the temperature threshold used during fitting. Cool stars remain uncorrected.

**Outputs** (saved to `results/teff_comparison/`):
- `teff_comparison_corrected_deg{N}_avg.png`: Corrected comparison
- `teff_comparison_corrected_deg{N}_distributions_avg.png`: Corrected distributions

## Script Documentation

### Catalog Preparation Scripts

#### `retrieve_coord_from_simbad.py`

Retrieves RA/Dec coordinates from SIMBAD by object name.

**Key function:**
```python
retrieve_coord_from_simbad(input_data, output_filename=None)
```

**Parameters:**
- `input_data`: Path to CSV/TXT file or pandas DataFrame (must have 'name' column)
- `output_filename`: Output CSV path (optional)

**Returns:** DataFrame with `ra` and `dec` columns

**Formats accepted:**
- RA/Dec: Sexagesimal (HH:MM:SS, DD:MM:SS) or decimal degrees
- Automatically converts to decimal degrees

#### `cross_with_gaia.py`

Cross-matches catalog with Gaia DR3 using 1 arcsec radius.

**Key function:**
```python
gaia_cross_match(input_data, table_name="my_table",
                 gaia_table="gaiadr3.gaia_source",
                 radius=1, output_filename=None)
```

**Parameters:**
- `input_data`: CSV file or DataFrame with 'name', 'ra', 'dec'
- `table_name`: Gaia archive table name
- `radius`: Cross-match radius (arcseconds)
- `output_filename`: Output ECSV path

**Requirements:**
- Environment variables: `GAIA_USERNAME`, `GAIA_PASSWORD`
- Gaia archive account (free at https://gea.esac.esa.int/archive/)

**Returns:** Astropy Table with Gaia parameters

#### `take_epoch_from_vsx.py`

Retrieves primary minimum epochs from VSX catalog.

**Key function:**
```python
take_epoch_from_vsx(input_table, output_filename, catalog="B/vsx/vsx")
```

**Parameters:**
- `input_table`: ECSV file path or Astropy Table
- `output_filename`: Output ECSV path
- `catalog`: Vizier catalog identifier

**Returns:** Table with added `jd_min` column

### Light Curve Processing Scripts

#### `create_lc_for_elisa.py`

Processes TESS light curves for ELISA analysis.

**Key function:**
```python
fold_and_normalize_lightcurves(input_table_filename, lightcurve_dir, period_column='P')
```

**Parameters:**
- `input_table_filename`: Catalog CSV/ECSV with periods
- `lightcurve_dir`: Directory containing TESS light curves
- `period_column`: Column name for orbital period

**Processing steps:**
1. **Find JD_min**: Fits Gaussian to eclipse to find center
2. **Fold**: Computes phase = `((JD - JD_min) / Period) % 1`
3. **Normalize**: Divides flux by maximum flux
4. **Save**: Outputs CSV with phase-sorted data

**Output columns:**
- `jd`: Julian Date
- `flux`: Original flux
- `phase`: Orbital phase (0-1)
- `normalized_flux`: Normalized flux

**Metadata:**
- `jd_min`: Eclipse center (JD)
- `period`: Orbital period (days)

### Teff Analysis Scripts

#### `compare_teff.py`

Compares catalog and Gaia effective temperatures.

**Usage:**
```bash
python scripts/compare_teff.py [options]
```

**Options:**
- `--teff-min FLOAT`: Minimum average catalog Teff (K)
- `--teff-max FLOAT`: Maximum average catalog Teff (K)
- `--output PREFIX`: Custom output prefix
- `--wumacat PATH`: WUMaCat file path
- `--debcat PATH`: debcat file path

**Temperature filtering:**
- Filter based on average catalog Teff: `(T1 + T2) / 2`
- Applies consistently to all plots (T1, T2, average)

**Outputs:**
- Scatter plots: Gaia Teff vs catalog Teff
- Histograms: Distribution of differences
- Statistics: Mean, median, std, RMS

#### `fit_teff_correction.py`

Fits polynomial correction for Gaia Teff systematics.

**Usage:**
```bash
python scripts/fit_teff_correction.py [options]
```

**Options:**
- `--degree INT`: Polynomial degree (1=linear, 2=quadratic, 3=cubic)
- `--threshold FLOAT`: Minimum Teff for fitting (default: 10000K)
- `--output PATH`: Output PKL file path
- `--wumacat PATH`: WUMaCat file path
- `--debcat PATH`: debcat file path

**Fitting method:**
- Uses average catalog Teff per binary system
- Filters: Gaia Teff > 0 AND average catalog Teff ≥ threshold
- Fits: `Catalog_Teff = f(Gaia_Teff)` using numpy.polynomial.Polynomial

**Output PKL contains:**
```python
{
    'polynomial': Polynomial object,
    'degree': int,
    'teff_threshold': float,
    'n_points': int,
    'rms': float
}
```

#### `compare_teff_corrected.py`

Applies polynomial correction and visualizes results.

**Usage:**
```bash
python scripts/compare_teff_corrected.py --correction PKL_FILE [options]
```

**Options:**
- `--correction PATH`: Required. Polynomial PKL file
- `--output PREFIX`: Custom output prefix
- `--wumacat PATH`: WUMaCat file path
- `--debcat PATH`: debcat file path

**Correction application:**
- Loads threshold from PKL file
- Applies correction **only** to systems with average Teff ≥ threshold
- Cool stars remain uncorrected (use original Gaia Teff)

## Output Files

### Directory Structure

```
results/
├── teff_comparison/
│   ├── teff_comparison_T1.png                    # Primary star comparison
│   ├── teff_comparison_T2.png                    # Secondary star comparison
│   ├── teff_comparison_avg.png                   # Average Teff comparison
│   ├── teff_comparison_distributions.png         # T1/T2 histograms
│   ├── teff_comparison_distributions_avg.png     # Average histogram
│   ├── teff_comparison_below10000K_*.png         # Cool stars only
│   ├── teff_comparison_above10000K_*.png         # Hot stars only
│   ├── teff_comparison_corrected_deg2_*.png      # With correction applied
│   └── ...
└── teff_correction/
    ├── teff_correction_coeffs_deg1.pkl           # Linear correction
    ├── teff_correction_coeffs_deg2.pkl           # Quadratic correction
    ├── teff_correction_coeffs_deg3.pkl           # Cubic correction
    ├── teff_correction_fit_deg1.png              # Linear fit diagnostic
    ├── teff_correction_fit_deg2.png              # Quadratic fit diagnostic
    └── teff_correction_fit_deg3.png              # Cubic fit diagnostic
```

### Plot Descriptions

**Comparison scatter plots:**
- X-axis: Catalog Teff (K)
- Y-axis: Gaia Teff (K)
- Points: Blue = WUMaCat, Red = debcat
- Reference: Black dashed line = 1:1 (perfect agreement)
- Threshold line: Green dotted line (if applicable)

**Distribution histograms:**
- X-axis: Gaia Teff - Catalog Teff (K)
- Y-axis: Number of objects
- Red dashed: Zero difference line
- Green solid: Mean difference
- Blue solid: Median difference

**Fit diagnostic plots:**
- Left panel: Data points + polynomial fit + 1:1 line
- Right panel: Residuals vs catalog Teff

## Usage Examples

### Example 1: Full analysis of cool stars

```bash
# Activate environment
source .venv/bin/activate

# Compare cool stars (< 10000K)
python scripts/compare_teff.py --teff-max 10000

# Check results in results/teff_comparison/
ls results/teff_comparison/teff_comparison_below10000K_*.png
```

**Expected results for cool stars (Teff < 10,000K):**
- **WUMaCat** (all systems are cool): Excellent agreement (mean ~60K, RMS ~540K)
- **DEBCat** (212 cool systems): Good agreement (mean ~193K, RMS ~760K)
- Both catalogs show Gaia GSP-Phot works well for cool stars

### Example 2: Develop correction for hot stars (DEBCat)

Hot stars (Teff > 10,000K) are only found in DEBCat. Gaia systematically underestimates their temperatures.

```bash
# Step 1: Compare hot stars to see the bias
python scripts/compare_teff.py --teff-min 10000

# Step 2: Fit quadratic correction using 32 hot DEBCat systems
python scripts/fit_teff_correction.py --degree 2

# Step 3: Apply correction and compare
python scripts/compare_teff_corrected.py --correction results/teff_correction/teff_correction_coeffs_deg2.pkl
```

**Expected results:**
- **WUMaCat**: No hot stars, so no correction applied (remains uncorrected)
- **DEBCat hot stars (32 systems)**:
  - Before correction: mean = -410K (Gaia systematically too cool)
  - After correction: mean = +168K (bias reduced by ~577K)
  - This improves agreement with catalog temperatures for O, B, and A-type stars

### Example 3: Process light curves for one object

```python
from astropy.table import Table
from scripts.create_lc_for_elisa import fold_and_normalize_lightcurves

# Create test catalog
data = {
    'name': ['AC Boo'],
    'P': [0.3268]  # Period in days
}
test_table = Table(data)
test_table.write('test_catalog.csv', format='csv', overwrite=True)

# Process
processed = fold_and_normalize_lightcurves(
    'test_catalog.csv',
    './data/wuma_lcs/',  # Directory with TESS light curves
    period_column='P'
)
```

## Important Notes

### Data Format Requirements

1. **Case-insensitive 'name' column**: All scripts accept 'name', 'Name', or 'NAME'

2. **Temperature units (CRITICAL):**
   - **WUMaCat**: T1, T2 in **Kelvin** (e.g., 5800 means 5800K)
   - **DEBCat**: logT1, logT2 in **log10(Kelvin)** (e.g., 3.763 means 10^3.763 = 5794K)
   - All scripts automatically handle this difference

3. **RA/Dec formats accepted:**
   - Sexagesimal: "10:00:00", "+20:30:15"
   - Decimal degrees: 150.0, 20.504167
   - Mixed formats in same file OK

4. **JD_min storage:**
   - Stored as JD - 2450000 for compact storage
   - Add 2450000 to get full Julian Date

5. **File naming:**
   - Light curves: `tess_lc_{object_name}_tess_jdmin.csv`
   - Auto-matched by object name (case-insensitive, fuzzy matching)

### Gaia Cross-matching

1. **Authentication required:**
   - Create free account: https://gea.esac.esa.int/archive/
   - Set environment variables before running

2. **Asynchronous job:**
   - Script waits for cross-match to complete
   - May take 30-60 seconds for large catalogs
   - Checks every 3 seconds (max 3 attempts)

3. **Zero Teff filtering:**
   - Gaia sometimes returns Teff = 0 (no measurement)
   - All Teff scripts automatically filter these out

### Temperature Analysis

1. **Binary system averages:**
   - All filtering and corrections use `(T1 + T2) / 2`
   - This represents the combined light Gaia observes

2. **Correction applicability:**
   - Only apply corrections to hot stars (Teff ≥ fitting threshold)
   - Don't extrapolate to cool stars

3. **Statistical interpretation:**
   - Median often more robust than mean (less affected by outliers)
   - RMS combines bias and scatter
   - Watch for large scatter (high std) even if mean is small

## Requirements

### Python Packages

See `requirements.txt` for full list. Key dependencies:

- `astropy >= 7.0`: Astronomical data formats and tables
- `astroquery >= 0.4.7`: Query astronomical databases (SIMBAD, Gaia, VSX)
- `numpy >= 2.2`: Numerical operations
- `pandas >= 2.2`: Data manipulation
- `matplotlib`: Plotting (installed as astropy dependency)
- `scipy`: Scientific computing (Gaussian fitting)

### External Services

- **SIMBAD**: Coordinate queries (no authentication required)
- **Gaia Archive**: Cross-matching (free account required)
- **VizieR/VSX**: Epoch queries (no authentication required)

### System Requirements

- Python 3.8 or higher
- ~500 MB disk space for catalogs and results
- Internet connection for catalog queries

## Troubleshooting

**Issue**: Gaia cross-match times out
- **Solution**: Increase `wait_time` in `cross_with_gaia.py` (line 50)

**Issue**: "No matches found" for object
- **Solution**: Check name format matches SIMBAD/VSX naming
- Try variations: "V* AC Boo", "AC Boo", "AC Bootis"

**Issue**: Temperature comparison shows no data
- **Solution**: Check temperature range - may have filtered all objects
- Try `--teff-max 20000` to see all temperatures

**Issue**: PKL file not found when applying correction
- **Solution**: Use full path to PKL file: `results/teff_correction/teff_correction_coeffs_deg2.pkl`

## Contributing

This is a research project for analyzing W UMa binary stars and eclipsing binaries. Suggestions and improvements welcome.

## Citation

If you use this pipeline in your research, please cite:
- Gaia DR3 catalog: Gaia Collaboration (2023)
- TESS mission: Ricker et al. (2015)
- Your local catalog sources (WUMaCat, debcat)

## License

[Specify your license here]

## Contact

[Your contact information]

---

**Last updated**: 2025-10-08
