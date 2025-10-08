import matplotlib.pyplot as plt
import numpy as np
from astropy.table import Table
import pickle

def apply_teff_correction(gaia_teff, correction_file):
    """
    Apply polynomial correction to Gaia Teff values.

    Args:
        gaia_teff: Gaia GSP-Phot Teff values
        correction_file: Path to pickle file with correction polynomial

    Returns:
        Corrected catalog Teff prediction
    """
    with open(correction_file, 'rb') as f:
        correction_data = pickle.load(f)

    poly = correction_data['polynomial']
    threshold = correction_data['teff_threshold']

    # Apply correction: predicted catalog Teff from Gaia Teff
    corrected_teff = poly(gaia_teff)

    return corrected_teff, threshold


def compare_teff_corrected(wumacat_file, debcat_file, correction_file,
                           output_prefix="teff_comparison_corrected"):
    """
    Compare Gaia GSP-Phot Teff with catalog average Teff, applying correction.

    Args:
        wumacat_file (str): Path to WUMaCat cross-matched with Gaia ECSV file
        debcat_file (str): Path to debcat cross-matched with Gaia ECSV file
        correction_file (str): Path to correction polynomial pickle file
        output_prefix (str): Prefix for output plot files
    """

    # Read the tables
    print("Reading WUMaCat...")
    wumacat = Table.read(wumacat_file, format='ascii.ecsv')
    print(f"WUMaCat: {len(wumacat)} objects")

    print("Reading debcat...")
    debcat = Table.read(debcat_file, format='ascii.ecsv')
    print(f"debcat: {len(debcat)} objects")

    # Calculate average temperatures for WUMaCat
    wuma_mask_avg = (~np.isnan(wumacat['teff_gspphot'])) & (wumacat['teff_gspphot'] > 0) & \
                    (~np.isnan(wumacat['T1'])) & (wumacat['T1'] > 0) & \
                    (~np.isnan(wumacat['T2'])) & (wumacat['T2'] > 0)
    wuma_teff_gsp_avg = wumacat['teff_gspphot'][wuma_mask_avg]
    wuma_t_avg = (wumacat['T1'][wuma_mask_avg] + wumacat['T2'][wuma_mask_avg]) / 2

    # Calculate average temperatures for debcat
    deb_mask_avg = (~np.isnan(debcat['teff_gspphot'])) & (debcat['teff_gspphot'] > 0) & \
                   (~np.isnan(debcat['logT1'])) & (debcat['logT1'] > 0) & \
                   (~np.isnan(debcat['logT2'])) & (debcat['logT2'] > 0)
    deb_teff_gsp_avg = debcat['teff_gspphot'][deb_mask_avg]
    deb_t_avg = (10**debcat['logT1'][deb_mask_avg] + 10**debcat['logT2'][deb_mask_avg]) / 2

    print(f"\nValid measurements:")
    print(f"WUMaCat: {len(wuma_t_avg)} systems")
    print(f"debcat: {len(deb_t_avg)} systems")

    # Apply correction only to hot stars
    print(f"\nLoading correction from: {correction_file}")

    # Get threshold from correction file
    with open(correction_file, 'rb') as f:
        correction_data = pickle.load(f)
    threshold = correction_data['teff_threshold']

    print(f"Correction applies to stars with average catalog Teff >= {threshold}K")

    # For WUMaCat: apply correction only where average Teff >= threshold
    wuma_teff_corrected = np.array(wuma_teff_gsp_avg).copy()
    wuma_hot_mask = np.array(wuma_t_avg) >= threshold
    if np.sum(wuma_hot_mask) > 0:
        wuma_teff_corrected[wuma_hot_mask], _ = apply_teff_correction(
            wuma_teff_gsp_avg[wuma_hot_mask], correction_file)
        print(f"  WUMaCat: {np.sum(wuma_hot_mask)} hot systems (corrected), "
              f"{np.sum(~wuma_hot_mask)} cool systems (uncorrected)")
    else:
        print(f"  WUMaCat: No systems above threshold")

    # For debcat: apply correction only where average Teff >= threshold
    deb_teff_corrected = np.array(deb_teff_gsp_avg).copy()
    deb_hot_mask = np.array(deb_t_avg) >= threshold
    if np.sum(deb_hot_mask) > 0:
        deb_teff_corrected[deb_hot_mask], _ = apply_teff_correction(
            deb_teff_gsp_avg[deb_hot_mask], correction_file)
        print(f"  debcat: {np.sum(deb_hot_mask)} hot systems (corrected), "
              f"{np.sum(~deb_hot_mask)} cool systems (uncorrected)")
    else:
        print(f"  debcat: No systems above threshold")

    # Plot 1: Corrected Gaia Teff vs Average Catalog Teff
    fig, ax = plt.subplots(figsize=(10, 8))

    # Plot WUMaCat
    ax.scatter(wuma_t_avg, wuma_teff_corrected, alpha=0.6, s=30, c='blue',
               label=f'WUMaCat (n={len(wuma_t_avg)})', edgecolors='none')

    # Plot debcat
    ax.scatter(deb_t_avg, deb_teff_corrected, alpha=0.6, s=30, c='red',
               label=f'debcat (n={len(deb_t_avg)})', edgecolors='none')

    # Add 1:1 line
    all_temps_avg = np.concatenate([np.array(wuma_t_avg), np.array(deb_t_avg),
                                    np.array(wuma_teff_corrected), np.array(deb_teff_corrected)])
    min_temp_avg = np.min(all_temps_avg)
    max_temp_avg = np.max(all_temps_avg)
    ax.plot([min_temp_avg, max_temp_avg], [min_temp_avg, max_temp_avg], 'k--',
            alpha=0.5, linewidth=1, label='1:1 line')

    # Add vertical line at threshold
    ax.axvline(threshold, color='green', linestyle=':', linewidth=2, alpha=0.7,
               label=f'Correction threshold ({threshold}K)')

    ax.set_xlabel('Catalog Average Teff [(T1+T2)/2] [K]', fontsize=12)
    ax.set_ylabel('Corrected Gaia Teff [K]', fontsize=12)
    ax.set_title('Comparison: Corrected Gaia Teff vs Catalog Average Teff', fontsize=14)
    ax.legend(loc='upper left', fontsize=10)
    ax.grid(True, alpha=0.3)
    ax.set_aspect('equal', adjustable='box')

    plt.tight_layout()
    plt.savefig(f'{output_prefix}_avg.png', dpi=150)
    print(f"\nSaved: {output_prefix}_avg.png")
    plt.close()

    # Plot 2: Distribution comparison
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    def plot_distribution(ax, cat_data, corrected_data, title):
        diff = np.array(corrected_data) - np.array(cat_data)

        ax.hist(diff, bins=50, alpha=0.7, edgecolor='black', linewidth=0.5)
        ax.axvline(0, color='red', linestyle='--', linewidth=2, label='Zero difference')
        ax.axvline(np.mean(diff), color='green', linestyle='-', linewidth=2,
                   label=f'Mean: {np.mean(diff):.1f} K')
        ax.axvline(np.median(diff), color='blue', linestyle='-', linewidth=2,
                   label=f'Median: {np.median(diff):.1f} K')

        ax.set_xlabel('Corrected Gaia Teff - Catalog Teff [K]', fontsize=10)
        ax.set_ylabel('Number of objects', fontsize=10)
        ax.set_title(title, fontsize=11)
        ax.legend(fontsize=8)
        ax.grid(True, alpha=0.3)

    # WUMaCat Average
    plot_distribution(axes[0], wuma_t_avg, wuma_teff_corrected,
                     f'WUMaCat Average (n={len(wuma_t_avg)})')

    # debcat Average
    plot_distribution(axes[1], deb_t_avg, deb_teff_corrected,
                     f'debcat Average (n={len(deb_t_avg)})')

    plt.tight_layout()
    plt.savefig(f'{output_prefix}_distributions_avg.png', dpi=150)
    print(f"Saved: {output_prefix}_distributions_avg.png")
    plt.close()

    # Print statistics
    print("\n=== Statistics (Corrected - Catalog) ===")

    def print_stats(catalog_name, cat_teff, corrected_teff):
        diff = np.array(corrected_teff) - np.array(cat_teff)
        rel_diff = (np.array(corrected_teff) - np.array(cat_teff)) / np.array(cat_teff) * 100

        print(f"\n{catalog_name}:")
        print(f"  Mean difference: {np.mean(diff):.1f} K ({np.mean(rel_diff):.1f}%)")
        print(f"  Median difference: {np.median(diff):.1f} K ({np.median(rel_diff):.1f}%)")
        print(f"  Std deviation: {np.std(diff):.1f} K ({np.std(rel_diff):.1f}%)")
        print(f"  RMS: {np.sqrt(np.mean(diff**2)):.1f} K")

    if len(wuma_t_avg) > 0:
        print_stats("WUMaCat Average", wuma_t_avg, wuma_teff_corrected)
    if len(deb_t_avg) > 0:
        print_stats("debcat Average", deb_t_avg, deb_teff_corrected)


if __name__ == "__main__":
    import argparse
    import os

    parser = argparse.ArgumentParser(description='Compare catalog Teff with corrected Gaia Teff')
    parser.add_argument('--correction', type=str, required=True,
                       help='Path to correction polynomial pickle file (e.g., teff_correction_coeffs_deg2.pkl)')
    parser.add_argument('--wumacat', type=str, default="./data/WUMaCat_cross_with_gaia.ecsv",
                       help='Path to WUMaCat ECSV file')
    parser.add_argument('--debcat', type=str, default="./data/debcat_cross_with_gaia.ecsv",
                       help='Path to debcat ECSV file')
    parser.add_argument('--output', type=str, default=None,
                       help='Output prefix for plots (default: auto-generated from correction filename)')

    args = parser.parse_args()

    # Auto-generate output prefix based on correction file if not provided
    if args.output is None:
        # Extract degree from filename (e.g., teff_correction_coeffs_deg2.pkl -> deg2)
        basename = os.path.basename(args.correction)
        if 'deg' in basename:
            degree_str = basename.split('deg')[1].split('.')[0]
            args.output = f"./results/teff_comparison/teff_comparison_corrected_deg{degree_str}"
        else:
            args.output = "./results/teff_comparison/teff_comparison_corrected"

    print("="*60)
    print("CORRECTED TEFF COMPARISON")
    print("="*60)
    print(f"Correction file: {args.correction}")
    print(f"Output prefix: {args.output}")
    print("="*60 + "\n")

    compare_teff_corrected(args.wumacat, args.debcat, args.correction,
                          output_prefix=args.output)
