import matplotlib.pyplot as plt
import numpy as np
from astropy.table import Table
from numpy.polynomial import Polynomial
import pickle

def fit_teff_correction(wumacat_file, debcat_file, teff_threshold=10000, poly_degree=2,
                       output_file="teff_correction_coeffs.pkl"):
    """
    Fit polynomial correction for Gaia Teff underestimation in hot stars.

    Args:
        wumacat_file (str): Path to WUMaCat cross-matched with Gaia ECSV file
        debcat_file (str): Path to debcat cross-matched with Gaia ECSV file
        teff_threshold (float): Minimum catalog Teff to include in fit (K)
        poly_degree (int): Degree of polynomial to fit
        output_file (str): File to save polynomial coefficients

    Returns:
        Polynomial: Fitted polynomial object
    """

    # Read the tables
    print("Reading WUMaCat...")
    wumacat = Table.read(wumacat_file, format='ascii.ecsv')
    print(f"WUMaCat: {len(wumacat)} objects")

    print("Reading debcat...")
    debcat = Table.read(debcat_file, format='ascii.ecsv')
    print(f"debcat: {len(debcat)} objects")

    # Collect hot star measurements using AVERAGE temperatures
    # WUMaCat - filter out NaN and zero values, average Teff must be >= threshold
    wuma_mask_avg = (~np.isnan(wumacat['teff_gspphot'])) & \
                    (wumacat['teff_gspphot'] > 0) & \
                    (~np.isnan(wumacat['T1'])) & (wumacat['T1'] > 0) & \
                    (~np.isnan(wumacat['T2'])) & (wumacat['T2'] > 0)

    wuma_cat_teff_avg = (wumacat['T1'][wuma_mask_avg] + wumacat['T2'][wuma_mask_avg]) / 2
    wuma_gaia_teff_avg = wumacat['teff_gspphot'][wuma_mask_avg]

    # Filter by threshold on average temperature
    wuma_hot_mask = wuma_cat_teff_avg >= teff_threshold
    wuma_cat_teff = np.array(wuma_cat_teff_avg[wuma_hot_mask])
    wuma_gaia_teff = np.array(wuma_gaia_teff_avg[wuma_hot_mask])

    # debcat (convert from log) - filter out NaN and zero values, average Teff must be >= threshold
    deb_mask_avg = (~np.isnan(debcat['teff_gspphot'])) & \
                   (debcat['teff_gspphot'] > 0) & \
                   (~np.isnan(debcat['logT1'])) & (debcat['logT1'] > 0) & \
                   (~np.isnan(debcat['logT2'])) & (debcat['logT2'] > 0)

    deb_cat_teff_avg = (10**debcat['logT1'][deb_mask_avg] + 10**debcat['logT2'][deb_mask_avg]) / 2
    deb_gaia_teff_avg = debcat['teff_gspphot'][deb_mask_avg]

    # Filter by threshold on average temperature
    deb_hot_mask = deb_cat_teff_avg >= teff_threshold
    deb_cat_teff = np.array(deb_cat_teff_avg[deb_hot_mask])
    deb_gaia_teff = np.array(deb_gaia_teff_avg[deb_hot_mask])

    # Combine all data
    cat_teff_all = np.concatenate([wuma_cat_teff, deb_cat_teff])
    gaia_teff_all = np.concatenate([wuma_gaia_teff, deb_gaia_teff])

    print(f"\nHot stars (Average Teff >= {teff_threshold}K):")
    print(f"  WUMaCat: {np.sum(wuma_hot_mask)} systems")
    print(f"  debcat: {np.sum(deb_hot_mask)} systems")
    print(f"  Total: {len(cat_teff_all)} systems")

    if len(cat_teff_all) == 0:
        print(f"\nNo stars found with Teff >= {teff_threshold}K. Cannot fit correction.")
        return None

    # Fit polynomial: gaia_teff = f(cat_teff)
    # We want to find the correction such that: cat_teff_corrected = g(gaia_teff)
    p_fit = Polynomial.fit(gaia_teff_all, cat_teff_all, poly_degree)

    print(f"\nFitted polynomial (degree {poly_degree}):")
    print(f"  Catalog_Teff = f(Gaia_Teff)")
    print(f"  Coefficients: {p_fit.coef}")

    # Calculate residuals
    predicted_cat_teff = p_fit(gaia_teff_all)
    residuals = cat_teff_all - predicted_cat_teff

    print(f"\nFit statistics:")
    print(f"  Mean residual: {np.mean(residuals):.1f} K")
    print(f"  Std residual: {np.std(residuals):.1f} K")
    print(f"  RMS: {np.sqrt(np.mean(residuals**2)):.1f} K")

    # Save coefficients
    correction_data = {
        'polynomial': p_fit,
        'degree': poly_degree,
        'teff_threshold': teff_threshold,
        'n_points': len(cat_teff_all),
        'rms': np.sqrt(np.mean(residuals**2))
    }

    with open(output_file, 'wb') as f:
        pickle.dump(correction_data, f)
    print(f"\nSaved correction polynomial to: {output_file}")

    # Plot the fit
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))

    # Left plot: Gaia vs Catalog with fit
    ax = axes[0]
    ax.scatter(cat_teff_all, gaia_teff_all, alpha=0.5, s=20, c='gray', label='Data')

    # Create smooth curve for the fit
    cat_range = np.linspace(np.min(cat_teff_all), np.max(cat_teff_all), 100)
    # Inverse relationship: given catalog Teff, what Gaia would measure
    p_inverse = Polynomial.fit(cat_teff_all, gaia_teff_all, poly_degree)
    gaia_fit = p_inverse(cat_range)

    ax.plot(cat_range, gaia_fit, 'r-', linewidth=2, label=f'Fit (deg {poly_degree})')
    ax.plot([np.min(cat_teff_all), np.max(cat_teff_all)],
            [np.min(cat_teff_all), np.max(cat_teff_all)],
            'k--', alpha=0.5, label='1:1 line')

    ax.set_xlabel('Catalog Average Teff [(T1+T2)/2] [K]', fontsize=12)
    ax.set_ylabel('Gaia GSP-Phot Teff [K]', fontsize=12)
    ax.set_title(f'Hot Stars (Average Teff ≥ {teff_threshold}K)', fontsize=13)
    ax.legend()
    ax.grid(True, alpha=0.3)
    ax.set_aspect('equal', adjustable='box')

    # Right plot: Residuals
    ax = axes[1]
    ax.scatter(cat_teff_all, residuals, alpha=0.5, s=20, c='gray')
    ax.axhline(0, color='red', linestyle='--', linewidth=2, label='Zero residual')
    ax.axhline(np.mean(residuals), color='green', linestyle='-', linewidth=2,
               label=f'Mean: {np.mean(residuals):.1f} K')

    ax.set_xlabel('Catalog Average Teff [(T1+T2)/2] [K]', fontsize=12)
    ax.set_ylabel('Residual (Catalog - Predicted) [K]', fontsize=12)
    ax.set_title('Fit Residuals', fontsize=13)
    ax.legend()
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    plot_filename = f'./results/teff_correction/teff_correction_fit_deg{poly_degree}.png'
    plt.savefig(plot_filename, dpi=150)
    print(f"Saved fit plot to: {plot_filename}")
    plt.close()

    return p_fit


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Fit polynomial correction for Gaia Teff')
    parser.add_argument('--degree', type=int, default=2,
                       help='Polynomial degree (default: 2)')
    parser.add_argument('--threshold', type=float, default=10000,
                       help='Minimum catalog Teff in K (default: 10000)')
    parser.add_argument('--wumacat', type=str, default="./data/WUMaCat_cross_with_gaia.ecsv",
                       help='Path to WUMaCat ECSV file')
    parser.add_argument('--debcat', type=str, default="./data/debcat_cross_with_gaia.ecsv",
                       help='Path to debcat ECSV file')
    parser.add_argument('--output', type=str, default=None,
                       help='Output pickle file (default: auto-generated based on degree)')

    args = parser.parse_args()

    # Auto-generate output filename if not provided
    if args.output is None:
        args.output = f"./results/teff_correction/teff_correction_coeffs_deg{args.degree}.pkl"

    print("="*60)
    print(f"POLYNOMIAL FIT (degree {args.degree})")
    print("="*60)
    print(f"Temperature threshold: {args.threshold}K")
    print(f"Output file: {args.output}")
    print("="*60 + "\n")

    poly = fit_teff_correction(args.wumacat, args.debcat,
                              teff_threshold=args.threshold,
                              poly_degree=args.degree,
                              output_file=args.output)
