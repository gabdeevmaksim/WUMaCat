import matplotlib.pyplot as plt
import numpy as np
from astropy.table import Table

def compare_teff(wumacat_file, debcat_file, output_prefix="teff_comparison",
                teff_min=None, teff_max=None):
    """
    Compare Gaia GSP-Phot Teff measurements with catalog Teff values.

    Args:
        wumacat_file (str): Path to WUMaCat cross-matched with Gaia ECSV file
        debcat_file (str): Path to debcat cross-matched with Gaia ECSV file
        output_prefix (str): Prefix for output plot files
        teff_min (float): Minimum average catalog Teff to include (None = no limit)
        teff_max (float): Maximum average catalog Teff to include (None = no limit)
    """

    # Read the tables
    print("Reading WUMaCat...")
    wumacat = Table.read(wumacat_file, format='ascii.ecsv')
    print(f"WUMaCat: {len(wumacat)} objects")

    print("Reading debcat...")
    debcat = Table.read(debcat_file, format='ascii.ecsv')
    print(f"debcat: {len(debcat)} objects")

    # Filter out objects with valid Teff measurements
    # WUMaCat: T1 and T2 are already in Kelvin
    # debcat: logT1 and logT2 need to be converted (10^logT)
    # IMPORTANT: Filter out zero values in teff_gspphot
    # Also apply temperature range filter based on average Teff

    # First, get average temperatures for filtering
    wuma_mask_for_avg = (~np.isnan(wumacat['teff_gspphot'])) & (wumacat['teff_gspphot'] > 0) & \
                        (~np.isnan(wumacat['T1'])) & (wumacat['T1'] > 0) & \
                        (~np.isnan(wumacat['T2'])) & (wumacat['T2'] > 0)
    wuma_avg_for_filter = (wumacat['T1'][wuma_mask_for_avg] + wumacat['T2'][wuma_mask_for_avg]) / 2

    deb_mask_for_avg = (~np.isnan(debcat['teff_gspphot'])) & (debcat['teff_gspphot'] > 0) & \
                       (~np.isnan(debcat['logT1'])) & (debcat['logT1'] > 0) & \
                       (~np.isnan(debcat['logT2'])) & (debcat['logT2'] > 0)
    deb_avg_for_filter = (10**debcat['logT1'][deb_mask_for_avg] + 10**debcat['logT2'][deb_mask_for_avg]) / 2

    # Apply temperature range filter on averages
    wuma_temp_filter = np.ones(len(wuma_avg_for_filter), dtype=bool)
    if teff_min is not None:
        wuma_temp_filter &= (wuma_avg_for_filter >= teff_min)
    if teff_max is not None:
        wuma_temp_filter &= (wuma_avg_for_filter < teff_max)

    deb_temp_filter = np.ones(len(deb_avg_for_filter), dtype=bool)
    if teff_min is not None:
        deb_temp_filter &= (deb_avg_for_filter >= teff_min)
    if teff_max is not None:
        deb_temp_filter &= (deb_avg_for_filter < teff_max)

    # WUMaCat - Primary star (T1) with temperature filter
    wuma_teff_gsp1 = wumacat['teff_gspphot'][wuma_mask_for_avg][wuma_temp_filter]
    wuma_t1 = wumacat['T1'][wuma_mask_for_avg][wuma_temp_filter]

    # WUMaCat - Secondary star (T2) with temperature filter
    wuma_teff_gsp2 = wumacat['teff_gspphot'][wuma_mask_for_avg][wuma_temp_filter]
    wuma_t2 = wumacat['T2'][wuma_mask_for_avg][wuma_temp_filter]

    # debcat - Primary star (10^logT1) with temperature filter
    deb_teff_gsp1 = debcat['teff_gspphot'][deb_mask_for_avg][deb_temp_filter]
    deb_t1 = 10**debcat['logT1'][deb_mask_for_avg][deb_temp_filter]

    # debcat - Secondary star (10^logT2) with temperature filter
    deb_teff_gsp2 = debcat['teff_gspphot'][deb_mask_for_avg][deb_temp_filter]
    deb_t2 = 10**debcat['logT2'][deb_mask_for_avg][deb_temp_filter]

    print(f"\nValid measurements:")
    print(f"WUMaCat - T1: {len(wuma_t1)} objects")
    print(f"WUMaCat - T2: {len(wuma_t2)} objects")
    print(f"debcat - T1: {len(deb_t1)} objects")
    print(f"debcat - T2: {len(deb_t2)} objects")

    # Plot 1: Teff_gspphot vs T1 (Primary star)
    fig, ax = plt.subplots(figsize=(10, 8))

    # Plot WUMaCat
    ax.scatter(wuma_t1, wuma_teff_gsp1, alpha=0.6, s=30, c='blue',
               label=f'WUMaCat T1 (n={len(wuma_t1)})', edgecolors='none')

    # Plot debcat
    ax.scatter(deb_t1, deb_teff_gsp1, alpha=0.6, s=30, c='red',
               label=f'debcat T1 (n={len(deb_t1)})', edgecolors='none')

    # Add 1:1 line
    all_temps1 = np.concatenate([np.array(wuma_t1), np.array(deb_t1),
                                 np.array(wuma_teff_gsp1), np.array(deb_teff_gsp1)])
    min_temp1 = np.min(all_temps1)
    max_temp1 = np.max(all_temps1)
    ax.plot([min_temp1, max_temp1], [min_temp1, max_temp1], 'k--',
            alpha=0.5, linewidth=1, label='1:1 line')

    ax.set_xlabel('Catalog Teff (Primary, T1) [K]', fontsize=12)
    ax.set_ylabel('Gaia GSP-Phot Teff [K]', fontsize=12)
    ax.set_title('Comparison: Gaia Teff vs Catalog Teff (Primary Stars)', fontsize=14)
    ax.legend(loc='upper left', fontsize=10)
    ax.grid(True, alpha=0.3)
    ax.set_aspect('equal', adjustable='box')

    plt.tight_layout()
    plt.savefig(f'{output_prefix}_T1.png', dpi=150)
    print(f"\nSaved: {output_prefix}_T1.png")
    plt.close()

    # Plot 2: Teff_gspphot vs T2 (Secondary star)
    fig, ax = plt.subplots(figsize=(10, 8))

    # Plot WUMaCat
    ax.scatter(wuma_t2, wuma_teff_gsp2, alpha=0.6, s=30, c='blue',
               label=f'WUMaCat T2 (n={len(wuma_t2)})', edgecolors='none')

    # Plot debcat
    ax.scatter(deb_t2, deb_teff_gsp2, alpha=0.6, s=30, c='red',
               label=f'debcat T2 (n={len(deb_t2)})', edgecolors='none')

    # Add 1:1 line
    all_temps2 = np.concatenate([np.array(wuma_t2), np.array(deb_t2),
                                 np.array(wuma_teff_gsp2), np.array(deb_teff_gsp2)])
    min_temp2 = np.min(all_temps2)
    max_temp2 = np.max(all_temps2)
    ax.plot([min_temp2, max_temp2], [min_temp2, max_temp2], 'k--',
            alpha=0.5, linewidth=1, label='1:1 line')

    ax.set_xlabel('Catalog Teff (Secondary, T2) [K]', fontsize=12)
    ax.set_ylabel('Gaia GSP-Phot Teff [K]', fontsize=12)
    ax.set_title('Comparison: Gaia Teff vs Catalog Teff (Secondary Stars)', fontsize=14)
    ax.legend(loc='upper left', fontsize=10)
    ax.grid(True, alpha=0.3)
    ax.set_aspect('equal', adjustable='box')

    plt.tight_layout()
    plt.savefig(f'{output_prefix}_T2.png', dpi=150)
    print(f"Saved: {output_prefix}_T2.png")
    plt.close()

    # Plot 3: Teff_gspphot vs Average Teff
    fig, ax = plt.subplots(figsize=(10, 8))

    # Calculate average temperatures for WUMaCat
    wuma_mask_avg = (~np.isnan(wumacat['teff_gspphot'])) & (wumacat['teff_gspphot'] > 0) & \
                    (~np.isnan(wumacat['T1'])) & (wumacat['T1'] > 0) & \
                    (~np.isnan(wumacat['T2'])) & (wumacat['T2'] > 0)
    wuma_teff_gsp_avg_all = wumacat['teff_gspphot'][wuma_mask_avg]
    wuma_t_avg_all = (wumacat['T1'][wuma_mask_avg] + wumacat['T2'][wuma_mask_avg]) / 2

    # Apply temperature range filter
    wuma_range_mask = np.ones(len(wuma_t_avg_all), dtype=bool)
    if teff_min is not None:
        wuma_range_mask &= (wuma_t_avg_all >= teff_min)
    if teff_max is not None:
        wuma_range_mask &= (wuma_t_avg_all < teff_max)

    wuma_teff_gsp_avg = wuma_teff_gsp_avg_all[wuma_range_mask]
    wuma_t_avg = wuma_t_avg_all[wuma_range_mask]

    # Calculate average temperatures for debcat
    deb_mask_avg = (~np.isnan(debcat['teff_gspphot'])) & (debcat['teff_gspphot'] > 0) & \
                   (~np.isnan(debcat['logT1'])) & (debcat['logT1'] > 0) & \
                   (~np.isnan(debcat['logT2'])) & (debcat['logT2'] > 0)
    deb_teff_gsp_avg_all = debcat['teff_gspphot'][deb_mask_avg]
    deb_t_avg_all = (10**debcat['logT1'][deb_mask_avg] + 10**debcat['logT2'][deb_mask_avg]) / 2

    # Apply temperature range filter
    deb_range_mask = np.ones(len(deb_t_avg_all), dtype=bool)
    if teff_min is not None:
        deb_range_mask &= (deb_t_avg_all >= teff_min)
    if teff_max is not None:
        deb_range_mask &= (deb_t_avg_all < teff_max)

    deb_teff_gsp_avg = deb_teff_gsp_avg_all[deb_range_mask]
    deb_t_avg = deb_t_avg_all[deb_range_mask]

    # Plot WUMaCat
    ax.scatter(wuma_t_avg, wuma_teff_gsp_avg, alpha=0.6, s=30, c='blue',
               label=f'WUMaCat avg (n={len(wuma_t_avg)})', edgecolors='none')

    # Plot debcat
    ax.scatter(deb_t_avg, deb_teff_gsp_avg, alpha=0.6, s=30, c='red',
               label=f'debcat avg (n={len(deb_t_avg)})', edgecolors='none')

    # Add 1:1 line
    all_temps_avg = np.concatenate([np.array(wuma_t_avg), np.array(deb_t_avg),
                                    np.array(wuma_teff_gsp_avg), np.array(deb_teff_gsp_avg)])
    min_temp_avg = np.min(all_temps_avg)
    max_temp_avg = np.max(all_temps_avg)
    ax.plot([min_temp_avg, max_temp_avg], [min_temp_avg, max_temp_avg], 'k--',
            alpha=0.5, linewidth=1, label='1:1 line')

    # Create title with temperature range info
    title = 'Comparison: Gaia Teff vs Catalog Average Teff'
    if teff_min is not None or teff_max is not None:
        if teff_min is not None and teff_max is not None:
            title += f'\n({teff_min}K ≤ Teff < {teff_max}K)'
        elif teff_min is not None:
            title += f'\n(Teff ≥ {teff_min}K)'
        else:
            title += f'\n(Teff < {teff_max}K)'

    ax.set_xlabel('Catalog Average Teff [(T1+T2)/2] [K]', fontsize=12)
    ax.set_ylabel('Gaia GSP-Phot Teff [K]', fontsize=12)
    ax.set_title(title, fontsize=14)
    ax.legend(loc='upper left', fontsize=10)
    ax.grid(True, alpha=0.3)
    ax.set_aspect('equal', adjustable='box')

    plt.tight_layout()
    plt.savefig(f'{output_prefix}_avg.png', dpi=150)
    print(f"Saved: {output_prefix}_avg.png")
    plt.close()

    # Plot 4: Distribution comparison (histogram) - T1 and T2
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))

    # Helper function for plotting distributions
    def plot_distribution(ax, cat_data, gaia_data, title, catalog_label):
        diff = np.array(gaia_data) - np.array(cat_data)

        ax.hist(diff, bins=50, alpha=0.7, edgecolor='black', linewidth=0.5)
        ax.axvline(0, color='red', linestyle='--', linewidth=2, label='Zero difference')
        ax.axvline(np.mean(diff), color='green', linestyle='-', linewidth=2,
                   label=f'Mean: {np.mean(diff):.1f} K')
        ax.axvline(np.median(diff), color='blue', linestyle='-', linewidth=2,
                   label=f'Median: {np.median(diff):.1f} K')

        ax.set_xlabel('Gaia Teff - Catalog Teff [K]', fontsize=10)
        ax.set_ylabel('Number of objects', fontsize=10)
        ax.set_title(title, fontsize=11)
        ax.legend(fontsize=8)
        ax.grid(True, alpha=0.3)

    # WUMaCat T1
    plot_distribution(axes[0, 0], wuma_t1, wuma_teff_gsp1,
                     f'WUMaCat T1 (n={len(wuma_t1)})', 'WUMaCat')

    # WUMaCat T2
    plot_distribution(axes[0, 1], wuma_t2, wuma_teff_gsp2,
                     f'WUMaCat T2 (n={len(wuma_t2)})', 'WUMaCat')

    # debcat T1
    plot_distribution(axes[1, 0], deb_t1, deb_teff_gsp1,
                     f'debcat T1 (n={len(deb_t1)})', 'debcat')

    # debcat T2
    plot_distribution(axes[1, 1], deb_t2, deb_teff_gsp2,
                     f'debcat T2 (n={len(deb_t2)})', 'debcat')

    plt.tight_layout()
    plt.savefig(f'{output_prefix}_distributions.png', dpi=150)
    print(f"Saved: {output_prefix}_distributions.png")
    plt.close()

    # Plot 5: Distribution comparison (histogram) - Average temperatures
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    # WUMaCat Average
    plot_distribution(axes[0], wuma_t_avg, wuma_teff_gsp_avg,
                     f'WUMaCat Average (n={len(wuma_t_avg)})', 'WUMaCat')

    # debcat Average
    plot_distribution(axes[1], deb_t_avg, deb_teff_gsp_avg,
                     f'debcat Average (n={len(deb_t_avg)})', 'debcat')

    plt.tight_layout()
    plt.savefig(f'{output_prefix}_distributions_avg.png', dpi=150)
    print(f"Saved: {output_prefix}_distributions_avg.png")
    plt.close()

    # Calculate and print statistics
    print("\n=== Statistics ===")

    def print_stats(catalog_name, component, cat_teff, gaia_teff):
        diff = gaia_teff - cat_teff
        rel_diff = (gaia_teff - cat_teff) / cat_teff * 100

        print(f"\n{catalog_name} - {component}:")
        print(f"  Mean difference (Gaia - Catalog): {np.mean(diff):.1f} K ({np.mean(rel_diff):.1f}%)")
        print(f"  Median difference: {np.median(diff):.1f} K ({np.median(rel_diff):.1f}%)")
        print(f"  Std deviation: {np.std(diff):.1f} K ({np.std(rel_diff):.1f}%)")
        print(f"  RMS: {np.sqrt(np.mean(diff**2)):.1f} K")

    if len(wuma_t1) > 0:
        print_stats("WUMaCat", "T1", wuma_t1, wuma_teff_gsp1)
    if len(wuma_t2) > 0:
        print_stats("WUMaCat", "T2", wuma_t2, wuma_teff_gsp2)
    if len(deb_t1) > 0:
        print_stats("debcat", "T1", deb_t1, deb_teff_gsp1)
    if len(deb_t2) > 0:
        print_stats("debcat", "T2", deb_t2, deb_teff_gsp2)
    if len(wuma_t_avg) > 0:
        print_stats("WUMaCat", "Avg", wuma_t_avg, wuma_teff_gsp_avg)
    if len(deb_t_avg) > 0:
        print_stats("debcat", "Avg", deb_t_avg, deb_teff_gsp_avg)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Compare catalog Teff with Gaia Teff')
    parser.add_argument('--wumacat', type=str, default="./data/WUMaCat_cross_with_gaia.ecsv",
                       help='Path to WUMaCat ECSV file')
    parser.add_argument('--debcat', type=str, default="./data/debcat_cross_with_gaia.ecsv",
                       help='Path to debcat ECSV file')
    parser.add_argument('--output', type=str, default=None,
                       help='Output prefix for plots (default: auto-generated based on temp range)')
    parser.add_argument('--teff-min', type=float, default=None,
                       help='Minimum average catalog Teff (K)')
    parser.add_argument('--teff-max', type=float, default=None,
                       help='Maximum average catalog Teff (K)')

    args = parser.parse_args()

    # Auto-generate output prefix if not provided
    if args.output is None:
        if args.teff_min is not None and args.teff_max is not None:
            args.output = f"./results/teff_comparison/teff_comparison_{int(args.teff_min)}-{int(args.teff_max)}K"
        elif args.teff_min is not None:
            args.output = f"./results/teff_comparison/teff_comparison_above{int(args.teff_min)}K"
        elif args.teff_max is not None:
            args.output = f"./results/teff_comparison/teff_comparison_below{int(args.teff_max)}K"
        else:
            args.output = "./results/teff_comparison/teff_comparison"

    print("="*60)
    print("TEFF COMPARISON")
    print("="*60)
    if args.teff_min is not None or args.teff_max is not None:
        if args.teff_min is not None and args.teff_max is not None:
            print(f"Temperature range: {args.teff_min}K ≤ Teff < {args.teff_max}K")
        elif args.teff_min is not None:
            print(f"Temperature range: Teff ≥ {args.teff_min}K")
        else:
            print(f"Temperature range: Teff < {args.teff_max}K")
    else:
        print("Temperature range: All temperatures")
    print(f"Output prefix: {args.output}")
    print("="*60 + "\n")

    compare_teff(args.wumacat, args.debcat, output_prefix=args.output,
                teff_min=args.teff_min, teff_max=args.teff_max)
