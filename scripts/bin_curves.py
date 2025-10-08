import os, sys
from astropy.io import ascii
from astropy.table import Table
import numpy as np
import matplotlib.pyplot as plt
from scipy.stats import binned_statistic
from scipy.interpolate import CubicSpline
import pandas as pd
from scipy.ndimage import gaussian_filter
from scipy.signal import savgol_filter



def exponential_moving_average(data, alpha=0.3):
    ema = np.zeros_like(data)
    ema[:] = np.nan  # Initialize with NaN
    for i in range(len(data)):
        if i == 0:
            ema[i] = data[i] if not np.isnan(data[i]) else 0
        elif not np.isnan(data[i]):
            ema[i] = alpha * data[i] + (1 - alpha) * ema[i - 1]
        else:
            ema[i] = ema[i - 1]  # Carry forward the last valid EMA
    return ema

# Apply exponential moving average

list_dir=[ 'smc/', 'lmc/', 'bulge/']

bins=100

input_dir='/home/parimucha/Virtual/OGLE/Processed/'
out_dir='/home/parimucha/Virtual/OGLE/Binned/'
out_images='/home/parimucha/Virtual/OGLE/Images_binned/'

for inp_dir in list_dir[2:]:
	input_dir=input_dir+inp_dir
	curves=os.listdir(input_dir)

	curves=sorted(curves)


	for curve in curves[200000:]:
		full_name=input_dir+curve
		binned_name=out_dir+inp_dir+curve
		binned_name=binned_name.replace('.ecvs','_b.ecvs')
		image_name=out_images+inp_dir+curve
		image_name=image_name.replace('.ecvs', '_b.png')
		print(image_name)
		if os.path.isfile(image_name):
			print('old')
		else:
			data=ascii.read(full_name)
		# print(data)
			phase=data['Phase']
			flux=data['norm_Flux_I']
			err=data['err_norm_Flux_I']

			ph=binned_statistic(phase,phase,statistic='median',bins=bins,range=[-0.5/bins,1+0.5/bins])[0]
			fl=binned_statistic(phase,flux,statistic='median',bins=bins,range=[-0.5/bins,1+0.5/bins])[0]
			err_fl=binned_statistic(phase,err,statistic='median',bins=bins,range=[-0.5/bins,1+0.5/bins])[0]

			new_phase=np.linspace(0, 1, 100)
			new_data=np.interp(new_phase, ph[~np.isnan(fl)], fl[~np.isnan(fl)])

			binned_data=Table()
			binned_data['Phase']=ph
			binned_data['Phase'].format= '.4f'
			binned_data['Flux']=fl
			binned_data['Flux'].format= '.3f'
			binned_data['err_Flux']=err_fl
			binned_data['err_Flux'].format= '.3f'
			binned_data.meta=data.meta
		# print(binned_data)
			ascii.write(binned_data, binned_name, overwrite=True,  format='ecsv')

			fig, ax = plt.subplots()
			fig.set_size_inches((6,4))
        # ax.scatter(tab['Phase'], tab['norm_FG'], s=2, c='black', cmap='gray')
        # ax.scatter(tab_orig['Phase'], tab_orig['norm_Flux_I'], s=2, c=tab_orig['err_norm_Flux_I'], cmap='Greens', norm=norm)
			ax.scatter(data['Phase'], data['norm_Flux_I'], s=2, c='Black')
			ax.scatter(ph, fl,s=5, c='Red')
		# ax.scatter(ph,smoothed_data, c='Blue')
		# ax.plot
        # cbar = plt.colorbar(sc)
        # fig.colorbar(ax, ax=axs
			plt.savefig(image_name)
			plt.close()
		# plt.show()
