# python /code/GEE_download/vis_tiles.py

from glob import glob
import pandas as pd

import rasterio
from rasterio.plot import plotting_extent
import matplotlib.pyplot as plt
from tqdm import tqdm
import os

tifs = glob("/nas/houce/Alphaearth_embedding/GEE_extracted/Africa_grid_2_4_16_14/*.tif")
# target_tifs = glob(tifs[0].split("-")[0] + "-*.tif")
tifs_df = pd.DataFrame(tifs, columns=["tif_path"])
tifs_df['tif_name'] = tifs_df['tif_path'].apply(lambda x: x.split("-")[0].split("/")[-1])
tifs_df['tif_name'] = tifs_df['tif_name'].apply(lambda x: x[42:-4] if x.endswith('.tif') else x[42:])


def check_files_visualization(target_tifs, savepath, tif_name):
    all_bounds = []
    for tif in target_tifs: 
        with rasterio.open(tif) as src:
            bounds = src.bounds
            all_bounds.append(bounds)

    if len(all_bounds) == 0:
        print(f"No bounds found for {tif_name}. Skipping visualization.")
        return
    
    # Create a proper union bounds calculation
    union_bounds = rasterio.coords.BoundingBox(
        left=min(b.left for b in all_bounds),
        bottom=min(b.bottom for b in all_bounds),  # Fixed: should be min for bottom
        right=max(b.right for b in all_bounds),
        top=max(b.top for b in all_bounds)  # Fixed: should be max for top
    )

    # print("Union Bounds:", union_bounds)

    # Create a figure for visualization
    fig, ax = plt.subplots(figsize=(10, 10))

    # Plot each TIF file
    for i, tif in enumerate(tqdm(target_tifs)):

            
        with rasterio.open(tif) as src:
            bounds = src.bounds
            img = src.read(1)
            
            # Get the proper extent for plotting in matplotlib
            extent = plotting_extent(src)
            
            # Plot the image with the correct extent
            ax.imshow(img, extent=extent, cmap='viridis', alpha=0.3)
            
            # Plot the boundary as a rectangle
            ax.plot([bounds.left, bounds.right, bounds.right, bounds.left, bounds.left],
                    [bounds.bottom, bounds.bottom, bounds.top, bounds.top, bounds.bottom],
                    label=f'File {i+1}')
            
            # Add text label in the center of each boundary
            ax.text((bounds.left + bounds.right) / 2, 
                    (bounds.bottom + bounds.top) / 2,
                    f'TIF {i+1}',
                    ha='center', va='center', 
                    color='black', fontweight='bold')

    # Highlight the union bounds in a distinct color
    ax.plot([union_bounds.left, union_bounds.right, union_bounds.right, union_bounds.left, union_bounds.left],
            [union_bounds.bottom, union_bounds.bottom, union_bounds.top, union_bounds.top, union_bounds.bottom],
            color='red', linewidth=2, label='Union Bounds')

    # Set proper axis limits with some padding
    horizontal_padding = (union_bounds.right - union_bounds.left) * 0.03  # 3% padding
    vertical_padding = (union_bounds.top - union_bounds.bottom) * 0.03  # 3% padding
    ax.set_xlim(union_bounds.left - horizontal_padding, union_bounds.right + horizontal_padding)
    ax.set_ylim(union_bounds.bottom - vertical_padding, union_bounds.top + vertical_padding)

    # Add grid and legend for better readability
    ax.grid(True, linestyle='--', alpha=0.6)
    # ax.legend(loc='upper right')

    ax.set_title('Raster Images with Union Bounds')
    ax.set_xlabel('X Coordinate')
    ax.set_ylabel('Y Coordinate')
    # plt.tight_layout()
    plt.savefig(f'{savepath}/{tif_name}.png', dpi=300)
    
savepath = '/nas/houce/Alphaearth_embedding/merged_files_vis/Africa_grid_2_4_16_14/'
tif_name_list = tifs_df.drop_duplicates(subset='tif_name')['tif_name'].tolist()
for tif_name in tqdm(tif_name_list):
    savename = tif_name
    if os.path.exists(f'{savepath}/{tif_name}.png'):
        continue
    target_tifs = tifs_df[tifs_df['tif_name'] == tif_name]['tif_path'].tolist()
    try:
        check_files_visualization(target_tifs, savepath, savename)
    except Exception as e:
        print(f"Error processing {tif_name}: {e}")
        continue

print(f"Visualization complete. All files saved to {savepath}")