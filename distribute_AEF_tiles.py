# python /code/GEE_download/distribute_AEF_tiles.py

import os
import shutil
from glob import glob
import pandas as pd
import geopandas as gpd
import numpy as np
from shapely.geometry import Polygon
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed


class TileDistributor:
    """
    A class for distributing, organizing, and cleaning Alphaearth tile files (multithreaded version).
    """
    def __init__(self, base_path="/nas/houce/Alphaearth_embedding/", max_workers=8):
        """
        Initialize paths and configuration.
        Args:
            base_path (str): Root directory for Alphaearth data.
            max_workers (int): Maximum threads for parallel tasks.
        """
        self.base_path = base_path
        self.gee_extracted_path = os.path.join(base_path, "GEE_extracted")
        self.aef_tiles_path = os.path.join(base_path, "AEF_tiles")
        self.metadata_path = os.path.join(base_path, "metadata")
        self.max_workers = max_workers
        os.makedirs(self.metadata_path, exist_ok=True)

        self.all_download_file_gdf = None
        self.grid_gdf = None
        self.AEF_file_paths_all_merged = None

    def _read_csv_worker(self, file):
        """Read a single CSV file."""
        try:
            return pd.read_csv(file)
        except Exception as e:
            print(f"\nWarning: Failed to read file {file}: {e}")
            return None

    def _load_metadata(self):
        """Parallel loading of all metadata CSV files and conversion to GeoDataFrame."""
        print("Loading metadata in parallel...")
        meta_files = glob(os.path.join(self.gee_extracted_path, "*/metadata/*_grid_cells.csv"))
        
        if not meta_files:
            print("Error: No metadata files found.")
            return False

        df_list = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_file = {executor.submit(self._read_csv_worker, file): file for file in meta_files}
            for future in tqdm(as_completed(future_to_file), total=len(meta_files), desc="Reading metadata CSV"):
                df = future.result()
                if df is not None:
                    df_list.append(df)
        
        if not df_list:
            print("Error: No metadata loaded successfully.")
            return False

        all_download_file_df = pd.concat(df_list, axis=0, ignore_index=True)

        # --- Robust Data Cleaning and Conversion ---
        coord_cols = ['lon_min', 'lat_min', 'lon_max', 'lat_max']
        for col in coord_cols:
            all_download_file_df[col] = pd.to_numeric(all_download_file_df[col], errors='coerce')
        all_download_file_df.dropna(subset=coord_cols, inplace=True)
        all_download_file_df.reset_index(drop=True, inplace=True)

        lons_min = all_download_file_df['lon_min'].values
        lats_min = all_download_file_df['lat_min'].values
        lons_max = all_download_file_df['lon_max'].values
        lats_max = all_download_file_df['lat_max'].values

        polygons = []
        for idx, (lon_min, lat_min, lon_max, lat_max) in enumerate(zip(lons_min, lats_min, lons_max, lats_max)):
            try:
                poly = Polygon([
                    (lon_min, lat_min), (lon_min, lat_max),
                    (lon_max, lat_max), (lon_max, lat_min),
                    (lon_min, lat_min)
                ])
                polygons.append(poly)
            except Exception as e:
                print(f"Polygon creation failed at index {idx} with coords: lon_min={lon_min}, lat_min={lat_min}, lon_max={lon_max}, lat_max={lat_max}. Error: {e}")
                polygons.append(None)

        valid_idx = [i for i, p in enumerate(polygons) if p is not None]
        all_download_file_df = all_download_file_df.iloc[valid_idx].reset_index(drop=True)
        polygons = [p for p in polygons if p is not None]

        self.all_download_file_gdf = gpd.GeoDataFrame(all_download_file_df, geometry=polygons, crs="EPSG:4326")
        print(f"Successfully loaded {len(self.all_download_file_gdf)} metadata records.")
        return True

    def _create_grid(self):
        """Create global 5x5 degree grid."""
        print("Creating 5x5 degree grid...")
        lon_min, lat_min, lon_max, lat_max = [-180, -90, 180, 90]
        lon_bins = np.arange(np.floor(lon_min), np.ceil(lon_max), 5)
        lat_bins = np.arange(np.floor(lat_min), np.ceil(lat_max), 5)

        grid_polygons = []
        for lon1 in lon_bins:
            for lat1 in lat_bins:
                lon2, lat2 = lon1 + 5, lat1 + 5
                poly = Polygon([(lon1, lat1), (lon1, lat2), (lon2, lat2), (lon2, lat1), (lon1, lat1)])
                grid_polygons.append({'geometry': poly})

        self.grid_gdf = gpd.GeoDataFrame(grid_polygons, geometry='geometry', crs="EPSG:4326")
        self.grid_gdf['folder_name'] = self.grid_gdf.apply(
            lambda row: f"grid_{int(row.geometry.bounds[0])}_{int(row.geometry.bounds[1])}_{int(row.geometry.bounds[2])}_{int(row.geometry.bounds[3])}", axis=1
        )
        print("Grid creation completed.")

    def _process_and_merge_data(self):
        """Perform spatial join and data merging."""
        print("Performing spatial join and data merging...")
        grid_gdf_merged = self.grid_gdf[['folder_name', 'geometry']].sjoin(
            self.all_download_file_gdf, how='left', predicate='intersects'
        )

        AEF_file_paths_all = pd.DataFrame(glob(os.path.join(self.aef_tiles_path, "*.tif")), columns=['file_path'])
        AEF_transferred_file_paths = pd.DataFrame(glob(os.path.join(self.aef_tiles_path, "*/*/*.tif")), columns=['file_path_copied'])

        AEF_file_paths_all['grid_name'] = AEF_file_paths_all['file_path'].apply(lambda x: x.split("/")[-1][20:-26])
        AEF_transferred_file_paths['grid_name'] = AEF_transferred_file_paths['file_path_copied'].apply(lambda x: x.split("/")[-1][20:-26])
        AEF_transferred_file_paths['start_time'] = AEF_transferred_file_paths['file_path_copied'].apply(lambda x: x.split("_")[-2])
        AEF_transferred_file_paths['end_time'] = AEF_transferred_file_paths['file_path_copied'].apply(lambda x: x.split("_")[-1][:-4])

        self.AEF_file_paths_all_merged = grid_gdf_merged.merge(AEF_file_paths_all, on='grid_name', how='left')
        self.AEF_file_paths_all_merged = self.AEF_file_paths_all_merged.merge(AEF_transferred_file_paths, on='grid_name', how='left')
        
        output_geojson = os.path.join(self.metadata_path, "all_grid_cells_5x5_merged.geojson")
        self.AEF_file_paths_all_merged.to_file(output_geojson, driver='GeoJSON')
        print(f"Merged data saved to {output_geojson}")

    def _move_file_worker(self, row_tuple):
        """Move a single file task."""
        _, row = row_tuple
        src_path = row['file_path']
        folder_name = row['folder_name']
        year_folder = row['file_path'].split('_')[-2][:4]
        dest_dir = os.path.join(self.aef_tiles_path, year_folder, folder_name)
        
        os.makedirs(dest_dir, exist_ok=True)
        dest_path = os.path.join(dest_dir, os.path.basename(src_path))

        try:
            shutil.copy(src_path, dest_path)
            os.remove(src_path)
        except Exception as e:
            print(f"\nError: Failed to move {src_path} to {dest_path}: {e}")

    def _move_files(self):
        """Parallel move of unorganized tile files to their grid folders."""
        print("Checking and moving files in parallel...")
        files_to_move = self.AEF_file_paths_all_merged[
            self.AEF_file_paths_all_merged['file_path'].notna() & 
            self.AEF_file_paths_all_merged['file_path_copied'].isna()
        ].drop_duplicates(subset='file_path')

        if files_to_move.empty:
            print("No new files to move.")
            return

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(self._move_file_worker, row) for row in files_to_move.iterrows()]
            for future in tqdm(as_completed(futures), total=len(futures), desc="Moving files"):
                future.result()

    def _cleanup_dir_worker(self, path):
        """Clean a single empty directory task."""
        try:
            if not os.listdir(path):
                os.rmdir(path)
        except OSError as e:
            print(f"\nError: Failed to remove directory {path}: {e}")

    def _cleanup_empty_dirs(self):
        """Parallel cleanup of all empty subdirectories in GEE extracted path."""
        print("Cleaning empty directories in parallel...")
        all_dirs = [d for d in glob(os.path.join(self.gee_extracted_path, "*/*")) if os.path.isdir(d)]
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(self._cleanup_dir_worker, path) for path in all_dirs]
            for future in tqdm(as_completed(futures), total=len(futures), desc="Cleaning directories"):
                future.result()

    def run(self):
        """Run all steps in order."""
        if not self._load_metadata():
            return
        self._create_grid()
        self._process_and_merge_data()
        self._move_files()
        self._cleanup_empty_dirs()
        print("All tasks completed.")


if __name__ == "__main__":
    distributor = TileDistributor(max_workers=16)
    distributor.run()