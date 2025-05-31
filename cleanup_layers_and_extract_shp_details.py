import geopandas as gpd
import pandas as pd
import os
import tempfile
import zipfile
from io import StringIO, BytesIO
import shutil, requests

def format_and_save_geodataframe(data_stream, output_dir, filename_base, output_format):
    try:
        if output_format == "csv":
            df = pd.read_csv(data_stream)
            gdf_types = ["mt:shape", "erl:shape", "shape", "geom", "geometry"]
            geo_col = next((col for col in gdf_types if col in df.columns), None)

            if not geo_col:
                print(f"[!] No geometry column found. Skipping: {filename_base}")
                return

            df[geo_col] = gpd.GeoSeries.from_wkt(df[geo_col])
            gdf = gpd.GeoDataFrame(df, geometry=geo_col, crs="EPSG:4326")

        elif output_format in ["GeoJSON", "json"]:
            gdf = gpd.read_file(data_stream)

        elif output_format == "SHAPE-ZIP":
            with tempfile.TemporaryDirectory() as tmpdir:
                with zipfile.ZipFile(data_stream) as z:
                    z.extractall(tmpdir)
                shp_files = [f for f in os.listdir(tmpdir) if f.endswith(".shp")]
                if not shp_files:
                    print(f"[!] No .shp found in ZIP for {filename_base}")
                    return
                gdf = gpd.read_file(os.path.join(tmpdir, shp_files[0]))

        else:
            print(f"[!] Unsupported format for saving: {output_format}")
            return

        # Save shapefile to temp directory
        with tempfile.TemporaryDirectory() as temp_shp_dir:
            shp_path = os.path.join(temp_shp_dir, f"{filename_base}.shp")
            gdf.to_file(shp_path)

            # Create ZIP
            zip_output_path = os.path.join(output_dir, f"{filename_base}.zip")
            with zipfile.ZipFile(zip_output_path, "w", zipfile.ZIP_DEFLATED) as zipf:
                for ext in [".shp", ".shx", ".dbf", ".prj", ".cpg"]:
                    file_path = os.path.join(temp_shp_dir, f"{filename_base}{ext}")
                    if os.path.exists(file_path):
                        zipf.write(file_path, arcname=os.path.basename(file_path))

            print(f"[✓] Zipped and saved: {zip_output_path}")

    except Exception as e:
        print(f"[✗] Error formatting/saving {filename_base}: {e}")

def download_and_extract_omi(url, output_zip_full_path="final_output_files_v3/Ontario/omi.zip"):
    """
    Downloads a zip file from a given URL, extracts the contents of a specific
    nested folder ('OMI' inside 'Geospatial_Data' within 'OMI'), and then
    creates a new zip file containing only those contents, placed in a
    specified output directory.

    Args:
        url (str): The URL of the zip file to download.
        output_zip_full_path (str): The full path and name for the new zip file
                                    that will contain only the required OMI data.
                                    E.g., "final_output_files_v3/Ontario/omi.zip"
    """
    print(f"Attempting to download zip file from: {url}")

    # Define temporary paths for the full downloaded zip and extracted contents
    temp_download_path = "temp_full_download.zip"
    temp_extract_dir = "temp_extracted_omi_contents"
    # Target path prefix now points to the innermost 'OMI' folder
    target_zip_path_prefix = "Geospatial_Data/OMI/"

    try:
        # Step 1: Download the zip file content and save it to a temporary file
        print(f"Downloading full zip file to {temp_download_path}...")
        with requests.get(url, stream=True) as r:
            r.raise_for_status()  # Raise an HTTPError for bad responses (4xx or 5xx)
            with open(temp_download_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        print(f"Full zip file downloaded successfully to: {os.path.abspath(temp_download_path)}")

        # Step 2: Extract the specific nested OMI folder contents to a temporary directory
        os.makedirs(temp_extract_dir, exist_ok=True)
        print(f"Created temporary extraction directory: {os.path.abspath(temp_extract_dir)}")

        extracted_count = 0
        with zipfile.ZipFile(temp_download_path, 'r') as zf:
            print(f"Extracting contents of '{target_zip_path_prefix}' to temporary directory...")
            for member in zf.namelist():
                # Check if the file is inside the target nested OMI folder
                if member.startswith(target_zip_path_prefix):
                    # Get the relative path of the file within the target OMI folder
                    # This flattens the structure, placing contents directly into output_dir
                    relative_path = os.path.relpath(member, target_zip_path_prefix)

                    # Construct the full path for temporary extraction
                    dest_path = os.path.join(temp_extract_dir, relative_path)

                    # Ensure parent directories exist for the destination file
                    os.makedirs(os.path.dirname(dest_path), exist_ok=True)

                    # Extract the file (avoid extracting directories as files)
                    if not member.endswith('/'):
                        with zf.open(member) as source, open(dest_path, 'wb') as target:
                            target.write(source.read())
                        extracted_count += 1

        if extracted_count == 0:
            print(f"Warning: No files found matching the target path prefix: '{target_zip_path_prefix}' inside the downloaded zip file.")
            print("Please verify the exact path within the zip file if you expected files.")
            return # Exit if nothing was extracted

        print(f"Successfully extracted {extracted_count} relevant files to '{temp_extract_dir}'.")
        print(f"Now creating new zip file '{output_zip_full_path}' with only the required data...")

        # Step 3: Create the final output directory structure
        final_output_dir = os.path.dirname(output_zip_full_path)
        os.makedirs(final_output_dir, exist_ok=True)
        print(f"Created final output directory: {os.path.abspath(final_output_dir)}")


        # Step 4: Create a new zip file containing only the extracted contents
        with zipfile.ZipFile(output_zip_full_path, 'w', zipfile.ZIP_DEFLATED) as new_zip:
            # Walk through the temporary extracted directory and add files to the new zip
            for root, dirs, files in os.walk(temp_extract_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    # Calculate the arcname (path within the new zip) to remove the temporary directory prefix
                    # This ensures the 'OMI/' folder structure is preserved inside the new zip
                    arcname = os.path.relpath(file_path, temp_extract_dir)
                    new_zip.write(file_path, arcname)
        print(f"New zip file '{output_zip_full_path}' created successfully.")

    except requests.exceptions.RequestException as e:
        print(f"Error downloading the file: {e}")
    except zipfile.BadZipFile:
        print("Error: The downloaded file is not a valid zip file or is corrupted.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        # Step 5: Clean up temporary files and directories
        print("Cleaning up temporary files and directories...")
        if os.path.exists(temp_download_path):
            os.remove(temp_download_path)
            print(f"Removed temporary file: {temp_download_path}")
        if os.path.exists(temp_extract_dir):
            shutil.rmtree(temp_extract_dir)
            print(f"Removed temporary directory: {temp_extract_dir}")
        print("Cleanup complete.")