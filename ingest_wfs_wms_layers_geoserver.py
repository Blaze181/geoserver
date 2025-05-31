from fetch_data_layers import fetch_wfs_layer
from create_geoserver_instances import (
    create_workspace,
    create_or_update_shapefile_datastore,
    create_layer_from_datastore,
    workspace_exists,
    create_or_update_wms_datastore,
    create_or_update_wms_layer,
    upload_and_assign_style,
    update_shapefile_layername
)
import os, json
import pandas as pd
import geopandas as gpd
from cleanup import format_and_save_geodataframe
from cleanup_layers_and_extract_shp_details import download_and_extract_omi
from datetime import datetime

def process_region_layers(region, layers, output_dir="final_output_files_v2"):
    log_entry_template =  {
        "timestamp": None,
        "region": region,
        "layer_name": None,
        "link": None,
        "layer_stream_fetched": False,
        "layer_processed": False,
        "workspace_created": False,
        "wfs_datastore_created": False,
        "wfs_datastore_updated": False,
        "wfs_layer_created": False,
        "wfs_layer_updated": False,
        "wms_datastore_created": False,
        "wms_datastore_updated": False,
        "wms_layer_created": False,
        "wms_layer_updated": False,
        "status": "success",
        "message": ""
    }

    region_dir = os.path.join(output_dir, region)
    os.makedirs(region_dir, exist_ok=True)

    enable_update = False
    workspace_name = f"{region}_v2"
    check_workspace = workspace_exists(workspace_name)
    workspace_created = not check_workspace
    if not check_workspace:
        create_workspace(workspace_name)
        enable_update = False
    else:
        enable_update = True
    # Track created WMS datastores for unique links
    wms_links_map = {}

    with open("geoserver_logs.jsonl", "a", encoding="utf-8") as jsonl_file:
        for layer in layers:
            log_entry = log_entry_template.copy()
            log_entry["timestamp"] = datetime.utcnow().isoformat()
            log_entry["workspace_created"] = workspace_created

            layer_name = layer["wfs_layer_name"]
            search_name = layer["wfs_layer_search_name"]
            link = layer["link"]
            output_format = layer.get("output_format", "")
            version = layer["version"]
            link_type = layer.get("link_type", "WFS").upper()
            standard_layer_name = layer["standard_layer_name"]

            print(f"\n[→] Processing layer: {search_name} ({link_type}) for region: {region}")

            try:
                if link_type == "WFS":
                    if region =="Ontario":
                        layer_dir = os.path.join(os.path.abspath(os.getcwd()), region_dir, search_name, )
                        # final_path = os.path.join(layer_dir, search_name)
                        # download_and_extract_omi(link, final_path +".zip")
                    else:
                        stream = fetch_wfs_layer(link, search_name, output_format, version=version)
                        if not stream:
                            log_entry["status"] = "error"
                            log_entry["message"] = f"No data stream returned for {search_name}"
                            jsonl_file.write(json.dumps(log_entry) + "\n")
                            continue

                        log_entry["layer_stream_fetched"] = True
                        search_name = search_name.replace(":", "_").replace(" ","_").replace(".","_")
                        layer_dir = os.path.join(os.path.abspath(os.getcwd()), region_dir, search_name)
                        os.makedirs(layer_dir, exist_ok=True)
                        format_and_save_geodataframe(stream, layer_dir, search_name, output_format)
                    shape_file_path = os.path.join(layer_dir,search_name+".zip")
                    print(shape_file_path)
                    log_entry["layer_processed"] = True

                    datastore_name = f"{search_name}_datastore"
                    create_or_update_shapefile_datastore(workspace_name, datastore_name, shape_file_path, enable_update)
                    if enable_update:
                        log_entry["wfs_datastore_updated"] = True
                        log_entry["wfs_layer_updated"] = True                 
                        log_entry["message"] = f"Updated WFS layer '{standard_layer_name}' processed successfully."
                    else:
                        log_entry["wfs_datastore_created"] = True
                        log_entry["wfs_layer_created"] = True
                        log_entry["message"] = f"Updated WFS layer '{standard_layer_name}' processed successfully."
                    update_shapefile_layername(workspace_name, datastore_name, search_name, standard_layer_name )

                    # create_layer_from_datastore(workspace_name, datastore_name,search_name, standard_layer_name, enable_update)
                    # log_entry["layer_name"] =standard_layer_name
                    # if enable_update:
                    #     log_entry["wfs_layer_updated"] = True                 
                    #     log_entry["message"] = f"Updated WFS layer '{standard_layer_name}' processed successfully."
                    # else:
                    #     log_entry["wfs_layer_created"] = True
                    #     log_entry["message"] = f"Updated WFS layer '{standard_layer_name}' processed successfully."    

                elif link_type == "WMS":
                    if link not in wms_links_map:
                        # Create a unique and consistent name for the datastore
                        hashed_suffix = abs(hash(link)) % 10**8  # Optional: shorten hash for readability
                        processed_search_name = search_name.replace(":", "_").replace(" ","_").replace(".","_")
                        datastore_name = f"{region.lower()}_wms_{processed_search_name}"
                        create_or_update_wms_datastore(workspace_name, datastore_name, link, enable_update=enable_update)
                        print("Done creating or updating wms datastore.")
                        wms_links_map[link] = datastore_name
                        if enable_update:
                            log_entry["wms_datastore_updated"] = True
                        else:
                            log_entry["wms_datastore_created"] = True
                    else:
                        datastore_name = wms_links_map[link]

                    create_or_update_wms_layer(workspace_name, datastore_name, search_name, standard_layer_name, enable_update)
                    log_entry["layer_name"] =search_name
                    if enable_update:
                        log_entry["wms_layer_updated"] = True
                        log_entry["message"] = f"Updated WMS layer '{search_name}' linked via datastore '{datastore_name}'."
                    else:
                        log_entry["wms_layer_created"] = True
                        log_entry["message"] = f"Created WMS layer '{search_name}' linked via datastore '{datastore_name}'."

                else:
                    raise ValueError(f"Unsupported link_type: {link_type}")

            except Exception as e:
                log_entry["status"] = "error"
                log_entry["message"] = str(e)
                print(f"[✗] Failed to process layer '{search_name}': {e}")

            # jsonl_file.write(json.dumps(log_entry) + "\n")

if __name__ =="__main__":
    jsonl_path = "final_output.jsonl"
    style_jsonl_path = "styles_path_details.jsonl"
    cmd = "layers"
    if cmd == "layers":
        with open(jsonl_path, encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                region_entry = json.loads(line)
                region = region_entry["region"]
                if region == "Ontario":
                    layers = region_entry.get("layers", [])
                    process_region_layers(region, layers)
    elif cmd == "styles":
        with open(style_jsonl_path, encoding="utf-8") as f:
            for line in f:
                record = json.loads(line)
                workspace, layer_name, style_name, sld_path = record["workspace"], record["layer"], record["style_name"], record["style_path"]
                print(workspace, layer_name, style_name, sld_path)
                upload_and_assign_style(workspace, layer_name, style_name, sld_path)