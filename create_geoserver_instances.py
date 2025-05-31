import os
import yaml
import requests
from urllib.parse import urljoin
from xml.sax.saxutils import escape
import xml.etree.ElementTree as ET


# -------- Load Configuration --------
def load_config(config_path=os.path.join("config", "geoserver_config.yaml")):
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    return config["prod_geoserver"]["url"], config["prod_geoserver"]["username"], config["prod_geoserver"]["password"]
GEOSERVER_URL, USERNAME, PASSWORD = load_config()


def create_workspace(workspace_name, uri=None):
    if not uri:
        uri = f"http://www.{workspace_name}.com"  

    url = f"{GEOSERVER_URL}workspaces"
    headers = {"Content-Type": "text/xml"}
    data = f"<workspace><name>{workspace_name}</name></workspace>"

    response = requests.post(url, data=data, headers=headers, auth=(USERNAME, PASSWORD))

    if response.status_code in [201, 200]:
        print(f"✅ Workspace '{workspace_name}' created successfully.")
    elif response.status_code == 401:
        print("❌ Unauthorized. Check your credentials.")
    elif response.status_code == 409:
        print(f"⚠️ Workspace '{workspace_name}' already exists.")
    else:
        print(f"❌ Failed to create workspace '{workspace_name}': {response.text}")
        return


# def create_or_update_shapefile_datastore(workspace, datastore, file_dir, enable_update=False):
#     datastore = datastore.replace(":", "_").replace(" ","_").replace(".","_")
#     headers = {"Content-type": "application/xml"}
#     base_url = f"{GEOSERVER_URL}workspaces/{workspace}/datastores/{datastore}"

#     payload = f"""
#     <dataStore>
#         <name>{datastore}</name>
#         <connectionParameters>
#             <entry key="url">file:{file_dir}</entry>
#             <entry key="namespace">http://{workspace}</entry>
#         </connectionParameters>
#         <type>Shapefile</type>
#     </dataStore>
#     """.strip()

#     # Check if datastore exists
#     response = requests.get(base_url, auth=(USERNAME, PASSWORD))
#     if response.status_code == 200:
#         if enable_update:
#             update_resp = requests.put(base_url, data=payload, auth=(USERNAME, PASSWORD), headers=headers)
#             if update_resp.status_code in [200, 201]:
#                 print(f"[↻] Datastore '{datastore}' in workspace '{workspace}' updated with new path.")
#             else:
#                 print(f"[✗] Failed to update datastore '{datastore}': {update_resp.status_code} {update_resp.text}")
#         else:
#             print(f"[✓] Datastore '{datastore}' already exists. Skipping update.")
#     elif response.status_code == 404:
#         # Datastore doesn't exist → create
#         create_url = f"{GEOSERVER_URL}workspaces/{workspace}/datastores"
#         create_resp = requests.post(create_url, data=payload, auth=(USERNAME, PASSWORD), headers=headers)
#         if create_resp.status_code in [200, 201]:
#             print(f"[+] Datastore '{datastore}' created in workspace '{workspace}'.")
#         else:
#             print(f"[✗] Failed to create datastore '{datastore}': {create_resp.status_code} {create_resp.text}")
#     else:
#         print(f"[✗] Error checking datastore '{datastore}': {response.status_code} {response.text}")

def create_or_update_shapefile_datastore(workspace, datastore, zip_file_path, enable_update):
    import time  # Optional: for short delay after deletion
    
    datastore = datastore.replace(":", "_").replace(" ", "_").replace(".", "_")
    headers = {"Content-type": "application/zip"}
    datastore_url = f"{GEOSERVER_URL}workspaces/{workspace}/datastores/{datastore}"
    upload_url = f"{datastore_url}/file.shp"

    response = requests.get(datastore_url, auth=(USERNAME, PASSWORD))
    
    if response.status_code == 200:
        
        print(f"[✘] Datastore '{datastore}' exists. Deleting it...")
        delete_url = f"{datastore_url}?recurse=true"
        del_resp = requests.delete(delete_url, auth=(USERNAME, PASSWORD))
        if del_resp.status_code not in [200, 202]:
            print(f"[✗] Failed to delete datastore: {del_resp.status_code} {del_resp.text}")
            return
        print(f"[✓] Datastore '{datastore}' deleted.")
        time.sleep(2)  # Optional short wait for GeoServer to finalize deletion

    elif response.status_code != 404:
        print(f"[✗] Error checking datastore '{datastore}': {response.status_code} {response.text}")
        return

    print(f"[+] Creating datastore '{datastore}' by uploading shapefile...")
    with open(zip_file_path, 'rb') as f:
        post_resp = requests.put(upload_url, data=f, headers=headers, auth=(USERNAME, PASSWORD))
    if post_resp.status_code in [200, 201]:
        print(f"[✓] Datastore '{datastore}' created and shapefile uploaded.")
    else:
        print(f"[✗] Failed to create datastore: {post_resp.status_code} {post_resp.text}")


def update_shapefile_layername(workspace_name, datastore_name, search_name, standard_layer_name):
    featuretype_url = urljoin(
    GEOSERVER_URL,
    f"workspaces/{workspace_name}/datastores/{datastore_name}/featuretypes/{search_name}"
    )
    print(featuretype_url)
    headers = {"Content-Type": "text/xml"}

    check_response = requests.get(
        featuretype_url,
        auth=(USERNAME, PASSWORD)
    )

    if check_response.status_code == 200:
        update_payload = f"""
        <featureType>
        <name>{standard_layer_name}</name>
        </featureType>
        """
        update_response = requests.put(
            featuretype_url,
            data=update_payload,
            headers=headers,
            auth=(USERNAME, PASSWORD)
        )

        if update_response.status_code in [200, 201]:
            print(f"[✓] Layer name updated to '{standard_layer_name}'.")
        else:
            print(f"[✗] Failed to update layer: {update_response.status_code} {update_response.text}")


def create_layer_from_datastore(workspace, datastore, search_name, standard_layer_name, enable_update=False):
    datastore = datastore.replace(":", "_").replace(" ","_").replace(".","_")
    featuretypes_url = f"{GEOSERVER_URL}workspaces/{workspace}/datastores/{datastore}/featuretypes"
    layer_url = f"{featuretypes_url}/{standard_layer_name}"
    headers = {"Content-type": "text/xml"}
    payload = f"""
    <featureType>
        <name>{standard_layer_name}</name>
        <nativeName>{search_name}</nativeName>
    </featureType>
    """

    # First check if the layer exists
    check_response = requests.get(layer_url, auth=(USERNAME, PASSWORD))

    if check_response.status_code == 200:
        print(f"[!] Layer '{standard_layer_name}' already exists.")
        if enable_update:
            update_response = requests.put(layer_url, data=payload.strip(), auth=(USERNAME, PASSWORD), headers=headers)
            if update_response.status_code in [200, 201]:
                print(f"[↻] Layer '{standard_layer_name}' updated successfully.")
            else:
                print(f"[✗] Failed to update layer '{standard_layer_name}': {update_response.status_code}\n{update_response.text}")
        else:
            print(f"[!] Skipping update for layer '{standard_layer_name}'.")
    elif check_response.status_code == 404:
        # Layer does not exist, so create it
        create_response = requests.post(featuretypes_url, data=payload.strip(), auth=(USERNAME, PASSWORD), headers=headers)
        if create_response.status_code in [201, 200]:
            print(f"[✓] Layer '{standard_layer_name}' created successfully.")
        else:
            print(f"[✗] Failed to create layer '{standard_layer_name}': {create_response.status_code}\n{create_response.text}")
    else:
        print(f"[✗] Failed to check layer existence: {check_response.status_code}\n{check_response.text}")


def workspace_exists(workspace):
    url = urljoin(GEOSERVER_URL, f"workspaces/{workspace}")
    response = requests.get(url, auth=(USERNAME, PASSWORD))
    return response.status_code == 200

def create_or_update_wms_datastore(workspace, datastore, wms_url,
                                   use_connection_pooling=True,
                                   max_connections=6,
                                   connect_timeout=30,
                                   read_timeout=60,
                                   username=None,
                                   password=None,
                                   enable_update=False):

    headers = {"Content-Type": "text/xml"}
    
    conn_pooling = "true" if use_connection_pooling else "false"
    escaped_url = escape(wms_url)

    # Optional auth tags (if provided)
    user_tag = f"<userName>{username}</userName>" if username else ""
    pass_tag = f"<password>{password}</password>" if password else ""

    payload = f"""<wmsStore>
  <name>{datastore}</name>
  <type>WMS</type>
  <enabled>true</enabled>
  <workspace>
    <name>{workspace}</name>
  </workspace>
  <capabilitiesURL>{escaped_url}</capabilitiesURL>
  {user_tag}
  {pass_tag}
  <maxConnections>{max_connections}</maxConnections>
  <connectTimeout>{connect_timeout}</connectTimeout>
  <readTimeout>{read_timeout}</readTimeout>
  <useConnectionPooling>{conn_pooling}</useConnectionPooling>
</wmsStore>""".strip()

    # Check if the datastore exists
    check_url = f"{GEOSERVER_URL}workspaces/{workspace}/wmsstores/{datastore}"
    response = requests.get(check_url, auth=(USERNAME, PASSWORD))

    if response.status_code == 200:
        if enable_update:
            # Update
            response = requests.put(check_url, data=payload, headers=headers, auth=(USERNAME, PASSWORD))
            print(response.status_code, "Update datastore.")
            if response.status_code in [200, 201]:
                print(f"[✓] Updated WMS datastore '{datastore}'.")
            else:
                raise Exception(f"[✗] Failed to update datastore: {response.status_code} -\n{response.text}")
        else:
            print(f"[↷] WMS datastore '{datastore}' already exists. Skipping update.")
    elif response.status_code == 404:
        # Create
        create_url = f"{GEOSERVER_URL}workspaces/{workspace}/wmsstores"
        response = requests.post(create_url, data=payload, headers=headers, auth=(USERNAME, PASSWORD))
        print(response.status_code, "Create datastore.")
        if response.status_code in [200, 201]:
            print(f"[✓] Created WMS datastore '{datastore}'.")
        else:
            raise Exception(f"[✗] Failed to create datastore: {response.status_code} -\n{response.text}")
    else:
        raise Exception(f"[✗] Unexpected response: {response.status_code} -\n{response.text}")


# def layer_exists(workspace, layer_name):
#     url = f"{GEOSERVER_URL}layers/{workspace}:{layer_name}"
#     response = requests.get(url, auth=(USERNAME, PASSWORD))
#     print(response.status_code)
#     return response.status_code == 200


def layer_exists(workspace, layer_name):
    url = f"{GEOSERVER_URL}layers/{layer_name}.xml"
    response = requests.get(url, auth=(USERNAME, PASSWORD))
    return response.status_code == 200

def delete_wms_layer(workspace, datastore, layer_name):
    # Step 1: Unpublish the layer (from catalog)
    unpublish_url = f"{GEOSERVER_URL}layers/{layer_name}"
    response1 = requests.delete(unpublish_url, auth=(USERNAME, PASSWORD))
    if response1.status_code not in [200, 202, 204]:
        print(f"[!] Failed to unpublish: {response1.status_code} - {response1.text}")
    
    # Step 2: Delete the resource from the store
    resource_url = f"{GEOSERVER_URL}workspaces/{workspace}/wmsstores/{datastore}/wmslayers/{layer_name}"
    response2 = requests.delete(resource_url, auth=(USERNAME, PASSWORD))
    if response2.status_code not in [200, 202, 204]:
        print(f"[!] Failed to delete WMS layer resource: {response2.status_code} - {response2.text}")
    else:
        print(f"[✓] Fully deleted layer '{layer_name}'.")


def wms_resource_exists(workspace, datastore, layer_name):
    url = f"{GEOSERVER_URL}workspaces/{workspace}/wmsstores/{datastore}/wmslayers/{layer_name}.xml"
    response = requests.get(url, auth=(USERNAME, PASSWORD))
    return response.status_code == 200

def create_or_update_wms_layer(workspace, datastore, layer_name, standard_layer_name, enable_update=False):
    # layer_name  =  layer_name.replace(" ", "_").replace(".", "_").replace(":", "_")
    # standard_layer_name =layer_name.replace(" ", "_").replace(".", "_").replace(":", "_")

    if wms_resource_exists(workspace, datastore, standard_layer_name):
        if enable_update:
            print("Deleting existing WMS layer and resource.")
            delete_wms_layer( workspace, datastore, standard_layer_name)
        else:
            print(f"[↷] WMS layer '{layer_name}' already exists. Skipping update.")
            return

    # Step 3: Recreate the WMS layer
    print("Creating new WMS layer.")
    create_url = f"{GEOSERVER_URL}workspaces/{workspace}/wmsstores/{datastore}/wmslayers"
    headers = {"Content-type": "text/xml"}
    payload = f"""
    <wmsLayer>
        <name>{standard_layer_name}</name>
        <nativeName>{layer_name}</nativeName>
    </wmsLayer>
    """
    response = requests.post(create_url, data=payload.strip(), headers=headers, auth=(USERNAME, PASSWORD))
    if response.status_code in [200, 201]:
        print(f"[✓] Created WMS layer '{layer_name}'.")
    else:
        raise Exception(f"[✗] Failed to create layer: {response.status_code} - {response}")



def log(message, level="info"):
    print(f"[{level.upper()}] {message}")

def upload_and_assign_style(workspace, layer_name, style_name, sld_path):
    headers = {"Content-type": "application/vnd.ogc.sld+xml"}
    style_url = f"{GEOSERVER_URL}workspaces/{workspace}/styles"
    style_file_url = f"{GEOSERVER_URL}workspaces/{workspace}/styles/{style_name}"
    style_assign_url = f"{GEOSERVER_URL}layers/{workspace}:{layer_name}"
    
    # Step 1: Check if style exists
    style_list_url = f"{GEOSERVER_URL}workspaces/{workspace}/styles.json"
    resp = requests.get(style_list_url, auth=(USERNAME, PASSWORD))
    if resp.status_code != 200:
        log(f"Failed to fetch style list: {resp.status_code} {resp.text}", "error")
        return
    
    style_names = [s["name"] for s in resp.json().get("styles", {}).get("style", [])]
    style_exists = style_name in style_names
    
    # Step 2: Read and validate SLD XML
    try:
        ET.parse(sld_path)
    except ET.ParseError as e:
        log(f"Invalid SLD XML: {e}", "error")
        return
    
    with open(sld_path, 'rb') as sld_file:
        sld_content = sld_file.read()
    
    # print(sld_content)
    
    # Step 3: Upload or update style
    if not style_exists:
        log(f"Uploading new style '{style_name}' to workspace '{workspace}'", "info")
        
        # Create the style entry first
        create_style_payload = f"""
        <style>
            <name>{style_name}</name>
            <filename>{style_name}.sld</filename>
        </style>
        """
        create_headers = {"Content-type": "application/xml"}
        
        create_style = requests.post(
            style_url,
            data=create_style_payload,
            headers=create_headers,
            auth=(USERNAME, PASSWORD)
        )
        
        if create_style.status_code not in [200, 201]:
            log(f"Failed to create style entry: {create_style.status_code} {create_style.text}", "error")
            return
        
        # Then upload the actual SLD content
        upload = requests.put(
            f"{style_file_url}",
            data=sld_content,
            headers=headers,
            auth=(USERNAME, PASSWORD)
        )
        
        if upload.status_code not in [200, 201]:
            log(f"Failed to upload style: {upload.status_code} {upload.text}", "error")
            return
        
        log(f"Style '{style_name}' uploaded successfully.", "success")
    else:
        log(f"Style '{style_name}' already exists. Updating...", "info")
        
        # Update the actual SLD content
        update = requests.put(
            f"{style_file_url}",
            data=sld_content,
            headers=headers,
            auth=(USERNAME, PASSWORD)
        )
        
        if update.status_code not in [200, 201]:
            log(f"Failed to update style: {update.status_code} {update.text}", "error")
            return
        
        log(f"Style '{style_name}' updated successfully.", "success")
    
    # Step 4: Assign style to the layer
    assign_payload = f"""
    <layer>
        <defaultStyle>
            <name>{style_name}</name>
            <workspace>{workspace}</workspace>
        </defaultStyle>
        <styles>
            <style>
                <name>{style_name}</name>
                <workspace>{workspace}</workspace>
            </style>
        </styles>
    </layer>
    """.strip()
    
    assign_headers = {"Content-type": "application/xml"}
    assign = requests.put(
        style_assign_url,
        data=assign_payload,
        headers=assign_headers,
        auth=(USERNAME, PASSWORD)
    )
    
    if assign.status_code in [200, 201, 204]:
        log(f"Style '{style_name}' assigned to layer '{layer_name}'", "success")
    else:
        log(f"Failed to assign style (URL: {style_assign_url}): {assign.status_code} {assign.text}", "error")
