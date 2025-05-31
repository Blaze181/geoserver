from owslib.wfs import WebFeatureService
import geopandas as gpd
from io import BytesIO, StringIO

def fetch_wfs_layer(wfs_url, typename, output_format, version="1.0.0"):
    try:
        eaWFS = WebFeatureService(wfs_url, version=version)
        
        typename_exists = [i for i in eaWFS.contents.keys() if typename in i]
        
        if not typename_exists:
            print(f"[!] Layer '{typename}' not found in WFS.")
            return None

        response = eaWFS.getfeature(typename=typename_exists, outputFormat=output_format)

        if output_format == "csv":
            return StringIO(response.read().decode("utf-8"))
        elif output_format in ["GeoJSON", "json"]:
            return BytesIO(response.read())
        elif output_format == "SHAPE-ZIP":
            return BytesIO(response.read())  # ZIP shapefile binary stream
        else:
            print(f"[!] Unsupported output format: {output_format}")
            return None
    except Exception as e:
        print(f"[✗] Error fetching WFS layer '{typename}': {e}")
        return None
