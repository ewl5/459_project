import os
import osmnx as ox
import geopandas as gpd
import pandas as pd

ox.settings.use_cache = True
ox.settings.log_console = False
ox.settings.timeout = 1800


def clean_name(s: str) -> str:
    # ensure file names are safe
    return s.replace(",", "").replace("/", "_").strip().replace(" ", "_")


def extract_amenities_for_place(place_name, tags_dict, output_folder):
    # ensure output folder exists and has a proper name
    output_folder = output_folder.replace(",", "")
    os.makedirs(output_folder, exist_ok=True)

    # normalize/clean name for querying
    query_place = " ".join(place_name.strip().split())  # normalize spaces
    print(f"Querying features_from_place for: {query_place}")

    try:
        gdf_all = ox.features_from_place(query_place, tags=tags_dict)
    except Exception as e:
        print(f"[ERROR] Getting features_from_place for {query_place}: {e}")
        return None

    if gdf_all is None or gdf_all.empty:
        print(f"No matching features for {query_place} with given tags.")
        return None

    # ensure we have epsg 4326 to get coordinates
    try:
        gdf_all = gdf_all.to_crs(epsg=4326)
    except Exception:
        pass

    # keep points only and not lines/polygons to avoid complexity
    if 'geometry' in gdf_all:
        geom_types = gdf_all.geom_type.value_counts()
        print(f"geometry types: {geom_types.to_dict()}")  # num of types for data

        # for simple Point type locations
        if 'Point' in geom_types.index:
            gdf_all = gdf_all[gdf_all.geom_type == 'Point'].copy()
            print(f"keeping only Point geometries — rows now: {len(gdf_all)}")
        else:
            print("no Point geometries found; proceeding with all geometries")

    # For Point geometries, geometry.x/geometry.y are lon/lat in EPSG:4326
    if 'geometry' in gdf_all:
        # handle cases where there may be missing values
        gdf_all = gdf_all[gdf_all.geometry.notnull()].copy()
        gdf_all['latitude'] = gdf_all.geometry.y
        gdf_all['longitude'] = gdf_all.geometry.x

    # get rid of duplicates by osm id if required
    if 'osmid' in gdf_all.columns:
        before = len(gdf_all)
        gdf_all = gdf_all.drop_duplicates(subset='osmid')
        print(f"Duplicate removed by osm id: {before} -> {len(gdf_all)}")

    # Save whole combined data in geojson and csv
    place_short = clean_name(query_place)

    combined_fname_geo = f"{place_short}_all_amenities.geojson"
    combined_path_geo = os.path.join(output_folder, combined_fname_geo)
    try:
        os.makedirs(os.path.dirname(combined_path_geo) or output_folder, exist_ok=True)
        gdf_all.to_file(combined_path_geo, driver="GeoJSON")
    except Exception as e:
        print(f"[ERROR] saving combined geojson: {e}")

    combined_fname_csv = combined_fname_geo.replace(".geojson", ".csv")
    combined_path_csv = os.path.join(output_folder, combined_fname_csv)
    try:
        os.makedirs(os.path.dirname(combined_path_csv) or output_folder, exist_ok=True)
        gdf_all.drop(columns='geometry', errors='ignore').to_csv(combined_path_csv, index=False)
    except Exception as e:
        print(f"[ERROR] Saving combined CSV: {e}")

    # Save in separate files for each tag
    for key, values in tags_dict.items():
        if key not in gdf_all.columns:
            print(f"[WARN] Tag '{key}' not in results for {query_place}, skipping this tag.")
            continue

        # Filter subset, handling different formats
        if values is True:
            subset = gdf_all[gdf_all[key].notnull()].copy()
        elif isinstance(values, str):
            subset = gdf_all[gdf_all[key] == values].copy()
        elif isinstance(values, (list, tuple)):
            subset = gdf_all[gdf_all[key].isin(values)].copy()
        else:
            subset = gpd.GeoDataFrame(columns=gdf_all.columns)

        if subset is None or subset.empty:
            print(f"No features found for tag {key} = {values} in {query_place}")
            continue

        tag_short = clean_name(key)
        fname_geojson = f"{place_short}_{tag_short}.geojson"
        path_geojson = os.path.join(output_folder, fname_geojson)
        try:
            os.makedirs(os.path.dirname(path_geojson) or output_folder, exist_ok=True)
            subset.to_file(path_geojson, driver="GeoJSON")
        except Exception as e:
            print(f"[ERROR] Saving subset geojson for {key}: {e}")

        fname_csv = fname_geojson.replace(".geojson", ".csv")
        path_csv = os.path.join(output_folder, fname_csv)
        try:
            os.makedirs(os.path.dirname(path_csv) or output_folder, exist_ok=True)
            subset.drop(columns='geometry', errors='ignore').to_csv(path_csv, index=False)
        except Exception as e:
            print(f"[ERROR] Saving subset CSV for tag {key}: {e}")

    return gdf_all


def extract_for_provinces(provinces_list, tags_dict, output_root_folder="osm_helper_data"):
    for prov in provinces_list:
        prov_clean = clean_name(prov)
        folder = os.path.join(output_root_folder, prov_clean)
        print(f"\n=== Extracting for region: {prov_clean}===\n")
        extract_amenities_for_place(prov, tags_dict, folder)


if __name__ == "__main__":
    tags = {
        "amenity": [
            "school", "university", "hospital", "clinic", "restaurant", "cafe", "library", "police",
            "post_office", "bank", "bar", "marketplace", "market", "theatre", "cinema", "dentist", "childcare",
            "college", "nursing_home", "pharmacy", "doctors", "place_of_worship", "grave_yard", "parking", "fast_food",
            "fire_station", "theatre", "ferry_terminal", "waste_dump_site", "waste_transfer_station"
        ],
        "leisure": ["park", "garden", "sports_centre", "stadium", "swimming_pool", "track", "playground", "picnic_site", "gym",
                   "community_centre", "arts_centre", "music_venue", "nightclub", "nature_reserve", "fitness_centre",
                   "golf_course", "sports_hall", "dog_park", "water_park", "miniature_golf", "ice_rink", "sauna", "resort", 
                   "bowling alley", "recreation_ground"],
        "shop": ["supermarket", "mall", "department_store", "clothes", "bookstore", "bakery", "butcher"],
        "public_transport": ["station", "platform", "subway_entrance", "stop_position"],
        "highway": ["bus_stop"],
        "railway": ["station"],
        "landuse": ["cemetery"],
        "natural": ["water", "wood", "beach"],
        "tourism": ["hotel", "museum", "viewpoint", "attraction"]
    }

    provinces = [
        "Greater Vancouver, British Columbia",
        "Calgary, Alberta",
        "Edmonton, Alberta",
        "Toronto, Ontario",
        "Ottawa, Ontario"
    ]

    output_folder = "osm_helper_data"
    extract_for_provinces(provinces, tags, output_folder)
