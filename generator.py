import os
import io
import zipfile
import httpx
import pandas as pd
import simplekml
import xml.etree.ElementTree as ET

async def generate_kmz_file(gtfs_url: str, github_token: str, output_format: str = "kmz") -> bytes:
    # ============================================================ 
    # STRICT ENV CONFIG (GitHub-safe)
    # ============================================================ 

    # NOTE:
    # Set this environment variable to your GTFS feed URL (operator/city specific).
    GTFS_URL = gtfs_url
    if not GTFS_URL:
        raise RuntimeError(
            "GTFS_URL is missing. "
            "Set it before running the script."
        )

    # NOTE:
    # Token is required because Metro Lines KMZ is stored in a PRIVATE repo via GitHub API.
    GITHUB_TOKEN = github_token
    if not GITHUB_TOKEN:
        raise RuntimeError(
            "GITHUB_TOKEN is missing. "
            "Set it before running the script."
        )

    # ============================================================ 
    # PRIVATE METRO / DEDICATED ROUTES (separate repo)
    # ============================================================ 

    # Metro lines are stored separately because they are permanent/dedicated infrastructure
    # and are not part of daily GTFS updates.
    GITHUB_OWNER = "Mavi2902"
    GITHUB_REPO = "Islamabad-rawalpindi-metro-lines"

    # Path inside the repository (URL-encoded space is OK for GitHub Contents API)
    METRO_FILE_PATH = "Metro%20Lines.kmz"

    # Use branch name for easier maintenance of permanent assets repo
    METRO_REF = "main"

    GITHUB_API_METRO_URL = (
        f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}/contents/"
        f"{METRO_FILE_PATH}?ref={METRO_REF}"
    )

    BASE_PATH = os.path.dirname(os.path.abspath(__file__))

    # ============================================================ 
    # DOWNLOAD GTFS
    # ============================================================ 
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"}
    async with httpx.AsyncClient() as client:
        resp = await client.get(GTFS_URL, timeout=60, headers=headers)
        resp.raise_for_status()

    gtfs_bytes = io.BytesIO(resp.content)

    try:
        with zipfile.ZipFile(gtfs_bytes, "r") as z:
            def read_gtfs(name: str) -> pd.DataFrame:
                if name not in z.namelist():
                    raise FileNotFoundError(f"{name} not found inside GTFS zip")
                return pd.read_csv(z.open(name))

            routes     = read_gtfs("routes.txt")
            trips      = read_gtfs("trips.txt")
            shapes     = read_gtfs("shapes.txt")
            stops      = read_gtfs("stops.txt")
            stop_times = read_gtfs("stop_times.txt")
    except zipfile.BadZipFile:
        raise RuntimeError(
            "Failed to unzip GTFS data. The downloaded file is not a valid zip archive."
        )

    # ============================================================ 
    # HELPERS
    # ============================================================ 

    def safe_col(df: pd.DataFrame, col: str) -> bool:
        return col in df.columns

    def get_route_name(route_row: pd.Series) -> str:
        if safe_col(routes, "route_short_name"):
            val = route_row.get("route_short_name", None)
            if isinstance(val, str) and val.strip():
                return val.strip()
        if safe_col(routes, "route_long_name"):
            val = route_row.get("route_long_name", None)
            if isinstance(val, str) and val.strip():
                return val.strip()
        return str(route_row["route_id"])

    def gtfs_color_to_kml(color_str: str, default: str) -> str:
        if not isinstance(color_str, str):
            return default
        color_str = color_str.strip().lstrip("#")
        if len(color_str) != 6:
            return default
        try:
            r = int(color_str[0:2], 16)
            g = int(color_str[2:4], 16)
            b = int(color_str[4:6], 16)
        except ValueError:
            return default
        return simplekml.Color.rgb(r, g, b)

    # ============================================================ 
    # VALIDATION
    # ============================================================ 

    required_shape_cols = {"shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence"}
    missing = required_shape_cols - set(shapes.columns)
    if missing:
        raise ValueError("shapes.txt missing columns: " + ",".join(missing))

    if "stop_sequence" not in stop_times.columns:
        raise ValueError("stop_times.txt missing required column: stop_sequence")

    # ============================================================ 
    # PREPARE TRIP MAP
    # ============================================================ 

    cols = ["trip_id", "route_id", "shape_id"]
    if safe_col(trips, "direction_id"):
        cols.append("direction_id")

    trip_route_map = trips[cols].copy()
    if "direction_id" not in trip_route_map.columns:
        trip_route_map["direction_id"] = 0

    def pick_representative_trip_id(trip_ids):
        st = stop_times[stop_times["trip_id"].isin(trip_ids)][["trip_id", "stop_sequence"]].copy()
        if st.empty:
            return None
        counts = st.groupby("trip_id").size().sort_values(ascending=False)
        return counts.index[0] if len(counts) else None

    def get_ordered_stops_for_trip(trip_id):
        st = stop_times[stop_times["trip_id"] == trip_id].copy()
        if st.empty:
            return pd.DataFrame()
        st["stop_sequence"] = pd.to_numeric(st["stop_sequence"], errors="coerce")
        st = st.dropna(subset=["stop_sequence"]).sort_values("stop_sequence")
        merged = st.merge(stops, on="stop_id", how="left")
        return merged

    # ============================================================ 
    # BUILD KML
    # ============================================================ 

    kml = simplekml.Kml()
    root_folder = kml.newfolder(name="EV Routes (GTFS + Metro)")

    fwd_group_folder   = root_folder.newfolder(name="Routes - Forward Path")
    bwd_group_folder   = root_folder.newfolder(name="Routes - Backward Path")
    stops_group_folder = root_folder.newfolder(name="Routes - Stops")

    for _, route_row in routes.iterrows():
        route_id = route_row["route_id"]
        route_name = get_route_name(route_row)

        route_color_val = route_row.get("route_color", None) if safe_col(routes, "route_color") else None
        line_color = gtfs_color_to_kml(route_color_val, default=simplekml.Color.blue)

        stop_color_val = route_row.get("route_text_color", None) if safe_col(routes, "route_text_color") else None
        stop_icon_color = gtfs_color_to_kml(stop_color_val, default=line_color)

        route_fwd_folder   = fwd_group_folder.newfolder(name=str(route_name))
        route_bwd_folder   = bwd_group_folder.newfolder(name=str(route_name))
        route_stops_folder = stops_group_folder.newfolder(name=str(route_name))

        route_stops_fwd_folder = route_stops_folder.newfolder(name="Stops - FWD")
        route_stops_bwd_folder = route_stops_folder.newfolder(name="Stops - BWD")

        route_trips = trip_route_map[trip_route_map["route_id"] == route_id]
        if route_trips.empty:
            continue

        directions = sorted(route_trips["direction_id"].dropna().unique())

        for direction in directions:
            dir_trips = route_trips[route_trips["direction_id"] == direction]
            if dir_trips.empty:
                continue

            try:
                dir_int = int(direction)
            except Exception:
                dir_int = 0

            dir_label = "FWD" if dir_int == 0 else "BWD"

            rep_trip_row = dir_trips.iloc[0]
            shape_id = rep_trip_row["shape_id"]
            if pd.isna(shape_id):
                continue

            route_shape = shapes[shapes["shape_id"] == shape_id].copy()
            if route_shape.empty:
                continue

            route_shape = route_shape.sort_values("shape_pt_sequence")
            coords = list(
                zip(
                    route_shape["shape_pt_lon"].astype(float),
                    route_shape["shape_pt_lat"].astype(float),
                )
            )
            if len(coords) < 2:
                continue

            line_name = f"{route_name} {dir_label}"
            if dir_int == 0:
                ls = route_fwd_folder.newlinestring(name=line_name)
            else:
                ls = route_bwd_folder.newlinestring(name=line_name)

            ls.coords = coords
            ls.style.linestyle.width = 4
            ls.style.linestyle.color = line_color

            rep_trip_id = pick_representative_trip_id(dir_trips["trip_id"].unique())
            if rep_trip_id is None:
                continue

            ordered = get_ordered_stops_for_trip(rep_trip_id)
            if ordered.empty:
                continue

            ordered = ordered.drop_duplicates(subset=["stop_id"], keep="first")
            target_folder = route_stops_fwd_folder if dir_int == 0 else route_stops_bwd_folder

            for _, row in ordered.iterrows():
                stop_name = row.get("stop_name", row.get("stop_id", ""))
                try:
                    lon = float(row["stop_lon"])
                    lat = float(row["stop_lat"])
                except Exception:
                    continue

                pnt = target_folder.newpoint(name=str(stop_name), coords=[(lon, lat)])
                pnt.style.labelstyle.scale = 0.8
                pnt.style.iconstyle.color = stop_icon_color
    
    # ============================================================ 
    # DOWNLOAD METRO KMZ FROM PRIVATE GITHUB (Contents API)
    # ============================================================ 

    metro_lines = []
    
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.raw",
    }

    async with httpx.AsyncClient() as client:
        r_metro = await client.get(GITHUB_API_METRO_URL, headers=headers, timeout=60)
        r_metro.raise_for_status()

    kmz_bytes = io.BytesIO(r_metro.content)

    with zipfile.ZipFile(kmz_bytes, "r") as mz:
        kml_names = [n for n in mz.namelist() if n.lower().endswith(".kml")]
        if not kml_names:
            raise RuntimeError("No .kml file found inside Metro KMZ")
        kml_data = mz.read(kml_names[0]).decode("utf-8")

    root = ET.fromstring(kml_data)
    ns = {"k": "http://www.opengis.net/kml/2.2"}

    for pm in root.findall(".//k:Placemark", ns):
        name_el = pm.find("k:name", ns)
        name = name_el.text.strip() if (name_el is not None and name_el.text) else "Metro Segment"

        ls = pm.find(".//k:LineString", ns)
        if ls is None:
            continue
        coords_el = ls.find("k:coordinates", ns)
        if coords_el is None or not coords_el.text:
            continue

        coords = []
        for part in coords_el.text.replace("\n", " ").split():
            parts = part.split(",")
            if len(parts) >= 2:
                try:
                    lon = float(parts[0])
                    lat = float(parts[1])
                    coords.append((lon, lat))
                except ValueError:
                    continue

        if len(coords) >= 2:
            metro_lines.append((name, coords))

    for name, coords in metro_lines:
        name_lower = name.lower()
        if "red" in name_lower:
            color = simplekml.Color.red
        elif "orange" in name_lower:
            color = simplekml.Color.rgb(255, 165, 0)
        else:
            color = simplekml.Color.white

        fwd_folder = fwd_group_folder.newfolder(name=name)
        bwd_folder = bwd_group_folder.newfolder(name=name)

        fwd_ls = fwd_folder.newlinestring(name=name)
        fwd_ls.coords = coords
        fwd_ls.style.linestyle.width = 5
        fwd_ls.style.linestyle.color = color

        bwd_ls = bwd_folder.newlinestring(name=name)
        bwd_ls.coords = list(reversed(coords))
        bwd_ls.style.linestyle.width = 5
        bwd_ls.style.linestyle.color = color

    # ============================================================ 
    # SAVE OUTPUT KMZ
    # ============================================================ 

    kml_content = kml.kml(format=False)

    if output_format == "kml":
        return kml_content.encode("utf-8")

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("doc.kml", kml_content)

    return zip_buffer.getvalue()