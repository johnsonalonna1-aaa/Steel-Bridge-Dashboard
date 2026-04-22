from datetime import date, datetime, timedelta
from io import BytesIO
from pathlib import Path
import shutil
import zipfile

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import yaml


APP_DIR = Path(__file__).parent
DATA_DIR = APP_DIR / "data"
RULES_DIR = APP_DIR / "rules"
ARCHIVE_DIR = DATA_DIR / "design_archive"
YEARLY_RECORDS_DIR = DATA_DIR / "yearly_records"
TRACKER_PATH = DATA_DIR / "fabrication_tracker.csv"
TASKS_PATH = DATA_DIR / "team_tasks.csv"
EVENTS_PATH = DATA_DIR / "calendar_events.csv"
COSTS_PATH = DATA_DIR / "cost_estimate.csv"

SAMPLE_JOINTS = APP_DIR / "Joint Coordinates 2026.xlsx"
SAMPLE_FRAMES = APP_DIR / "Frame Connectivity 2026.xlsx"
SAMPLE_RULES = RULES_DIR / "rules_2026.yaml"

STATUS_OPTIONS = [
    "Not Started",
    "Cut",
    "Drilled",
    "Welded",
    "Painted",
    "Completed",
]

STATUS_COLORS = {
    "Not Started": "#94a3b8",
    "Cut": "#38bdf8",
    "Drilled": "#fbbf24",
    "Welded": "#fb7185",
    "Painted": "#a78bfa",
    "Completed": "#22c55e",
}

TASK_STATUS_OPTIONS = ["Not Started", "In Progress", "Blocked", "Done"]
TASK_PRIORITY_OPTIONS = ["High", "Medium", "Low"]
EVENT_TYPE_OPTIONS = ["Meeting", "Deadline", "Fabrication", "Testing", "Competition", "Other"]
COST_CATEGORY_OPTIONS = ["Steel", "Bolts", "Welding", "Paint", "Tools", "Travel", "Testing", "Other"]


st.set_page_config(
    page_title="UTC Steel Bridge Dashboard",
    page_icon="bridge_at_night",
    layout="wide",
)

st.markdown(
    """
    <style>
    :root {
        --utc-blue: #12355b;
        --steel: #44546a;
        --rivet: #d6a94a;
        --panel: #f8fafc;
        --ink: #111827;
    }
    .stApp {
        background:
            linear-gradient(135deg, rgba(18, 53, 91, 0.05), rgba(214, 169, 74, 0.08)),
            #ffffff;
    }
    .block-container {
        padding-top: 2rem;
        padding-bottom: 3rem;
        max-width: 1320px;
    }
    div[data-testid="stMetric"] {
        background: rgba(255, 255, 255, 0.84);
        border: 1px solid rgba(68, 84, 106, 0.18);
        border-radius: 8px;
        padding: 0.85rem 1rem;
        box-shadow: 0 8px 22px rgba(17, 24, 39, 0.06);
    }
    div[data-testid="stMetricLabel"] p {
        color: var(--steel);
        font-weight: 700;
    }
    .hero {
        border-left: 6px solid var(--rivet);
        padding: 0.4rem 0 0.6rem 1rem;
        margin-bottom: 1rem;
    }
    .hero h1 {
        color: var(--utc-blue);
        font-size: 2.65rem;
        line-height: 1.05;
        margin-bottom: 0.2rem;
    }
    .hero p {
        color: var(--steel);
        font-size: 1.05rem;
        margin: 0;
    }
    .check-pass, .check-fail, .check-warn {
        border-radius: 8px;
        padding: 0.85rem 1rem;
        margin: 0.35rem 0;
        border: 1px solid;
        font-weight: 650;
    }
    .check-pass {
        background: #ecfdf5;
        border-color: #86efac;
        color: #166534;
    }
    .check-fail {
        background: #fef2f2;
        border-color: #fca5a5;
        color: #991b1b;
    }
    .check-warn {
        background: #fffbeb;
        border-color: #fcd34d;
        color: #92400e;
    }
    .small-note {
        color: #64748b;
        font-size: 0.9rem;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


def normalize_column_name(value):
    return "".join(ch for ch in str(value).strip().lower() if ch.isalnum())


def safe_slug(value, fallback="steel_bridge"):
    slug = "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in str(value).strip())
    return slug.strip("_") or fallback


def read_excel_table(file_or_path):
    """Read SAP2000-style Excel exports where the useful header is often row 2."""
    last_error = None
    for header_row in (1, 0):
        try:
            df = pd.read_excel(file_or_path, header=header_row)
            df = df.dropna(how="all").reset_index(drop=True)
            if len(df) > 0:
                return df
        except Exception as exc:
            last_error = exc
    raise ValueError(f"Could not read Excel file. {last_error}")


def pick_columns(df, required):
    normalized = {normalize_column_name(col): col for col in df.columns}
    picked = {}
    for output_name, aliases in required.items():
        for alias in aliases:
            key = normalize_column_name(alias)
            if key in normalized:
                picked[output_name] = normalized[key]
                break
        if output_name not in picked:
            raise ValueError(
                f"Missing column for '{output_name}'. Expected one of: {', '.join(aliases)}"
            )
    return picked


@st.cache_data(show_spinner=False)
def clean_joints(file_or_path):
    df = read_excel_table(file_or_path)
    columns = pick_columns(
        df,
        {
            "Joint": ["Joint", "Joint ID", "JointID"],
            "GlobalX": ["GlobalX", "Global X", "X"],
            "GlobalY": ["GlobalY", "Global Y", "Y"],
            "Z": ["GlobalZ", "Global Z", "Z"],
        },
    )
    cleaned = df[list(columns.values())].rename(
        columns={source: target for target, source in columns.items()}
    )
    for col in ["Joint", "GlobalX", "GlobalY", "Z"]:
        cleaned[col] = pd.to_numeric(cleaned[col], errors="coerce")
    return cleaned.dropna(subset=["Joint", "GlobalX", "GlobalY", "Z"]).reset_index(drop=True)


@st.cache_data(show_spinner=False)
def clean_frames(file_or_path):
    df = read_excel_table(file_or_path)
    columns = pick_columns(
        df,
        {
            "Frame": ["Frame", "Frame ID", "FrameID", "Member", "MemberID"],
            "JointI": ["JointI", "Joint I", "I-Joint", "I Joint"],
            "JointJ": ["JointJ", "Joint J", "J-Joint", "J Joint"],
            "Length": ["Length", "Object Length", "Member Length"],
        },
    )
    cleaned = df[list(columns.values())].rename(
        columns={source: target for target, source in columns.items()}
    )
    cleaned["Frame"] = cleaned["Frame"].astype(str).str.strip()
    for col in ["JointI", "JointJ", "Length"]:
        cleaned[col] = pd.to_numeric(cleaned[col], errors="coerce")
    return cleaned.dropna(subset=["Frame", "JointI", "JointJ"]).reset_index(drop=True)


@st.cache_data(show_spinner=False)
def load_rules(file_or_path):
    if file_or_path is None:
        return {}
    if hasattr(file_or_path, "getvalue"):
        content = file_or_path.getvalue().decode("utf-8")
    elif hasattr(file_or_path, "read"):
        content = file_or_path.read()
        if isinstance(content, bytes):
            content = content.decode("utf-8")
    else:
        content = Path(file_or_path).read_text()
    rules = yaml.safe_load(content) or {}
    rules.setdefault("year", datetime.now().year)
    rules.setdefault("limits", {})
    rules.setdefault("deflection", {})
    return rules


def get_input_file(uploaded_file, default_path, use_sample_data):
    if uploaded_file is not None:
        return uploaded_file
    if use_sample_data and default_path.exists():
        return default_path
    return None


def calculate_geometry(joints, frames):
    joint_lookup = joints.set_index("Joint")[["GlobalX", "GlobalY", "Z"]].to_dict("index")
    geometry = {
        "bridge_length": joints["GlobalX"].max() - joints["GlobalX"].min(),
        "bridge_width": joints["GlobalY"].max() - joints["GlobalY"].min(),
        "bridge_height": joints["Z"].max() - joints["Z"].min(),
        "total_member_length": frames["Length"].dropna().sum(),
        "missing_connections": 0,
    }

    calculated_lengths = []
    for _, row in frames.iterrows():
        p1 = joint_lookup.get(row["JointI"])
        p2 = joint_lookup.get(row["JointJ"])
        if not p1 or not p2:
            geometry["missing_connections"] += 1
            calculated_lengths.append(None)
            continue
        dx = p2["GlobalX"] - p1["GlobalX"]
        dy = p2["GlobalY"] - p1["GlobalY"]
        dz = p2["Z"] - p1["Z"]
        calculated_lengths.append((dx**2 + dy**2 + dz**2) ** 0.5)

    frames = frames.copy()
    frames["CalculatedLength"] = calculated_lengths
    if frames["Length"].isna().all():
        geometry["total_member_length"] = frames["CalculatedLength"].dropna().sum()
    geometry["longest_member"] = frames[["Length", "CalculatedLength"]].max(axis=1).max()
    return geometry, frames


def build_rule_checks(geometry, rules):
    limits = rules.get("limits", {})
    checks = []
    check_specs = [
        ("Bridge length", geometry["bridge_length"], limits.get("max_length"), "ft"),
        ("Bridge width", geometry["bridge_width"], limits.get("max_width"), "ft"),
        ("Bridge height", geometry["bridge_height"], limits.get("max_height"), "ft"),
    ]

    if limits.get("max_member_length") is not None:
        check_specs.append(
            ("Longest member", geometry["longest_member"], limits.get("max_member_length"), "ft")
        )

    for name, actual, limit, unit in check_specs:
        if limit is None:
            status = "Warning"
            message = f"No {name.lower()} limit is defined in the rules file."
        elif actual <= limit:
            status = "Pass"
            message = f"{actual:.3f} {unit} is within the {limit:.3f} {unit} limit."
        else:
            status = "Fail"
            message = f"{actual:.3f} {unit} exceeds the {limit:.3f} {unit} limit."
        checks.append({"Check": name, "Actual": actual, "Limit": limit, "Unit": unit, "Status": status, "Message": message})

    if geometry["missing_connections"]:
        checks.append(
            {
                "Check": "Frame connectivity",
                "Actual": geometry["missing_connections"],
                "Limit": 0,
                "Unit": "missing",
                "Status": "Fail",
                "Message": f"{geometry['missing_connections']} frame(s) reference joints that were not found.",
            }
        )
    else:
        checks.append(
            {
                "Check": "Frame connectivity",
                "Actual": 0,
                "Limit": 0,
                "Unit": "missing",
                "Status": "Pass",
                "Message": "Every frame references valid joint coordinates.",
            }
        )

    return pd.DataFrame(checks)


def load_tracker(frame_ids):
    frame_ids = [str(frame_id) for frame_id in frame_ids]
    if TRACKER_PATH.exists():
        tracker = pd.read_csv(TRACKER_PATH, dtype=str).fillna("")
    else:
        tracker = pd.DataFrame(columns=["MemberID", "Label", "Material", "Welder", "Status", "Notes"])

    for column in ["MemberID", "Label", "Material", "Welder", "Status", "Notes"]:
        if column not in tracker.columns:
            tracker[column] = ""

    tracker["MemberID"] = tracker["MemberID"].astype(str)
    existing_ids = set(tracker["MemberID"])
    missing_ids = [frame_id for frame_id in frame_ids if frame_id not in existing_ids]

    if missing_ids:
        new_rows = pd.DataFrame(
            {
                "MemberID": missing_ids,
                "Label": "",
                "Material": "",
                "Welder": "",
                "Status": "Not Started",
                "Notes": "",
            }
        )
        tracker = pd.concat([tracker, new_rows], ignore_index=True)

    tracker["Status"] = tracker["Status"].where(tracker["Status"].isin(STATUS_OPTIONS), "Not Started")
    return tracker[["MemberID", "Label", "Material", "Welder", "Status", "Notes"]]


def save_tracker(tracker_df):
    DATA_DIR.mkdir(exist_ok=True)
    tracker_df.to_csv(TRACKER_PATH, index=False)


def load_csv_table(path, columns, starter_rows=None):
    if path.exists():
        table = pd.read_csv(path, dtype=str).fillna("")
    else:
        table = pd.DataFrame(starter_rows or [], columns=columns)

    for column in columns:
        if column not in table.columns:
            table[column] = ""
    return table[columns].copy()


def save_csv_table(path, table):
    DATA_DIR.mkdir(exist_ok=True)
    table.to_csv(path, index=False)


def load_team_tasks(year):
    starter_rows = build_rule_based_tasks({}, year)
    return load_csv_table(TASKS_PATH, ["Task", "Owner", "Priority", "Status", "DueDate", "Notes"], starter_rows)


def pretty_rule_name(name):
    return str(name).replace("_", " ").replace("-", " ").title()


def build_rule_based_tasks(rules, year):
    starter_rows = [
        {
            "Task": "Update yearly rules YAML",
            "Owner": "Captain",
            "Priority": "High",
            "Status": "In Progress",
            "DueDate": f"{year}-01-31",
            "Notes": "Review new SSBC rules and update limit values.",
        },
        {
            "Task": "Export SAP2000 joint and frame tables",
            "Owner": "Design Lead",
            "Priority": "High",
            "Status": "Not Started",
            "DueDate": f"{year}-02-15",
            "Notes": "Use the final analysis model before checking compliance.",
        },
        {
            "Task": "Archive final design package",
            "Owner": "Captain",
            "Priority": "Medium",
            "Status": "Not Started",
            "DueDate": f"{year}-04-15",
            "Notes": "Save model exports, rule checks, budget, tasks, and schedule.",
        },
    ]
    limits = rules.get("limits", {}) if isinstance(rules, dict) else {}
    deflection = rules.get("deflection", {}) if isinstance(rules, dict) else {}

    for limit_name, limit_value in limits.items():
        starter_rows.append(
            {
                "Task": f"Check {pretty_rule_name(limit_name)} rule",
                "Owner": "Design Lead",
                "Priority": "High",
                "Status": "Not Started",
                "DueDate": f"{year}-02-28",
                "Notes": f"Verify SAP2000/exported design satisfies rule value: {limit_value}.",
            }
        )

    for deflection_name, deflection_value in deflection.items():
        starter_rows.append(
            {
                "Task": f"Verify {pretty_rule_name(deflection_name)} deflection",
                "Owner": "Analysis Lead",
                "Priority": "High",
                "Status": "Not Started",
                "DueDate": f"{year}-03-15",
                "Notes": f"Compare analysis results against deflection value: {deflection_value}.",
            }
        )

    return starter_rows


def load_calendar_events(year):
    starter_rows = [
        {
            "Event": "Rules review meeting",
            "Type": "Meeting",
            "Date": f"{year}-01-20",
            "StartTime": "18:00",
            "EndTime": "19:00",
            "Location": "Engineering building",
            "Notes": "Assign rule-check responsibilities.",
        },
        {
            "Event": "Bridge fit-up check",
            "Type": "Testing",
            "Date": f"{year}-03-15",
            "StartTime": "17:30",
            "EndTime": "19:30",
            "Location": "Fabrication space",
            "Notes": "Bring tape measure, rule book, and current model exports.",
        },
    ]
    return load_csv_table(
        EVENTS_PATH,
        ["Event", "Type", "Date", "StartTime", "EndTime", "Location", "Notes"],
        starter_rows,
    )


def load_cost_estimate():
    starter_rows = [
        {
            "Item": "Steel members",
            "Category": "Steel",
            "Quantity": "1",
            "Unit": "lot",
            "UnitCost": "0",
            "Vendor": "",
            "NeededBy": "",
            "Purchased": "No",
            "Notes": "Replace with actual quote.",
        },
        {
            "Item": "Bolts and connection hardware",
            "Category": "Bolts",
            "Quantity": "1",
            "Unit": "lot",
            "UnitCost": "0",
            "Vendor": "",
            "NeededBy": "",
            "Purchased": "No",
            "Notes": "Track size, grade, and count.",
        },
    ]
    return load_csv_table(
        COSTS_PATH,
        ["Item", "Category", "Quantity", "Unit", "UnitCost", "Vendor", "NeededBy", "Purchased", "Notes"],
        starter_rows,
    )


def clean_date(value):
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.date()


def prepare_date_columns(table, columns):
    prepared = table.copy()
    for column in columns:
        if column in prepared.columns:
            prepared[column] = pd.to_datetime(prepared[column], errors="coerce")
    return prepared


def calculate_task_metrics(tasks):
    if tasks.empty:
        return 0, 0, 0
    due_dates = tasks["DueDate"].apply(clean_date)
    today = date.today()
    open_tasks = tasks["Status"].ne("Done")
    overdue = (due_dates < today) & open_tasks & due_dates.notna()
    due_soon = (due_dates >= today) & (due_dates <= today + timedelta(days=14)) & open_tasks
    completion = tasks["Status"].eq("Done").mean() * 100
    return completion, int(overdue.sum()), int(due_soon.sum())


def calculate_cost_table(costs):
    cost_table = costs.copy()
    cost_table["Quantity"] = pd.to_numeric(cost_table["Quantity"], errors="coerce").fillna(0)
    cost_table["UnitCost"] = pd.to_numeric(cost_table["UnitCost"], errors="coerce").fillna(0)
    cost_table["EstimatedCost"] = cost_table["Quantity"] * cost_table["UnitCost"]
    return cost_table


def format_ics_datetime(event_date, event_time, fallback_hour):
    parsed_date = clean_date(event_date)
    if parsed_date is None:
        return None
    parsed_time = pd.to_datetime(event_time, errors="coerce")
    if pd.isna(parsed_time):
        event_dt = datetime.combine(parsed_date, datetime.min.time()).replace(hour=fallback_hour)
    else:
        event_dt = datetime.combine(parsed_date, parsed_time.time())
    return event_dt.strftime("%Y%m%dT%H%M%S")


def escape_ics_text(value):
    text = str(value or "")
    return text.replace("\\", "\\\\").replace(",", "\\,").replace(";", "\\;").replace("\n", "\\n")


def build_calendar_ics(events, tasks, project_name):
    lines = [
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//UTC Steel Bridge Dashboard//EN",
        "CALSCALE:GREGORIAN",
    ]
    stamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

    for idx, row in events.reset_index(drop=True).iterrows():
        start = format_ics_datetime(row.get("Date"), row.get("StartTime"), 18)
        end = format_ics_datetime(row.get("Date"), row.get("EndTime"), 19)
        if start is None:
            continue
        if end is None or end <= start:
            start_date = datetime.strptime(start, "%Y%m%dT%H%M%S")
            end = (start_date + timedelta(hours=1)).strftime("%Y%m%dT%H%M%S")
        lines.extend(
            [
                "BEGIN:VEVENT",
                f"UID:event-{idx}-{stamp}@utc-steel-bridge",
                f"DTSTAMP:{stamp}",
                f"DTSTART:{start}",
                f"DTEND:{end}",
                f"SUMMARY:{escape_ics_text(row.get('Event'))}",
                f"LOCATION:{escape_ics_text(row.get('Location'))}",
                f"DESCRIPTION:{escape_ics_text(row.get('Notes'))}",
                "END:VEVENT",
            ]
        )

    for idx, row in tasks[tasks["Status"].ne("Done")].reset_index(drop=True).iterrows():
        due = format_ics_datetime(row.get("DueDate"), "09:00", 9)
        if due is None:
            continue
        lines.extend(
            [
                "BEGIN:VEVENT",
                f"UID:task-{idx}-{stamp}@utc-steel-bridge",
                f"DTSTAMP:{stamp}",
                f"DTSTART:{due}",
                f"DTEND:{due}",
                f"SUMMARY:{escape_ics_text(project_name)} task due: {escape_ics_text(row.get('Task'))}",
                f"DESCRIPTION:{escape_ics_text(row.get('Notes'))}",
                "END:VEVENT",
            ]
        )

    lines.append("END:VCALENDAR")
    return "\r\n".join(lines).encode("utf-8")


def build_3d_figure(joints, frames, tracker=None):
    fig = go.Figure()
    joint_lookup = joints.set_index("Joint")[["GlobalX", "GlobalY", "Z"]].to_dict("index")
    status_lookup = {}
    if tracker is not None and "MemberID" in tracker.columns:
        status_lookup = dict(zip(tracker["MemberID"].astype(str), tracker["Status"]))

    for _, row in frames.iterrows():
        p1 = joint_lookup.get(row["JointI"])
        p2 = joint_lookup.get(row["JointJ"])
        if not p1 or not p2:
            continue

        status = status_lookup.get(str(row["Frame"]), "Not Started")
        fig.add_trace(
            go.Scatter3d(
                x=[p1["GlobalX"], p2["GlobalX"]],
                y=[p1["GlobalY"], p2["GlobalY"]],
                z=[p1["Z"], p2["Z"]],
                mode="lines",
                line=dict(width=6, color=STATUS_COLORS.get(status, "#2563eb")),
                hovertemplate=(
                    f"Frame: {row['Frame']}<br>Status: {status}<br>"
                    "X: %{x:.2f}<br>Y: %{y:.2f}<br>Z: %{z:.2f}<extra></extra>"
                ),
                showlegend=False,
            )
        )

    fig.add_trace(
        go.Scatter3d(
            x=joints["GlobalX"],
            y=joints["GlobalY"],
            z=joints["Z"],
            mode="markers+text",
            marker=dict(size=4, color="#dc2626"),
            text=joints["Joint"].astype(int).astype(str),
            textposition="top center",
            hovertemplate="Joint %{text}<br>X: %{x:.2f}<br>Y: %{y:.2f}<br>Z: %{z:.2f}<extra></extra>",
            name="Joints",
        )
    )

    fig.update_layout(
        height=720,
        margin=dict(l=0, r=0, t=10, b=0),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        scene=dict(
            aspectmode="data",
            xaxis_title="Global X (ft)",
            yaxis_title="Global Y (ft)",
            zaxis_title="Z (ft)",
            xaxis=dict(backgroundcolor="#f8fafc", gridcolor="#dbe4ef"),
            yaxis=dict(backgroundcolor="#f8fafc", gridcolor="#dbe4ef"),
            zaxis=dict(backgroundcolor="#f8fafc", gridcolor="#dbe4ef"),
        ),
    )
    return fig


def archive_design(project_name, year, joints, frames, checks, tracker, rules, tasks, events, costs):
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    safe_project_name = safe_slug(project_name)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    archive_base = ARCHIVE_DIR / f"{year}_{safe_project_name}_{stamp}"
    archive_base.mkdir(parents=True, exist_ok=True)

    joints.to_csv(archive_base / "joints.csv", index=False)
    frames.to_csv(archive_base / "frames.csv", index=False)
    checks.to_csv(archive_base / "rule_checks.csv", index=False)
    tracker.to_csv(archive_base / "fabrication_tracker.csv", index=False)
    tasks.to_csv(archive_base / "team_tasks.csv", index=False)
    events.to_csv(archive_base / "calendar_events.csv", index=False)
    costs.to_csv(archive_base / "cost_estimate.csv", index=False)
    (archive_base / "rules.yaml").write_text(yaml.safe_dump(rules, sort_keys=False))
    return archive_base


def save_source_file(source, destination):
    if source is None:
        return None
    destination.parent.mkdir(parents=True, exist_ok=True)
    if hasattr(source, "getvalue"):
        destination.write_bytes(source.getvalue())
    else:
        source_path = Path(source)
        if source_path.exists():
            shutil.copy2(source_path, destination)
    return destination


def save_season_record(project_name, year, joints, frames, checks, tracker, rules, tasks, events, costs, sources):
    season_dir = YEARLY_RECORDS_DIR / str(year)
    exports_dir = season_dir / "processed_exports"
    source_dir = season_dir / "source_files"
    exports_dir.mkdir(parents=True, exist_ok=True)
    source_dir.mkdir(parents=True, exist_ok=True)

    joints.to_csv(exports_dir / "joints_cleaned.csv", index=False)
    frames.to_csv(exports_dir / "frames_cleaned.csv", index=False)
    checks.to_csv(exports_dir / "rule_checks.csv", index=False)
    tracker.to_csv(exports_dir / "fabrication_tracker.csv", index=False)
    tasks.to_csv(exports_dir / "team_tasks.csv", index=False)
    events.to_csv(exports_dir / "calendar_events.csv", index=False)
    costs.to_csv(exports_dir / "cost_estimate.csv", index=False)
    (season_dir / "rules.yaml").write_text(yaml.safe_dump(rules, sort_keys=False))

    source_map = {
        "joints": "joint_coordinates.xlsx",
        "frames": "frame_connectivity.xlsx",
        "rules": "rules.yaml",
        "rule_pdf": "rule_book.pdf",
    }
    saved_sources = {}
    for key, filename in source_map.items():
        saved = save_source_file(sources.get(key), source_dir / filename)
        if saved:
            saved_sources[key] = str(saved.relative_to(season_dir))

    manifest = {
        "project_name": project_name,
        "competition_year": int(year),
        "saved_at": datetime.now().isoformat(timespec="seconds"),
        "source_files": saved_sources,
        "processed_exports": [
            "joints_cleaned.csv",
            "frames_cleaned.csv",
            "rule_checks.csv",
            "fabrication_tracker.csv",
            "team_tasks.csv",
            "calendar_events.csv",
            "cost_estimate.csv",
        ],
        "handoff_note": "Keep this folder in a team-owned shared drive or GitHub organization, not only in one student's school OneDrive.",
    }
    (season_dir / "manifest.yaml").write_text(yaml.safe_dump(manifest, sort_keys=False))
    return season_dir


def list_season_records():
    if not YEARLY_RECORDS_DIR.exists():
        return []
    return sorted([folder for folder in YEARLY_RECORDS_DIR.iterdir() if folder.is_dir()], reverse=True)


def make_folder_zip(folder):
    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for path in folder.rglob("*"):
            if path.is_file():
                archive.write(path, path.relative_to(folder.parent))
    buffer.seek(0)
    return buffer.getvalue()


def read_file_bytes(path):
    return Path(path).read_bytes()


st.markdown(
    """
    <div class="hero">
        <h1>UTC Steel Bridge Digital Compliance Dashboard</h1>
        <p>Reusable SAP2000 export checker, 3D model viewer, and fabrication archive for future competition years.</p>
    </div>
    """,
    unsafe_allow_html=True,
)

with st.sidebar:
    st.header("Project Setup")
    project_name = st.text_input("Design name", value="UTC Steel Bridge")
    competition_year = st.number_input("Competition year", min_value=2020, max_value=2100, value=2026, step=1)
    use_sample_data = st.toggle("Use included 2026 example files", value=True)

    st.divider()
    joints_file = st.file_uploader("Joint coordinates export", type=["xlsx"])
    frames_file = st.file_uploader("Frame connectivity export", type=["xlsx"])
    rules_file = st.file_uploader("Yearly rules file", type=["yaml", "yml"])
    rule_pdf_file = st.file_uploader("Official rule book PDF", type=["pdf"])

    st.caption("Expected workflow: SAP2000 exports Excel tables, the YAML file stores yearly rule limits, and this Streamlit app checks the design.")

joints_source = get_input_file(joints_file, SAMPLE_JOINTS, use_sample_data)
frames_source = get_input_file(frames_file, SAMPLE_FRAMES, use_sample_data)
rules_source = get_input_file(rules_file, SAMPLE_RULES, use_sample_data)

if not all([joints_source, frames_source, rules_source]):
    st.info("Upload the SAP2000 joint table, frame connectivity table, and yearly YAML rules file to begin.")
    st.stop()

try:
    joints = clean_joints(joints_source)
    frames = clean_frames(frames_source)
    rules = load_rules(rules_source)
    geometry, frames = calculate_geometry(joints, frames)
    checks = build_rule_checks(geometry, rules)
except Exception as exc:
    st.error(f"The dashboard could not read the input files: {exc}")
    st.stop()

current_sources = {
    "joints": joints_source,
    "frames": frames_source,
    "rules": rules_source,
    "rule_pdf": rule_pdf_file,
}

tracker = load_tracker(frames["Frame"].tolist())
tasks = load_team_tasks(int(competition_year))
events = load_calendar_events(int(competition_year))
costs = load_cost_estimate()
cost_table = calculate_cost_table(costs)
completion_rate = (tracker["Status"].eq("Completed").mean() * 100) if len(tracker) else 0
task_completion_rate, overdue_tasks, due_soon_tasks = calculate_task_metrics(tasks)
total_estimated_cost = cost_table["EstimatedCost"].sum()
pass_count = checks["Status"].eq("Pass").sum()
fail_count = checks["Status"].eq("Fail").sum()

metric_cols = st.columns(5)
metric_cols[0].metric("Rule checks passed", f"{pass_count}/{len(checks)}")
metric_cols[1].metric("Open issues", int(fail_count))
metric_cols[2].metric("Joints", len(joints))
metric_cols[3].metric("Frames", len(frames))
metric_cols[4].metric("Fabrication complete", f"{completion_rate:.0f}%")

project_cols = st.columns(4)
project_cols[0].metric("Tasks complete", f"{task_completion_rate:.0f}%")
project_cols[1].metric("Overdue tasks", overdue_tasks)
project_cols[2].metric("Due next 14 days", due_soon_tasks)
project_cols[3].metric("Estimated budget", f"${total_estimated_cost:,.2f}")

tab_overview, tab_model, tab_fabrication, tab_schedule, tab_budget, tab_archive, tab_data, tab_workflow = st.tabs(
    ["Overview", "3D Model", "Fabrication", "Schedule", "Budget", "Archive", "Raw Data", "Workflow"]
)

with tab_overview:
    st.subheader("Compliance Summary")
    for _, row in checks.iterrows():
        css_class = {"Pass": "check-pass", "Fail": "check-fail"}.get(row["Status"], "check-warn")
        st.markdown(
            f"<div class='{css_class}'>{row['Check']}: {row['Message']}</div>",
            unsafe_allow_html=True,
        )

    st.subheader("Bridge Envelope")
    envelope_cols = st.columns(4)
    envelope_cols[0].metric("Length (ft)", f"{geometry['bridge_length']:.3f}")
    envelope_cols[1].metric("Width (ft)", f"{geometry['bridge_width']:.3f}")
    envelope_cols[2].metric("Height (ft)", f"{geometry['bridge_height']:.3f}")
    envelope_cols[3].metric("Total member length (ft)", f"{geometry['total_member_length']:.3f}")

    left, right = st.columns([1.2, 1])
    with left:
        st.subheader("Rule Table")
        st.dataframe(checks, use_container_width=True, hide_index=True)
    with right:
        st.subheader("Yearly Rules")
        st.json(rules, expanded=True)

with tab_model:
    st.subheader("Interactive 3D Bridge Model")
    color_by_tracker = st.toggle("Color members by fabrication status", value=True)
    fig = build_3d_figure(joints, frames, tracker if color_by_tracker else None)
    st.plotly_chart(fig, use_container_width=True)

    st.markdown(
        "<p class='small-note'>Tip: drag to rotate, scroll to zoom, and hover over members or joints for labels.</p>",
        unsafe_allow_html=True,
    )

with tab_fabrication:
    st.subheader("Fabrication Tracker")
    edited_tracker = st.data_editor(
        tracker,
        use_container_width=True,
        num_rows="dynamic",
        column_config={
            "MemberID": st.column_config.TextColumn("Member ID", disabled=True),
            "Label": st.column_config.TextColumn("Shop label"),
            "Material": st.column_config.TextColumn("Material"),
            "Welder": st.column_config.TextColumn("Welder"),
            "Status": st.column_config.SelectboxColumn("Status", options=STATUS_OPTIONS, required=True),
            "Notes": st.column_config.TextColumn("Notes"),
        },
        key="tracker_editor",
    )

    button_cols = st.columns([1, 1, 4])
    if button_cols[0].button("Save tracker", type="primary"):
        save_tracker(edited_tracker)
        st.success(f"Saved to {TRACKER_PATH}")

    merged = frames.merge(edited_tracker, left_on="Frame", right_on="MemberID", how="left")
    status_counts = (
        edited_tracker["Status"]
        .value_counts()
        .reindex(STATUS_OPTIONS, fill_value=0)
        .reset_index()
    )
    status_counts.columns = ["Status", "Count"]

    chart = go.Figure(
        go.Bar(
            x=status_counts["Status"],
            y=status_counts["Count"],
            marker_color=[STATUS_COLORS[status] for status in status_counts["Status"]],
            text=status_counts["Count"],
            textposition="auto",
        )
    )
    chart.update_layout(
        height=360,
        margin=dict(l=10, r=10, t=20, b=10),
        yaxis_title="Members",
        xaxis_title="Status",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    st.plotly_chart(chart, use_container_width=True)

    st.subheader("Member Table With Tracker Fields")
    st.dataframe(merged, use_container_width=True, hide_index=True)

    st.download_button(
        "Download fabrication table",
        data=merged.to_csv(index=False).encode("utf-8"),
        file_name=f"steel_bridge_fabrication_{competition_year}.csv",
        mime="text/csv",
    )

with tab_schedule:
    st.subheader("Team Progress Tracker")
    tasks_for_editor = prepare_date_columns(tasks, ["DueDate"])
    edited_tasks = st.data_editor(
        tasks_for_editor,
        use_container_width=True,
        num_rows="dynamic",
        column_config={
            "Task": st.column_config.TextColumn("Task", required=True),
            "Owner": st.column_config.TextColumn("Owner"),
            "Priority": st.column_config.SelectboxColumn("Priority", options=TASK_PRIORITY_OPTIONS),
            "Status": st.column_config.SelectboxColumn("Status", options=TASK_STATUS_OPTIONS, required=True),
            "DueDate": st.column_config.DateColumn("Due date"),
            "Notes": st.column_config.TextColumn("Notes"),
        },
        key="tasks_editor",
    )

    task_button_cols = st.columns([1, 1, 4])
    if task_button_cols[0].button("Save tasks", type="primary"):
        save_csv_table(TASKS_PATH, edited_tasks)
        st.success(f"Saved to {TASKS_PATH}")
    with task_button_cols[1].popover("Rules tasks"):
        st.write("Create checklist items from the current YAML rule limits and deflection values.")
        generated_tasks = pd.DataFrame(build_rule_based_tasks(rules, int(competition_year)))
        if st.button("Append generated tasks"):
            combined_tasks = pd.concat([edited_tasks, generated_tasks], ignore_index=True)
            combined_tasks = combined_tasks.drop_duplicates(subset=["Task", "DueDate"], keep="first")
            save_csv_table(TASKS_PATH, combined_tasks)
            st.success("Added generated rule tasks. Refreshing the task table.")
            st.rerun()
        if st.button("Replace with generated tasks"):
            save_csv_table(TASKS_PATH, generated_tasks)
            st.success("Rebuilt the task list from the current rules. Refreshing the task table.")
            st.rerun()

    task_summary = (
        edited_tasks["Status"]
        .value_counts()
        .reindex(TASK_STATUS_OPTIONS, fill_value=0)
        .reset_index()
    )
    task_summary.columns = ["Status", "Count"]
    task_chart = go.Figure(
        go.Bar(
            x=task_summary["Status"],
            y=task_summary["Count"],
            marker_color=["#94a3b8", "#38bdf8", "#fb7185", "#22c55e"],
            text=task_summary["Count"],
            textposition="auto",
        )
    )
    task_chart.update_layout(
        height=320,
        margin=dict(l=10, r=10, t=20, b=10),
        yaxis_title="Tasks",
        xaxis_title="Status",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    st.plotly_chart(task_chart, use_container_width=True)

    st.subheader("Calendar and Deadlines")
    events_for_editor = prepare_date_columns(events, ["Date"])
    edited_events = st.data_editor(
        events_for_editor,
        use_container_width=True,
        num_rows="dynamic",
        column_config={
            "Event": st.column_config.TextColumn("Event", required=True),
            "Type": st.column_config.SelectboxColumn("Type", options=EVENT_TYPE_OPTIONS),
            "Date": st.column_config.DateColumn("Date"),
            "StartTime": st.column_config.TextColumn("Start time"),
            "EndTime": st.column_config.TextColumn("End time"),
            "Location": st.column_config.TextColumn("Location"),
            "Notes": st.column_config.TextColumn("Notes"),
        },
        key="events_editor",
    )

    calendar_cols = st.columns([1, 1.4, 3.6])
    if calendar_cols[0].button("Save calendar"):
        save_csv_table(EVENTS_PATH, edited_events)
        st.success(f"Saved to {EVENTS_PATH}")
    calendar_cols[1].download_button(
        "Download calendar file",
        data=build_calendar_ics(edited_events, edited_tasks, project_name),
        file_name=f"utc_steel_bridge_{competition_year}_calendar.ics",
        mime="text/calendar",
    )

    upcoming_events = edited_events.copy()
    upcoming_events["DateParsed"] = upcoming_events["Date"].apply(clean_date)
    upcoming_events = upcoming_events.dropna(subset=["DateParsed"]).sort_values("DateParsed")
    upcoming_events = upcoming_events[upcoming_events["DateParsed"] >= date.today()].head(8)
    if not upcoming_events.empty:
        st.subheader("Upcoming Dates")
        st.dataframe(
            upcoming_events.drop(columns=["DateParsed"]),
            use_container_width=True,
            hide_index=True,
        )

with tab_budget:
    st.subheader("Cost Estimation")
    costs_for_editor = prepare_date_columns(costs, ["NeededBy"])
    edited_costs = st.data_editor(
        costs_for_editor,
        use_container_width=True,
        num_rows="dynamic",
        column_config={
            "Item": st.column_config.TextColumn("Item", required=True),
            "Category": st.column_config.SelectboxColumn("Category", options=COST_CATEGORY_OPTIONS),
            "Quantity": st.column_config.NumberColumn("Quantity", min_value=0.0, step=1.0),
            "Unit": st.column_config.TextColumn("Unit"),
            "UnitCost": st.column_config.NumberColumn("Unit cost ($)", min_value=0.0, step=1.0, format="$%.2f"),
            "Vendor": st.column_config.TextColumn("Vendor"),
            "NeededBy": st.column_config.DateColumn("Needed by"),
            "Purchased": st.column_config.SelectboxColumn("Purchased", options=["No", "Yes"]),
            "Notes": st.column_config.TextColumn("Notes"),
        },
        key="costs_editor",
    )

    edited_cost_table = calculate_cost_table(edited_costs)
    purchased_cost = edited_cost_table.loc[edited_cost_table["Purchased"].eq("Yes"), "EstimatedCost"].sum()
    remaining_cost = edited_cost_table.loc[edited_cost_table["Purchased"].ne("Yes"), "EstimatedCost"].sum()

    budget_cols = st.columns(4)
    budget_cols[0].metric("Total estimate", f"${edited_cost_table['EstimatedCost'].sum():,.2f}")
    budget_cols[1].metric("Purchased", f"${purchased_cost:,.2f}")
    budget_cols[2].metric("Still needed", f"${remaining_cost:,.2f}")
    budget_cols[3].metric("Line items", len(edited_cost_table))

    if st.button("Save cost estimate", type="primary"):
        save_csv_table(COSTS_PATH, edited_costs)
        st.success(f"Saved to {COSTS_PATH}")

    category_summary = (
        edited_cost_table.groupby("Category", dropna=False)["EstimatedCost"]
        .sum()
        .reset_index()
        .sort_values("EstimatedCost", ascending=False)
    )
    if not category_summary.empty:
        cost_chart = go.Figure(
            go.Bar(
                x=category_summary["Category"],
                y=category_summary["EstimatedCost"],
                marker_color="#d6a94a",
                text=category_summary["EstimatedCost"].map(lambda value: f"${value:,.0f}"),
                textposition="auto",
            )
        )
        cost_chart.update_layout(
            height=340,
            margin=dict(l=10, r=10, t=20, b=10),
            yaxis_title="Estimated cost ($)",
            xaxis_title="Category",
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
        )
        st.plotly_chart(cost_chart, use_container_width=True)

    st.subheader("Budget Table With Calculated Totals")
    st.dataframe(edited_cost_table, use_container_width=True, hide_index=True)
    st.download_button(
        "Download cost estimate",
        data=edited_cost_table.to_csv(index=False).encode("utf-8"),
        file_name=f"steel_bridge_cost_estimate_{competition_year}.csv",
        mime="text/csv",
    )

with tab_archive:
    st.subheader("Reusable Season Library")
    st.write(
        "Save each competition year as a complete handoff folder so future UTC Steel Bridge teams can recover the rules, SAP2000 exports, compliance checks, fabrication tracker, schedule, and budget."
    )

    archive_actions = st.columns([1.2, 1.2, 3])
    if archive_actions[0].button("Save season record", type="primary"):
        saved_path = save_season_record(
            project_name,
            competition_year,
            joints,
            frames,
            checks,
            edited_tracker,
            rules,
            edited_tasks,
            edited_events,
            edited_costs,
            current_sources,
        )
        st.success(f"Saved the yearly handoff folder to {saved_path}")

    if archive_actions[1].button("Archive timestamped snapshot"):
        saved_path = archive_design(
            project_name,
            competition_year,
            joints,
            frames,
            checks,
            edited_tracker,
            rules,
            edited_tasks,
            edited_events,
            edited_costs,
        )
        st.success(f"Archived current design snapshot to {saved_path}")

    st.subheader("Previous Years")
    season_folders = list_season_records()
    if season_folders:
        selected_season = st.selectbox(
            "Open saved season",
            season_folders,
            format_func=lambda path: path.name,
        )
        st.download_button(
            "Download selected season package",
            data=make_folder_zip(selected_season),
            file_name=f"utc_steel_bridge_{selected_season.name}_handoff.zip",
            mime="application/zip",
        )

        manifest_path = selected_season / "manifest.yaml"
        if manifest_path.exists():
            st.subheader("Season Manifest")
            st.code(manifest_path.read_text(), language="yaml")

        saved_files = sorted([path for path in selected_season.rglob("*") if path.is_file()])
        if saved_files:
            st.subheader("Saved Files")
            file_rows = []
            for saved_file in saved_files:
                file_rows.append(
                    {
                        "File": str(saved_file.relative_to(selected_season)),
                        "Size KB": round(saved_file.stat().st_size / 1024, 1),
                        "Modified": datetime.fromtimestamp(saved_file.stat().st_mtime).strftime("%Y-%m-%d %H:%M"),
                    }
                )
            st.dataframe(pd.DataFrame(file_rows), use_container_width=True, hide_index=True)

            selected_file = st.selectbox(
                "Download one saved file",
                saved_files,
                format_func=lambda path: str(path.relative_to(selected_season)),
            )
            st.download_button(
                "Download file",
                data=read_file_bytes(selected_file),
                file_name=selected_file.name,
                mime="application/octet-stream",
            )
    else:
        st.info("No yearly records have been saved yet. Click 'Save season record' after uploading this year's rules and design exports.")

    st.subheader("Timestamped Design Snapshots")

    if ARCHIVE_DIR.exists():
        archive_rows = []
        for folder in sorted(ARCHIVE_DIR.iterdir(), reverse=True):
            if folder.is_dir():
                archive_rows.append(
                    {
                        "Archive": folder.name,
                        "Modified": datetime.fromtimestamp(folder.stat().st_mtime).strftime("%Y-%m-%d %H:%M"),
                        "Path": str(folder),
                    }
                )
        if archive_rows:
            st.dataframe(pd.DataFrame(archive_rows), use_container_width=True, hide_index=True)
        else:
            st.info("No archived designs have been saved yet.")
    else:
        st.info("No archived designs have been saved yet.")

with tab_data:
    st.subheader("Joint Data")
    st.dataframe(joints, use_container_width=True, hide_index=True)

    st.subheader("Frame Data")
    st.dataframe(frames, use_container_width=True, hide_index=True)

    st.download_button(
        "Download rule check results",
        data=checks.to_csv(index=False).encode("utf-8"),
        file_name=f"steel_bridge_rule_checks_{competition_year}.csv",
        mime="text/csv",
    )

with tab_workflow:
    st.subheader("Computational Tool Workflow")
    st.write(
        """
        This project uses Python for data cleaning and rule checking, Streamlit for the web-based user interface,
        SAP2000 Excel export tables as engineering model inputs, RStudio as the project development environment,
        and GitHub or another version-control workflow to preserve yearly updates.
        """
    )

    st.subheader("Responsible AI Use Note")
    st.write(
        """
        AI can help draft code, debug errors, and improve visual communication, but the engineering assumptions,
        rule interpretation, SAP2000 model setup, and final compliance decisions should be checked by the student
        and the UTC Steel Bridge team.
        """
    )

    st.subheader("How To Reuse Next Year")
    st.markdown(
        """
        1. Export the new SAP2000 joint coordinates and frame connectivity tables to Excel.
        2. Copy the current YAML file in the `rules` folder and update the yearly limits.
        3. Run the app, upload the new files and the official rule-book PDF, then generate the to-do list from the rules.
        4. Review failed checks, update the schedule and budget, and click `Save season record`.
        5. Download the season package and store it somewhere team-owned, such as a GitHub organization or permanent shared drive, instead of relying only on one student's school OneDrive.
        """
    )
