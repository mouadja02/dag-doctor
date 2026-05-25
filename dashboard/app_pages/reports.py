"""Reports page — stored reports browser with filtering."""

from __future__ import annotations

import streamlit as st

from utils import (
    FAILURE_ICON,
    SEVERITY_ICON,
    _sev_color,
    format_time_ago,
    get_reports_data,
)


st.header(":material/description: Reports")

# ── Filters ──
with st.form("report_filters", border=False):
    with st.container(horizontal=True):
        search_query = st.text_input(
            "Search",
            placeholder="Filter by DAG or task...",
            label_visibility="collapsed",
        )
        type_filter = st.pills(
            "Type",
            [
                "sql_error",
                "python_exception",
                "timeout",
                "permissions_auth",
                "missing_dependency",
                "infrastructure_resource",
                "schema_data_quality",
                "upstream_dependency",
            ],
            selection_mode="multi",
            label_visibility="collapsed",
        )
        st.form_submit_button(":material/filter_list: Filter", type="secondary")

st.session_state.reports_filters = {
    "search": search_query,
    "type": type_filter,
}

# ── Fetch & Filter ──
reports = get_reports_data()

filtered = reports
if search_query:
    q = search_query.lower()
    filtered = [
        r
        for r in filtered
        if q in r.get("dag_id", "").lower() or q in r.get("task_id", "").lower()
    ]
if type_filter:
    filtered = [r for r in filtered if r.get("failure_type", "unknown") in type_filter]

if not reports:
    st.info("No stored reports yet.", icon=":material/info:")
    st.stop()

if not filtered:
    st.caption("No reports match your filters.")
    st.stop()

st.caption(f"Showing {len(filtered)} of {len(reports)} reports")

for rep in filtered:
    ft = rep.get("failure_type", "unknown")
    icon = FAILURE_ICON.get(ft, ":material/help:")
    sev = rep.get("severity", "—")
    sev_icon = SEVERITY_ICON.get(sev, "")
    dt = str(rep.get("created_at", ""))[:19]
    ago = format_time_ago(rep.get("created_at"))

    with st.container(border=True):
        c1, c2 = st.columns([4, 1])
        with c1:
            st.markdown(
                f"{icon} {sev_icon} [{rep['dag_id']}] `{rep['task_id']}` — "
                f"**{ft.replace('_', ' ').title()}**"
            )
            st.caption(f"{ago}  ({dt})")
        with c2:
            sev_color = _sev_color(sev)
            if sev != "—":
                st.badge(sev.upper(), color=sev_color)
