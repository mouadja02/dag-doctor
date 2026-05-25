"""Overview page — KPIs, summary charts, and recent incidents."""

from __future__ import annotations

import altair as alt
import pandas as pd
import streamlit as st

from utils import (
    FAILURE_ICON,
    SEVERITY_ICON,
    STATUS_ICON,
    _sev_color,
    _status_color,
    format_time_ago,
    get_clusters_data,
    get_incidents_data,
    get_metrics_data,
    get_reports_data,
)


def _build_category_df(incidents: list[dict]) -> pd.DataFrame:
    rows: list[dict] = []
    for inc in incidents:
        ft = (
            inc.get("classification", {}).get("failure_type", "unknown")
            if isinstance(inc.get("classification"), dict)
            else "unknown"
        )
        rows.append({"category": ft.replace("_", " ").title()})
    return pd.DataFrame(rows)


def _build_severity_df(incidents: list[dict]) -> pd.DataFrame:
    rows: list[dict] = []
    for inc in incidents:
        sev = inc.get("severity", "unknown")
        rows.append({"severity": sev.upper()})
    return pd.DataFrame(rows)


def _build_time_series(reports: list[dict]) -> pd.DataFrame:
    """Build a daily time-series from stored reports."""
    rows: list[dict] = []
    for rep in reports:
        dt = rep.get("created_at")
        if dt:
            try:
                day = dt[:10]
                rows.append({"date": day})
            except Exception:
                pass
    if not rows:
        return pd.DataFrame({"date": [], "count": []})
    df = pd.DataFrame(rows)
    df = df.groupby("date").size().to_frame(name="count").reset_index()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")
    return df


def _build_owner_df(ownership: dict) -> pd.DataFrame:
    counts = ownership.get("owner_counts", {})
    if not counts:
        return pd.DataFrame({"owner": [], "failures": []})
    rows = [
        {"owner": k.replace("-", " ").title(), "failures": v} for k, v in counts.items()
    ]
    return pd.DataFrame(rows).sort_values("failures", ascending=False)


st.header(":material/dashboard: Overview")

# ── KPI Row ──
metrics = get_metrics_data()
failed = metrics.get("failed_today", 0)
diag = metrics.get("avg_diagnosis_time_seconds", 0)
category = metrics.get("top_failure_category", "N/A")
recurring = metrics.get("recurring_candidates", 0)

# Sparkline data: pull from reports for trend context
reports = get_reports_data()
ts_df = _build_time_series(reports)
spark_values = ts_df["count"].tolist() if not ts_df.empty else [failed]

with st.container(horizontal=True):
    st.metric(
        ":material/warning: Failed today",
        failed,
        border=True,
        chart_data=spark_values,
        chart_type="line",
    )
    st.metric(
        ":material/schedule: Avg diagnosis time",
        f"{diag}s",
        border=True,
    )
    cat_icon = FAILURE_ICON.get(category.lower().replace(" ", "_"), ":material/help:")
    st.metric(
        f"{cat_icon} Top category",
        category if category != "N/A" else "—",
        border=True,
    )
    st.metric(
        ":material/repeat: Recurring candidates",
        recurring,
        border=True,
    )

st.space("medium")

# ── Charts Row ──
incidents = get_incidents_data()

# If live mode has no incidents but reports exist, synthesise from reports
if not incidents and reports:
    for rep in reports[:20]:
        incidents.append(
            {
                "dag_id": rep.get("dag_id", "—"),
                "dag_run_id": rep.get("dag_run_id", "—"),
                "state": "failed",
                "logical_date": rep.get("created_at"),
                "severity": rep.get("severity", "medium"),
                "classification": {"failure_type": rep.get("failure_type", "unknown")},
            }
        )

left, right = st.columns(2)

with left:
    with st.container(border=True):
        st.subheader(":material/trending_up: Incidents over time")
        if not ts_df.empty:
            chart = (
                alt.Chart(ts_df)
                .mark_area(opacity=0.4, line=True)
                .encode(
                    x=alt.X("date:T", title="Date"),
                    y=alt.Y("count:Q", title="Incidents"),
                    tooltip=[
                        alt.Tooltip("date:T", title="Date"),
                        alt.Tooltip("count:Q", title="Count"),
                    ],
                )
                .properties(height=250)
            )
            st.altair_chart(chart, use_container_width=True)
        else:
            st.caption("No historical data available.")

with right:
    with st.container(border=True):
        st.subheader(":material/donut_large: Failure category breakdown")
        cat_df = _build_category_df(incidents)
        if not cat_df.empty:
            cat_counts = (
                cat_df.groupby("category").size().to_frame(name="count").reset_index()
            )
            chart = (
                alt.Chart(cat_counts)
                .mark_arc(innerRadius=50)
                .encode(
                    theta=alt.Theta("count:Q", title="Count"),
                    color=alt.Color(
                        "category:N",
                        title="Category",
                        scale=alt.Scale(
                            range=[
                                "#2563EB",
                                "#16A34A",
                                "#7C3AED",
                                "#EA580C",
                                "#DC2626",
                                "#CA8A04",
                                "#64748B",
                            ]
                        ),
                    ),
                    tooltip=["category:N", "count:Q"],
                )
                .properties(height=250)
            )
            st.altair_chart(chart, use_container_width=True)
        else:
            st.caption("No incidents to categorise.")

st.space("medium")

# ── Second charts row ──
left2, right2 = st.columns(2)

with left2:
    with st.container(border=True):
        st.subheader(":material/scale: Severity distribution")
        sev_df = _build_severity_df(incidents)
        if not sev_df.empty:
            sev_counts = (
                sev_df.groupby("severity").size().to_frame(name="count").reset_index()
            )
            chart = (
                alt.Chart(sev_counts)
                .mark_bar()
                .encode(
                    x=alt.X(
                        "severity:N", title="Severity", sort=["HIGH", "MEDIUM", "LOW"]
                    ),
                    y=alt.Y("count:Q", title="Count"),
                    color=alt.Color(
                        "severity:N",
                        scale=alt.Scale(
                            domain=["HIGH", "MEDIUM", "LOW"],
                            range=["#DC2626", "#EA580C", "#2563EB"],
                        ),
                    ),
                    tooltip=["severity:N", "count:Q"],
                )
                .properties(height=220)
            )
            st.altair_chart(chart, use_container_width=True)
        else:
            st.caption("No incidents to display.")

with right2:
    with st.container(border=True):
        st.subheader(":material/badge: Ownership distribution")
        ownership = get_clusters_data()  # fallback: clusters have owner info indirectly
        # Use reports as proxy for ownership if endpoint is sparse
        owner_df = _build_owner_df({"owner_counts": {}})
        # Build owner counts from incidents
        owner_counts: dict[str, int] = {}
        for inc in incidents:
            owner = inc.get("owner", "unknown")
            owner_counts[owner] = owner_counts.get(owner, 0) + 1
        if owner_counts:
            owner_df = pd.DataFrame(
                [
                    {"owner": k.replace("-", " ").title(), "failures": v}
                    for k, v in sorted(
                        owner_counts.items(), key=lambda x: x[1], reverse=True
                    )
                ]
            )
        if not owner_df.empty:
            chart = (
                alt.Chart(owner_df)
                .mark_bar()
                .encode(
                    x=alt.X("failures:Q", title="Failures"),
                    y=alt.Y(
                        "owner:N",
                        title="Owner",
                        sort=alt.EncodingSortField(
                            field="failures", order="descending"
                        ),
                    ),
                    tooltip=["owner:N", "failures:Q"],
                )
                .properties(height=220)
            )
            st.altair_chart(chart, use_container_width=True)
        else:
            st.caption("No ownership data available.")

st.space("medium")

# ── Recent incidents ──
with st.container(border=True):
    st.subheader(":material/queue: Recent incidents")
    if not incidents:
        st.info("No failed incidents found.", icon=":material/info:")
    else:
        for inc in incidents[:6]:
            dag_id = inc.get("dag_id", "—")
            run_id = inc.get("dag_run_id", inc.get("run_id", "—"))
            state = inc.get("state", inc.get("status", "failed")).upper()
            date_raw = inc.get("logical_date", "")
            date_str = str(date_raw)[:19] if date_raw else "—"
            ago = format_time_ago(date_raw)
            sev = inc.get("severity", "medium")
            sev_color = _sev_color(sev)
            rec = inc.get("recurrence_count", 0)
            short_run = run_id[:28] + "…" if len(run_id) > 28 else run_id

            c1, c2, c3 = st.columns([3, 2, 2])
            with c1:
                st.markdown(f":material/receipt_long: **{dag_id}**")
                st.caption(f"`{short_run}`")
            with c2:
                st.caption(f"{ago}  ({date_str})")
                if rec:
                    st.caption(f":material/repeat: {rec}x recurring")
            with c3:
                st.badge(
                    state,
                    icon=STATUS_ICON.get(state.lower(), ":material/help:"),
                    color=_status_color(state.lower()),
                )
                st.badge(sev.upper(), icon=SEVERITY_ICON.get(sev, ""), color=sev_color)
