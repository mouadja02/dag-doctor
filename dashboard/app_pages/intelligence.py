"""Intelligence page — clusters, trends, ownership analytics with charts."""

from __future__ import annotations

import altair as alt
import pandas as pd
import streamlit as st

from utils import (
    FAILURE_ICON,
    SEVERITY_ICON,
    TREND_ICON,
    _sev_color,
    _trend_color,
    format_time_ago,
    get_clusters_data,
    get_ownership_data,
)


st.header(":material/psychology: Intelligence")
st.caption("Recurring failures, ownership, and preventive recommendations.")

# ── Clusters ──
clusters = get_clusters_data()

with st.container(border=True):
    st.subheader(":material/account_tree: Recurring failure clusters")
    if clusters:
        cluster_df = pd.DataFrame(
            [
                {
                    "dag_id": c["dag_id"],
                    "task_id": c["task_id"],
                    "failure_type": c["failure_type"].replace("_", " ").title(),
                    "count": c["count"],
                    "trend": c.get("trend", "flat").title(),
                    "first_seen": str(c.get("first_seen", ""))[:19],
                    "last_seen": str(c.get("last_seen", ""))[:19],
                }
                for c in clusters
            ]
        )

        # Heatmap-style timeline: count vs dag/task
        heat_df = cluster_df.copy()
        heat_df["label"] = heat_df["dag_id"] + " / " + heat_df["task_id"]
        chart = (
            alt.Chart(heat_df)
            .mark_rect()
            .encode(
                x=alt.X("failure_type:N", title="Failure type"),
                y=alt.Y("label:N", title="DAG / Task"),
                color=alt.Color(
                    "count:Q",
                    title="Occurrences",
                    scale=alt.Scale(scheme="reds"),
                ),
                tooltip=[
                    "label:N",
                    "failure_type:N",
                    "count:Q",
                    "trend:N",
                ],
            )
            .properties(height=max(200, len(heat_df) * 35))
        )
        st.altair_chart(chart, use_container_width=True)

        st.space("small")
        for _, c in cluster_df.iterrows():
            trend_val = str(c["trend"]).lower()
            trend_icon = TREND_ICON.get(trend_val, ":material/trending_flat:")
            trend_color = _trend_color(trend_val)
            with st.container(border=True):
                cc1, cc2, cc3 = st.columns([3, 1, 1])
                with cc1:
                    st.markdown(
                        f"**{c['dag_id']}** / `{c['task_id']}` — {c['failure_type']}"
                    )
                with cc2:
                    st.metric("Count", int(c["count"]), border=True)
                with cc3:
                    st.badge(
                        str(c["trend"]).title(),
                        icon=trend_icon,
                        color=trend_color,
                    )
                st.caption(f"First: {c['first_seen']} | Last: {c['last_seen']}")
    else:
        st.info(
            "No recurring failure clusters yet. Run more analyses to build clusters.",
            icon=":material/info:",
        )

st.space("medium")

# ── Ownership ──
ownership = get_ownership_data()
with st.container(border=True):
    st.subheader(":material/badge: Ownership distribution")
    if ownership.get("top_owner") and ownership["top_owner"] != "N/A":
        owner_counts = ownership.get("owner_counts", {})
        if owner_counts:
            owner_df = pd.DataFrame(
                [
                    {"owner": k.replace("-", " ").title(), "failures": v}
                    for k, v in sorted(
                        owner_counts.items(), key=lambda x: x[1], reverse=True
                    )
                ]
            )
            chart = (
                alt.Chart(owner_df)
                .mark_bar()
                .encode(
                    x=alt.X(
                        "owner:N",
                        title="Owner",
                        sort=alt.EncodingSortField(
                            field="failures", order="descending"
                        ),
                    ),
                    y=alt.Y("failures:Q", title="Failure count"),
                    color=alt.Color(
                        "owner:N",
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
                    tooltip=["owner:N", "failures:Q"],
                )
                .properties(height=280)
            )
            st.altair_chart(chart, use_container_width=True)

            st.space("small")
            with st.container(horizontal=True):
                for owner, count in owner_counts.items():
                    st.metric(owner.replace("-", " ").title(), count, border=True)
        else:
            st.caption("No ownership counts available.")
    else:
        st.info(
            "No ownership data available yet.",
            icon=":material/info:",
        )

st.space("medium")

# ── Prevention recommendations (placeholder scaffold) ──
with st.container(border=True):
    st.subheader(":material/shield: Prevention recommendations")
    st.caption("Top preventive actions based on recent failure patterns.")
    if clusters:
        for c in clusters[:3]:
            ft = c.get("failure_type", "unknown")
            icon = FAILURE_ICON.get(ft, ":material/help:")
            with st.container(border=True):
                c1, c2 = st.columns([4, 1])
                with c1:
                    st.markdown(
                        f"{icon} **{ft.replace('_', ' ').title()}** in `{c['dag_id']}` / `{c['task_id']}`"
                    )
                    st.caption(
                        f"Seen {c['count']} times · Trend: {c.get('trend', 'flat')}"
                    )
                with c2:
                    st.badge("Action", icon=":material/build:", color="blue")
    else:
        st.info(
            "Run analyses to generate preventive recommendations.",
            icon=":material/info:",
        )
