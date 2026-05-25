"""Incidents page — full queue with search, filter, sort, and detail/analysis panel."""

from __future__ import annotations

import streamlit as st

from utils import (
    FAILURE_ICON,
    SEVERITY_ICON,
    STATUS_ICON,
    DEMO_MODE,
    _sev_color,
    _status_color,
    api_post,
    format_time_ago,
    get_incidents_data,
    get_run_detail,
)


def _incident_key(inc: dict) -> str:
    return inc.get("id") or inc.get("dag_run_id") or inc.get("run_id", "")


st.header(":material/queue: Incidents")

# ── Search / Filter Bar ──
with st.form("incident_filters", border=False):
    with st.container(horizontal=True):
        search_query = st.text_input(
            "Search DAG",
            placeholder="Filter by DAG name...",
            label_visibility="collapsed",
        )
        severity_filter = st.pills(
            "Severity",
            ["high", "medium", "low"],
            selection_mode="multi",
            label_visibility="collapsed",
        )
        st.form_submit_button(":material/filter_list: Filter", type="secondary")

# Persist filters in session state so they survive reruns
st.session_state.incidents_filters = {
    "search": search_query,
    "severity": severity_filter,
}

# ── Fetch & Filter ──
incidents = get_incidents_data()

filtered = incidents
if search_query:
    q = search_query.lower()
    filtered = [inc for inc in filtered if q in inc.get("dag_id", "").lower()]
if severity_filter:
    filtered = [
        inc for inc in filtered if inc.get("severity", "").lower() in severity_filter
    ]

if not filtered and not incidents:
    if not DEMO_MODE and not st.session_state.get("airflow_available"):
        st.info(
            "Airflow is unavailable — no live incidents to display.",
            icon=":material/info:",
        )
    else:
        st.info("No failed incidents found.", icon=":material/info:")
    # No detail panel when empty
    st.stop()

if not filtered:
    st.caption("No incidents match your filters.")
    st.stop()

# ── Master / Detail Layout ──
list_col, detail_col = st.columns([2, 3])

with list_col:
    st.caption(f"Showing {len(filtered)} of {len(incidents)} incidents")
    for inc in filtered:
        dag_id = inc.get("dag_id", "—")
        run_id = inc.get("dag_run_id", inc.get("run_id", "—"))
        state = inc.get("state", inc.get("status", "failed")).upper()
        date_raw = inc.get("logical_date", "")
        ago = format_time_ago(date_raw)
        sev = inc.get("severity", "medium")
        sev_color = _sev_color(sev)
        rec = inc.get("recurrence_count", 0)
        short_run = run_id[:28] + "…" if len(run_id) > 28 else run_id
        inc_key = _incident_key(inc)

        with st.container(border=True):
            c1, c2 = st.columns([3, 1])
            with c1:
                st.markdown(f":material/receipt_long: **{dag_id}**")
                st.caption(f"`{short_run}` · {ago}")
                if rec:
                    st.caption(f":material/repeat: {rec}x")
            with c2:
                st.badge(
                    state,
                    icon=STATUS_ICON.get(state.lower(), ":material/help:"),
                    color=_status_color(state.lower()),
                )
                st.badge(
                    sev.upper(),
                    icon=SEVERITY_ICON.get(sev, ""),
                    color=sev_color,
                )
                if st.button(
                    "Inspect",
                    icon=":material/search:",
                    key=f"inspect_{inc_key}",
                    use_container_width=True,
                    type="primary",
                ):
                    st.session_state.selected_run = inc
                    st.session_state.selected_task = None
                    st.session_state.analysis_result = None
                    st.session_state.ticket_payload = None
                    # Deep-link via query param
                    st.query_params["incident"] = inc_key
                    st.rerun()

with detail_col:
    # If query param exists but no selection, try to restore
    if not st.session_state.get("selected_run") and st.query_params.get("incident"):
        target_key = st.query_params["incident"]
        for inc in incidents:
            if _incident_key(inc) == target_key:
                st.session_state.selected_run = inc
                break

    incident = st.session_state.get("selected_run")
    if not incident:
        st.info(
            "Select an incident from the list to inspect details.",
            icon=":material/info:",
        )
        st.stop()

    dag_id = incident.get("dag_id", "—")
    run_id = incident.get("dag_run_id", incident.get("run_id", ""))
    task_id = incident.get("task_id")
    classification = incident.get("classification", {}) or {}
    failure_type = classification.get("failure_type", "unknown")
    state = incident.get("state", incident.get("status", "unknown")).upper()

    # Breadcrumb
    st.caption(f":material/arrow_back: [Back to incidents](/incidents)")
    st.markdown(f"### :material/receipt_long: {dag_id}")

    with st.container(border=True):
        with st.container(horizontal=True):
            st.metric(":material/flag: State", state, border=True)
            short_run = run_id[:24] + "…" if len(run_id) > 24 else run_id
            st.metric(":material/fingerprint: Run ID", short_run, border=True)
            dt = str(incident.get("logical_date", ""))[:19] or "—"
            st.metric(":material/calendar_today: Date", dt, border=True)
            rec = incident.get("recurrence_count", 0)
            st.metric(
                ":material/repeat: Recurring", f"{rec}x" if rec else "—", border=True
            )

        if failure_type != "unknown":
            type_icon = FAILURE_ICON.get(failure_type, ":material/help:")
            sev_icon = SEVERITY_ICON.get(
                incident.get("severity", "medium"), ":material/warning:"
            )
            conf = classification.get("confidence", 0)
            with st.container(horizontal=True):
                st.metric(
                    f"{type_icon} Type",
                    failure_type.replace("_", " ").title(),
                    border=True,
                )
                st.metric(":material/analytics: Confidence", f"{conf:.0%}", border=True)
                st.metric(
                    f"{sev_icon} Severity",
                    incident.get("severity", "medium").upper(),
                    border=True,
                )

        # Failed task instances
        failed_tasks = []
        if not DEMO_MODE and dag_id and run_id:
            detail = get_run_detail(dag_id, run_id)
            task_instances = detail.get("task_instances", [])
            failed_tasks = [ti for ti in task_instances if ti.get("state") == "failed"]

        if failed_tasks:
            st.space("small")
            with st.container(border=True):
                st.markdown("**:material/list: Failed tasks in this run**")
                for ft in failed_tasks:
                    ft_task_id = ft.get("task_id", "—")
                    fc1, fc2 = st.columns([3, 1])
                    with fc1:
                        st.markdown(
                            f":material/bug_report: `{ft_task_id}` — {ft.get('operator', 'unknown')}"
                        )
                    with fc2:
                        if st.button(
                            "Analyze",
                            icon=":material/play_arrow:",
                            key=f"analyze_{dag_id}_{ft_task_id}",
                            use_container_width=True,
                            type="primary",
                        ):
                            with st.spinner(f"Analyzing {ft_task_id}..."):
                                result = api_post(
                                    "/analyze",
                                    {
                                        "dag_id": dag_id,
                                        "run_id": run_id,
                                        "task_id": ft_task_id,
                                        "try_number": ft.get("try_number", 1),
                                    },
                                )
                                if result:
                                    st.session_state.selected_task = ft_task_id
                                    st.session_state.analysis_result = result
                                    st.rerun()

        # Demo analyse button
        if DEMO_MODE and task_id:
            st.space("small")
            if st.button(
                "Analyze with dag-doctor",
                icon=":material/health_and_safety:",
                type="primary",
                use_container_width=True,
            ):
                with st.spinner("Analysing..."):
                    inc_id = incident.get("id", "")
                    result = api_post("/demo/analyze", {"incident_id": inc_id})
                    if result:
                        st.session_state.selected_task = task_id
                        st.session_state.analysis_result = result
                        st.rerun()

    # ── Analysis Result ──
    result = st.session_state.get("analysis_result")
    if result:
        task_label = task_id or result.get("task_id", "—")
        st.space("medium")
        st.subheader(
            f":material/health_and_safety: Analysis: `{dag_id}` / `{task_label}`"
        )

        explanation = result.get("explanation", {})
        evidence_list = result.get("evidence", [])
        sev = result.get("severity", incident.get("severity", "medium"))

        col_left, col_right = st.columns(2)
        with col_left:
            with st.container(border=True):
                st.markdown("**:material/search: Evidence from logs**")
                if evidence_list:
                    for item in evidence_list:
                        signal_label = (
                            item.get("signal_type", "").replace("_", " ").title()
                        )
                        st.caption(f"**{signal_label}**")
                        source = item.get("source_line", "")
                        if source:
                            st.code(source, language="text")
                        context = item.get("context_lines", [])
                        if context:
                            with st.expander("Context", icon=":material/expand_more:"):
                                st.code("\n".join(context), language="text")
                else:
                    st.info("No evidence extracted.", icon=":material/info:")

        with col_right:
            with st.container(border=True):
                st.markdown("**:material/healing: Safe remediation**")
                sev_icon = SEVERITY_ICON.get(sev, ":material/warning:")
                sev_color = _sev_color(sev)
                st.badge(sev.upper(), icon=sev_icon, color=sev_color)
                st.space("small")
                if explanation:
                    if explanation.get("root_cause"):
                        with st.expander(
                            "Root cause", expanded=True, icon=":material/help:"
                        ):
                            st.markdown(explanation["root_cause"])
                    steps = explanation.get("remediation_steps", [])
                    if steps:
                        st.markdown("**Remediation steps**")
                        for step in steps:
                            tag = (
                                ":material/gpp_maybe:"
                                if sev == "high"
                                else ":material/gpp_good:"
                            )
                            st.markdown(f"- {tag} {step}")
                    what_not = explanation.get("what_not_to_do", [])
                    if what_not:
                        with st.expander("What not to do", icon=":material/warning:"):
                            for item in what_not:
                                st.markdown(f"- {item}")

        st.space("medium")
        with st.container(border=True):
            ca1, ca2 = st.columns([2, 1])
            with ca1:
                channel = st.selectbox(
                    "Integration channel",
                    options=["slack", "jira", "github"],
                    format_func=lambda x: {
                        "slack": "Slack",
                        "jira": "Jira",
                        "github": "GitHub",
                    }[x],
                    key=f"channel_{incident.get('id', '')}",
                )
            with ca2:
                st.space("small")
                if st.button(
                    "Create ticket",
                    icon=":material/confirmation_number:",
                    type="secondary",
                    key=f"ticket_{incident.get('id', '')}",
                    use_container_width=True,
                ):
                    if DEMO_MODE:
                        payload = api_post(
                            "/demo/ticket", {"incident_id": incident.get("id", "")}
                        )
                        if payload:
                            st.session_state.ticket_payload = payload
                            st.toast(
                                f"Ticket {payload.get('ticket_id', 'unknown')} created!"
                            )
                            st.rerun()
                    else:
                        with st.spinner(f"Notifying via {channel}..."):
                            report_id = result.get("id")
                            if report_id:
                                resp = api_post(
                                    "/integrations/notify",
                                    {"incident_id": report_id, "channel": channel},
                                )
                                if resp and resp.get("status") == "sent":
                                    st.toast(f"Posted to {channel}!")
                                    st.session_state.ticket_payload = resp
                                    st.rerun()
                                elif resp and resp.get("issue"):
                                    st.toast(
                                        f"GitHub issue #{resp['issue'].get('issue_number')} created!"
                                    )
                                    st.session_state.ticket_payload = resp
                                    st.rerun()
                                else:
                                    st.error(f"Failed to notify via {channel}")

        ticket = st.session_state.get("ticket_payload")
        if ticket:
            with st.expander(
                f":material/confirmation_number: Ticket {ticket.get('ticket_id', '')} — payload",
                icon=":material/expand_more:",
            ):
                st.json(ticket)

        report_md = result.get("report_markdown", "")
        if report_md:
            st.download_button(
                "Download report",
                report_md,
                file_name=f"incident_report_{dag_id}_{task_label}.md",
                mime="text/markdown",
                key=f"download_{incident.get('id', '')}",
                icon=":material/download:",
                use_container_width=True,
            )
            with st.expander("Full report", icon=":material/description:"):
                st.markdown(report_md)
