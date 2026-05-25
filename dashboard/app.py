"""dag-doctor — AI incident copilot for Apache Airflow.

Main entry point for the multi-page Streamlit dashboard.
"""

from __future__ import annotations

import os
import sys

# Ensure the dashboard package is importable from page modules
_dash_dir = os.path.dirname(os.path.abspath(__file__))
if _dash_dir not in sys.path:
    sys.path.insert(0, _dash_dir)

import streamlit as st  # noqa: E402

from utils import (  # noqa: E402
    AUTH_ENABLED,
    DEMO_MODE,
    init_session,
    render_login,
    render_sidebar,
)

st.set_page_config(
    page_title="dag-doctor",
    page_icon=":material/ecg_heart:",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        "Get help": None,
        "Report a bug": None,
        "About": "dag-doctor v0.1.0 — AI incident copilot for Airflow",
    },
)

# Hide Streamlit's default toolbar (Deploy, Main menu)
st.markdown(
    """
<style>
    [data-testid="stToolbar"] {display: none !important;}
    .stApp > header {display: none !important;}
    [data-testid="stDecoration"] {display: none !important;}
    [data-testid="stStatusWidget"] {display: none !important;}
    header[data-testid="stHeader"] {display: none !important;}
    #MainMenu {visibility: hidden !important;}
    footer {visibility: hidden !important;}
</style>
""",
    unsafe_allow_html=True,
)

init_session()

if AUTH_ENABLED and not st.session_state.get("auth_token"):
    render_login()
else:
    render_sidebar()

    if not DEMO_MODE and st.session_state.get("airflow_available") is False:
        st.warning(
            "Airflow is unreachable. Showing stored reports.", icon=":material/warning:"
        )

    # Define pages for enterprise navigation
    pages = {
        "": [
            st.Page(
                "app_pages/overview.py", title="Overview", icon=":material/dashboard:"
            ),
            st.Page(
                "app_pages/incidents.py", title="Incidents", icon=":material/queue:"
            ),
            st.Page(
                "app_pages/intelligence.py",
                title="Intelligence",
                icon=":material/psychology:",
            ),
            st.Page(
                "app_pages/reports.py", title="Reports", icon=":material/description:"
            ),
        ],
    }

    nav = st.navigation(pages, position="sidebar")
    nav.run()
