import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

st.set_page_config(
    page_title="WARN Act Tracker",
    page_icon="⚠️",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
[data-testid="stMetric"] {
    background:#fff;border:1px solid #e5e7eb;border-radius:12px;padding:16px 20px;
}
[data-testid="stMetricLabel"] p {
    font-size:11px!important;font-weight:700!important;
    letter-spacing:.06em;text-transform:uppercase;color:#6b7280!important;
}
[data-testid="stMetricValue"] {
    font-size:26px!important;font-weight:800!important;color:#111827!important;
}
[data-testid="stMetricDelta"] { display:none; }
.block-container { padding-top:1.25rem!important; }
</style>
""", unsafe_allow_html=True)

STATES_META = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
    "DC": "District of Columbia", "FL": "Florida", "GA": "Georgia", "HI": "Hawaii",
    "ID": "Idaho", "IL": "Illinois", "IN": "Indiana", "IA": "Iowa",
    "KS": "Kansas", "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine",
    "MD": "Maryland", "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota",
    "MS": "Mississippi", "MO": "Missouri", "MT": "Montana", "NE": "Nebraska",
    "NV": "Nevada", "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico",
    "NY": "New York", "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio",
    "OK": "Oklahoma", "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island",
    "SC": "South Carolina", "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas",
    "UT": "Utah", "VT": "Vermont", "VA": "Virginia", "WA": "Washington",
    "WV": "West Virginia", "WI": "Wisconsin", "WY": "Wyoming",
}

DATA_URL = (
    "https://raw.githubusercontent.com/amp2026/Warn-Tracker/main"
    "/data/processed/consolidated.csv"
)

CLR = dict(red="#ef4444", orange="#f97316", blue="#3b82f6",
           purple="#8b5cf6", green="#10b981")

CHART_LAYOUT = dict(
    plot_bgcolor="#fff", paper_bgcolor="#fff",
    margin=dict(t=44, b=10, l=40, r=10),
    legend=dict(orientation="h", y=-0.28, font=dict(size=11)),
    font=dict(family="Inter, system-ui, sans-serif", size=11),
)


# ── Data loading ───────────────────────────────────────────────────────────

@st.cache_data(ttl=3600)
def load_data() -> pd.DataFrame:
    df = pd.read_csv(DATA_URL)
    df.columns = [c.lower().replace(" ", "_").replace("-", "_") for c in df.columns]
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    # Normalise "type" column
    if "type" not in df.columns:
        for alt in ("layoff_type", "event_type", "notice_type", "action_type",
                    "type_of_layoff"):
            if alt in df.columns:
                df["type"] = df[alt]
                break
        else:
            df["type"] = "Layoff"

    # Normalise "city" column
    if "city" not in df.columns:
        for alt in ("city_name", "municipality", "facility_city", "location"):
            if alt in df.columns:
                df["city"] = df[alt]
                break
        else:
            df["city"] = ""

    df["workers"] = pd.to_numeric(df.get("workers", 0), errors="coerce").fillna(0).astype(int)
    df["company"] = df.get("company", pd.Series(dtype=str)).fillna("Unknown")
    df["state"]   = df.get("state",   pd.Series(dtype=str)).fillna("??")
    df["city"]    = df["city"].fillna("")
    df["type"]    = df["type"].fillna("Layoff")

    if "scraped_at" in df.columns:
        df["scraped_at"] = pd.to_datetime(df["scraped_at"], errors="coerce", utc=True)

    return (df.dropna(subset=["date"])
              .sort_values("date", ascending=False)
              .reset_index(drop=True))


# ── Helper: month grouping (works with all pandas versions) ────────────────

def monthly_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """Aggregate notices + workers by calendar month."""
    if frame.empty:
        return pd.DataFrame(columns=["period", "label", "notices", "workers"])
    f = frame.copy()
    f["period"] = f["date"].dt.to_period("M")
    m = (f.groupby("period", sort=True)
          .agg(notices=("company", "count"), workers=("workers", "sum"))
          .reset_index())
    m["label"] = m["period"].dt.strftime("%b %y")
    return m


def monthly_median_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=["period", "label", "median"])
    f = frame.copy()
    f["period"] = f["date"].dt.to_period("M")
    m = (f.groupby("period", sort=True)["workers"]
          .median()
          .reset_index())
    m.columns = ["period", "median"]
    m["label"] = m["period"].dt.strftime("%b %y")
    return m


def bar_chart(df_plot, x, y, title, color, height=280, horizontal=False):
    kwargs = dict(color_discrete_sequence=[color])
    if horizontal:
        fig = px.bar(df_plot, x=y, y=x, orientation="h", title=title,
                     labels={y: y.replace("_", " ").title(), x: ""}, **kwargs)
        fig.update_xaxes(gridcolor="#f3f4f6")
        fig.update_yaxes(showgrid=False)
    else:
        fig = px.bar(df_plot, x=x, y=y, title=title,
                     labels={x: "", y: y.replace("_", " ").title()}, **kwargs)
        fig.update_xaxes(showgrid=False)
        fig.update_yaxes(gridcolor="#f3f4f6")
    fig.update_traces(marker_line_width=0)
    fig.update_layout(height=height, showlegend=False, **CHART_LAYOUT)
    return fig


# ── Load ───────────────────────────────────────────────────────────────────

try:
    raw = load_data()
except Exception:
    st.info("⏳ Waiting for first data build — run the GitHub Action to start.")
    st.stop()

if raw.empty:
    st.info("⏳ No data yet — run the GitHub Action to populate the dataset.")
    st.stop()


# ── Page header ────────────────────────────────────────────────────────────

hdr_l, hdr_r = st.columns([5, 1])
with hdr_l:
    st.markdown("## ⚠️ WARN Act Tracker")
    if "scraped_at" in raw.columns and raw["scraped_at"].notna().any():
        last = raw["scraped_at"].max().strftime("%b %d, %Y")
    else:
        last = raw["date"].max().strftime("%b %d, %Y")
    st.caption(f"Data from state WARN filings · Last updated {last}")
with hdr_r:
    if st.button("🔄 Refresh", width="stretch"):
        st.cache_data.clear()
        st.rerun()


# ── Sidebar filters ────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("## 🔍 Filters")

    search = st.text_input("Search", placeholder="Company, city, or state…")

    state_list   = sorted(raw["state"].dropna().unique())
    state_labels = ["All States"] + [f"{s} – {STATES_META.get(s, s)}" for s in state_list]
    state_sel    = st.selectbox("State", state_labels)
    state_code   = None if state_sel == "All States" else state_sel.split(" – ")[0]

    type_filter = st.selectbox("Type", ["All Types", "Layoff", "Closure"])

    st.markdown("**Date range**")
    min_d = raw["date"].dt.date.min()
    max_d = raw["date"].dt.date.max()
    d_from = st.date_input("From", value=min_d, min_value=min_d, max_value=max_d)
    d_to   = st.date_input("To",   value=max_d, min_value=min_d, max_value=max_d)

    if st.button("✕  Clear filters", width="stretch"):
        st.rerun()


# ── Apply filters ──────────────────────────────────────────────────────────

df = raw.copy()
if search:
    q  = search.lower()
    df = df[df["company"].str.lower().str.contains(q, na=False)
          | df["city"].str.lower().str.contains(q, na=False)
          | df["state"].str.lower().str.contains(q, na=False)]
if state_code:
    df = df[df["state"] == state_code]
if type_filter != "All Types":
    df = df[df["type"] == type_filter]
df = df[(df["date"].dt.date >= d_from) & (df["date"].dt.date <= d_to)]


# ── Stat cards ─────────────────────────────────────────────────────────────

c1, c2, c3, c4 = st.columns(4)
c1.metric("📈 Total Notices",    f"{len(df):,}")
c2.metric("👥 Workers Affected", f"{df['workers'].sum():,}")
c3.metric("🏢 Companies",        f"{df['company'].nunique():,}")
c4.metric("📍 States + DC",      f"{df['state'].nunique():,}")
st.markdown("<div style='height:8px'/>", unsafe_allow_html=True)


# ── Tabs ───────────────────────────────────────────────────────────────────

t_dash, t_notices, t_states, t_companies, t_db = st.tabs([
    "📊 Dashboard", "📋 All Notices", "📍 States", "🏢 Companies", "🗄️ Database",
])


# ═══════════════════════════════════════════════════════════════════════════
# DASHBOARD
# ═══════════════════════════════════════════════════════════════════════════

with t_dash:
    monthly = monthly_frame(df)

    # Row 1 — monthly notices + workers
    r1a, r1b = st.columns(2)
    with r1a:
        st.plotly_chart(bar_chart(monthly, "label", "notices",
                                  "Monthly WARN Notices", CLR["red"]),
                        width='stretch')
    with r1b:
        st.plotly_chart(bar_chart(monthly, "label", "workers",
                                  "Workers Affected Over Time", CLR["orange"]),
                        width='stretch')

    # Row 2 — stacked Layoffs vs Closures
    df_l = monthly_frame(df[df["type"] == "Layoff"]).rename(
        columns={"notices": "layoffs", "workers": "layoff_workers"})
    df_c = monthly_frame(df[df["type"] == "Closure"]).rename(
        columns={"notices": "closures", "workers": "closure_workers"})

    def stacked_bar(title, l_df, c_df, l_col, c_col, l_name, c_name, height=300):
        fig = go.Figure()
        if not l_df.empty:
            fig.add_trace(go.Bar(x=l_df["label"], y=l_df[l_col],
                                 name=l_name, marker_color=CLR["blue"],
                                 marker_line_width=0))
        if not c_df.empty:
            fig.add_trace(go.Bar(x=c_df["label"], y=c_df[c_col],
                                 name=c_name, marker_color=CLR["red"],
                                 marker_line_width=0))
        fig.update_layout(title=title, barmode="stack", height=height,
                          **CHART_LAYOUT)
        fig.update_xaxes(showgrid=False)
        fig.update_yaxes(gridcolor="#f3f4f6")
        return fig

    r2a, r2b = st.columns(2)
    with r2a:
        st.plotly_chart(
            stacked_bar("Layoffs vs Closures — Notices",
                        df_l, df_c, "layoffs", "closures",
                        "Layoffs", "Closures"),
            width='stretch')
    with r2b:
        st.plotly_chart(
            stacked_bar("Layoffs vs Closures — Workers",
                        df_l, df_c, "layoff_workers", "closure_workers",
                        "Layoffs", "Closures"),
            width='stretch')

    # Row 3 — median workers line chart
    med_all     = monthly_median_frame(df)
    med_layoff  = monthly_median_frame(df[df["type"] == "Layoff"])
    med_closure = monthly_median_frame(df[df["type"] == "Closure"])

    fig_med = go.Figure()
    fig_med.add_trace(go.Scatter(
        x=med_all["label"], y=med_all["median"], name="All Notices",
        mode="lines+markers",
        line=dict(color=CLR["purple"], width=2.5),
        marker=dict(size=4, color=CLR["purple"])))
    fig_med.add_trace(go.Scatter(
        x=med_layoff["label"], y=med_layoff["median"], name="Layoffs",
        mode="lines", line=dict(color=CLR["blue"], width=1.5, dash="dash")))
    fig_med.add_trace(go.Scatter(
        x=med_closure["label"], y=med_closure["median"], name="Closures",
        mode="lines", line=dict(color=CLR["red"], width=1.5, dash="dash")))
    fig_med.update_layout(
        title="Median Workers Per Notice",
        height=300, yaxis_title="Workers", **CHART_LAYOUT)
    fig_med.update_xaxes(showgrid=False)
    fig_med.update_yaxes(gridcolor="#f3f4f6")
    st.plotly_chart(fig_med, width='stretch')

    # Row 4 — pie + state ranking
    r4a, r4b = st.columns([1, 2])

    with r4a:
        tc = df["type"].value_counts().reset_index()
        tc.columns = ["type", "count"]
        fig_pie = px.pie(tc, values="count", names="type", title="Notice Types",
                         color="type",
                         color_discrete_map={"Layoff": CLR["blue"],
                                             "Closure": CLR["red"]})
        fig_pie.update_layout(height=280, **{**CHART_LAYOUT,
                               "margin": dict(t=44, b=10, l=10, r=10)})
        fig_pie.update_traces(textfont_size=11)
        st.plotly_chart(fig_pie, width='stretch')

    with r4b:
        st.markdown("**States by Notices**")
        st.caption("All 50 states + DC ranked by WARN filings")
        sdf = (df.groupby("state")
                 .agg(notices=("company", "count"), workers=("workers", "sum"))
                 .reset_index()
                 .sort_values("notices", ascending=False))
        sdf["name"] = sdf["state"].map(STATES_META).fillna(sdf["state"])
        max_n = int(sdf["notices"].max()) if len(sdf) else 1
        st.dataframe(
            sdf[["state", "name", "notices", "workers"]],
            column_config={
                "state":   st.column_config.TextColumn("Code", width=60),
                "name":    st.column_config.TextColumn("State"),
                "notices": st.column_config.ProgressColumn(
                    "Notices", max_value=max_n, format="%d"),
                "workers": st.column_config.NumberColumn("Workers", format="%d"),
            },
            height=280, width='stretch', hide_index=True,
        )


# ═══════════════════════════════════════════════════════════════════════════
# ALL NOTICES
# ═══════════════════════════════════════════════════════════════════════════

with t_notices:
    st.markdown(f"**{len(df):,} notices found** — real data from state WARN filings")
    show_cols = [c for c in ("date", "company", "city", "state", "workers", "type")
                 if c in df.columns]
    st.dataframe(
        df[show_cols].reset_index(drop=True),
        column_config={
            "date":    st.column_config.DateColumn("Date", format="YYYY-MM-DD"),
            "company": st.column_config.TextColumn("Company"),
            "city":    st.column_config.TextColumn("City"),
            "state":   st.column_config.TextColumn("State", width=70),
            "workers": st.column_config.NumberColumn("Workers", format="%d"),
            "type":    st.column_config.TextColumn("Type", width=90),
        },
        height=600, width='stretch', hide_index=True,
    )


# ═══════════════════════════════════════════════════════════════════════════
# STATES
# ═══════════════════════════════════════════════════════════════════════════

with t_states:
    sdf = (df.groupby("state")
             .agg(notices=("company", "count"), workers=("workers", "sum"))
             .reset_index()
             .sort_values("notices", ascending=False))
    sdf["name"]           = sdf["state"].map(STATES_META).fillna(sdf["state"])
    sdf["avg_per_notice"] = (sdf["workers"] / sdf["notices"]).round(0).astype(int)

    st.plotly_chart(
        bar_chart(sdf.head(20).sort_values("notices"), "name", "notices",
                  "Top 20 States by WARN Filings", CLR["red"],
                  height=450, horizontal=True),
        width='stretch')

    max_n = int(sdf["notices"].max()) if len(sdf) else 1
    st.dataframe(
        sdf[["state", "name", "notices", "workers", "avg_per_notice"]],
        column_config={
            "state":          st.column_config.TextColumn("Code", width=60),
            "name":           st.column_config.TextColumn("State"),
            "notices":        st.column_config.ProgressColumn(
                "Notices", max_value=max_n, format="%d"),
            "workers":        st.column_config.NumberColumn("Workers", format="%d"),
            "avg_per_notice": st.column_config.NumberColumn("Avg / Notice", format="%d"),
        },
        height=500, width='stretch', hide_index=True,
    )


# ═══════════════════════════════════════════════════════════════════════════
# COMPANIES
# ═══════════════════════════════════════════════════════════════════════════

with t_companies:
    cdf = (df.groupby("company")
             .agg(notices=("company", "count"),
                  workers=("workers", "sum"),
                  states=("state", "nunique"))
             .reset_index()
             .sort_values("workers", ascending=False)
             .head(20))

    st.plotly_chart(
        bar_chart(cdf.sort_values("workers"), "company", "workers",
                  "Top 20 Companies by Workers Affected", CLR["orange"],
                  height=500, horizontal=True),
        width='stretch')

    st.dataframe(
        cdf[["company", "notices", "workers", "states"]],
        column_config={
            "company": st.column_config.TextColumn("Company"),
            "notices": st.column_config.NumberColumn("Notices", format="%d"),
            "workers": st.column_config.NumberColumn("Workers", format="%d"),
            "states":  st.column_config.NumberColumn("States", format="%d"),
        },
        height=400, width='stretch', hide_index=True,
    )


# ═══════════════════════════════════════════════════════════════════════════
# DATABASE
# ═══════════════════════════════════════════════════════════════════════════

with t_db:
    st.markdown("### 🗄️ Database & Storage")
    st.caption("State WARN filings · Updated daily via GitHub Actions")

    d1, d2, d3, d4 = st.columns(4)
    d1.metric("Total Records",  f"{len(raw):,}")
    d2.metric("Date Range",
              f"{raw['date'].min().strftime('%Y-%m-%d')} – "
              f"{raw['date'].max().strftime('%Y-%m-%d')}")
    d3.metric("States Covered", f"{raw['state'].nunique():,}")
    d4.metric("Source", "State WARN filings")

    st.markdown("---")

    dl1, dl2, dl3 = st.columns(3)
    with dl1:
        st.download_button("⬇️  Export All (CSV)",
                           data=raw.to_csv(index=False),
                           file_name="warn_notices_all.csv",
                           mime="text/csv",
                           width='stretch')
    with dl2:
        st.download_button("🔽  Export Filtered (CSV)",
                           data=df.to_csv(index=False),
                           file_name="warn_notices_filtered.csv",
                           mime="text/csv",
                           width='stretch')
    with dl3:
        import json
        st.download_button("📄  Export All (JSON)",
                           data=json.dumps({
                               "total_records": len(raw),
                               "source": "State WARN filings / amp2026/Warn-Tracker",
                               "records": raw.assign(
                                   date=raw["date"].dt.strftime("%Y-%m-%d")
                               ).to_dict(orient="records"),
                           }, indent=2, default=str),
                           file_name="warn_notices_all.json",
                           mime="application/json",
                           width='stretch')

    st.markdown("**Data Schema**")
    st.code("""{
  "date":    "YYYY-MM-DD  // WARN notice effective date",
  "company": "String      // company from state WARN filing",
  "city":    "String      // city of affected facility",
  "state":   "String      // two-letter state code",
  "workers": "Number      // employees affected",
  "type":    "String      // Layoff | Closure"
}""", language="json")

    st.markdown("**State Coverage**")
    covered = set(raw["state"].dropna().unique())
    badges  = " ".join(
        f'<span style="display:inline-block;padding:2px 7px;margin:2px;'
        f'border-radius:4px;font-size:11px;font-family:monospace;'
        f'background:{"#dcfce7;color:#166534" if s in covered else "#f3f4f6;color:#9ca3af"}'
        f'">{s}</span>'
        for s in sorted(STATES_META)
    )
    st.markdown(badges, unsafe_allow_html=True)
