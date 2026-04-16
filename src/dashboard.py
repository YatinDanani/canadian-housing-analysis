"""
src/dashboard.py
~~~~~~~~~~~~~~~~
Streamlit dashboard for Canadian housing affordability analysis.

Run:
    streamlit run src/dashboard.py
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

PROCESSED_DIR = Path(__file__).parent.parent / "data" / "processed"

st.set_page_config(
    page_title="Canadian Housing Affordability",
    page_icon="🏠",
    layout="wide",
)


@st.cache_data
def load_features() -> pd.DataFrame:
    """Load the feature-engineered parquet file.

    Returns:
        Wide-format features DataFrame.
    """
    return pd.read_parquet(PROCESSED_DIR / "cmhc_features.parquet")


@st.cache_data
def load_forecasts() -> pd.DataFrame | None:
    """Load the forecasts parquet if it exists.

    Returns:
        Forecasts DataFrame, or None if model.py has not been run.
    """
    path = PROCESSED_DIR / "forecasts.parquet"
    return pd.read_parquet(path) if path.exists() else None


def cma_total_slice(df: pd.DataFrame, bedroom: str) -> pd.DataFrame:
    """Filter to CMA-total rows for a given bedroom type.

    Args:
        df: Features DataFrame.
        bedroom: Bedroom type string (e.g. ``'Total'``).

    Returns:
        Filtered DataFrame sorted by city name.
    """
    return (
        df[df["is_cma_total"] & (df["bedroom_type"] == bedroom)]
        .sort_values("city")
        .reset_index(drop=True)
    )


def chart_rent_comparison(cma: pd.DataFrame) -> go.Figure:
    """Grouped bar chart: Oct-24 vs Oct-25 average rent by city.

    Args:
        cma: CMA-total slice for one bedroom type.

    Returns:
        Plotly Figure.
    """
    fig = go.Figure()
    fig.add_bar(x=cma["city"], y=cma["avg_rent_oct24"],
                name="Oct 2024", marker_color="#6baed6")
    fig.add_bar(x=cma["city"], y=cma["avg_rent_oct25"],
                name="Oct 2025", marker_color="#2171b5")
    fig.update_layout(
        title="Average Rent by City",
        barmode="group",
        xaxis_tickangle=-40,
        yaxis_title="Avg Rent ($)",
        legend=dict(orientation="h", y=1.05),
        height=420,
    )
    return fig


def chart_rent_growth(cma: pd.DataFrame) -> go.Figure:
    """Horizontal bar chart: YoY rent growth % ranked by city.

    Args:
        cma: CMA-total slice for one bedroom type.

    Returns:
        Plotly Figure.
    """
    s = cma.sort_values("avg_rent_yoy_pct")
    colors = ["#ef3b2c" if v >= 0 else "#74c476" for v in s["avg_rent_yoy_pct"]]
    fig = go.Figure(go.Bar(
        x=s["avg_rent_yoy_pct"],
        y=s["city"],
        orientation="h",
        marker_color=colors,
        text=s["avg_rent_yoy_pct"].round(1).astype(str) + "%",
        textposition="outside",
    ))
    fig.update_layout(
        title="YoY Rent Growth (Oct-24 → Oct-25)",
        xaxis_title="% Change",
        height=460,
    )
    return fig


def chart_vacancy_vs_rent_growth(cma: pd.DataFrame) -> go.Figure:
    """Scatter: Oct-25 vacancy rate vs YoY rent growth, sized by universe.

    Args:
        cma: CMA-total slice for one bedroom type.

    Returns:
        Plotly Figure.
    """
    fig = px.scatter(
        cma.dropna(subset=["vacancy_rate_oct25", "avg_rent_yoy_pct"]),
        x="vacancy_rate_oct25",
        y="avg_rent_yoy_pct",
        size="rental_universe_oct25",
        color="market_tightness",
        text="city",
        color_continuous_scale="RdYlGn_r",
        labels={
            "vacancy_rate_oct25": "Vacancy Rate Oct-25 (%)",
            "avg_rent_yoy_pct": "Rent Growth YoY (%)",
            "market_tightness": "Tightness",
        },
        title="Vacancy vs Rent Growth (bubble = rental stock size)",
        height=460,
    )
    fig.update_traces(textposition="top center", textfont_size=10)
    return fig


def chart_market_tightness(cma: pd.DataFrame) -> go.Figure:
    """Horizontal bar chart of composite market-tightness score.

    Args:
        cma: CMA-total slice for one bedroom type.

    Returns:
        Plotly Figure.
    """
    s = cma.dropna(subset=["market_tightness"]).sort_values("market_tightness", ascending=False)
    fig = go.Figure(go.Bar(
        x=s["market_tightness"].round(3),
        y=s["city"],
        orientation="h",
        marker_color="#2171b5",
    ))
    fig.update_layout(
        title="Market Tightness Score (1 = tightest)",
        xaxis=dict(range=[0, 1], title="Score"),
        height=460,
    )
    return fig


def chart_forecasts(forecasts: pd.DataFrame, bedroom: str) -> go.Figure:
    """Bar chart with CI error bars for Oct-26 rent forecasts.

    Args:
        forecasts: Forecasts DataFrame from ``model.py``.
        bedroom: Bedroom type to display.

    Returns:
        Plotly Figure.
    """
    f = forecasts[forecasts["bedroom_type"] == bedroom].sort_values("predicted_rent")
    fig = go.Figure(go.Bar(
        x=f["city"],
        y=f["predicted_rent"].round(0),
        error_y=dict(
            type="data",
            symmetric=False,
            array=(f["upper_ci"] - f["predicted_rent"]).round(0),
            arrayminus=(f["predicted_rent"] - f["lower_ci"]).round(0),
        ),
        marker_color="#756bb1",
        name="Predicted Oct-26",
    ))
    fig.update_layout(
        title=f"Predicted Oct-26 Rent — {bedroom} ({int(CI_LEVEL * 100)}% CI)",
        xaxis_tickangle=-40,
        yaxis_title="Predicted Rent ($)",
        height=420,
    )
    return fig


CI_LEVEL = 0.90


def main() -> None:
    """Render the Streamlit dashboard."""
    st.title("Canadian Rental Housing Dashboard")
    st.caption("Data: CMHC Rental Market Reports — Oct 2024 & Oct 2025 | 18 CMAs")

    df = load_features()
    forecasts = load_forecasts()

    # --- Sidebar controls ---
    st.sidebar.header("Filters")
    bedroom = st.sidebar.selectbox(
        "Bedroom Type",
        ["Total", "Studio", "1 Bedroom", "2 Bedroom", "3 Bedroom +"],
        index=0,
    )
    cities = sorted(df["city"].unique())
    selected = st.sidebar.multiselect("Cities", cities, default=cities)

    cma = cma_total_slice(df, bedroom)
    cma = cma[cma["city"].isin(selected)]

    if cma.empty:
        st.warning("No data for the selected filters.")
        return

    # --- Summary metrics ---
    top = cma.sort_values("avg_rent_yoy_pct", ascending=False).iloc[0]
    bot = cma.sort_values("avg_rent_yoy_pct").iloc[0]
    tightest = cma.dropna(subset=["market_tightness"]).sort_values(
        "market_tightness", ascending=False).iloc[0]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Cities shown", len(cma))
    c2.metric("Highest rent growth", top["city"],
              f"+{top['avg_rent_yoy_pct']:.1f}%")
    c3.metric("Lowest rent growth", bot["city"],
              f"{bot['avg_rent_yoy_pct']:.1f}%")
    c4.metric("Tightest market", tightest["city"],
              f"score {tightest['market_tightness']:.2f}")

    st.divider()

    # --- Charts row 1 ---
    col1, col2 = st.columns(2)
    with col1:
        st.plotly_chart(chart_rent_comparison(cma), use_container_width=True)
    with col2:
        st.plotly_chart(chart_rent_growth(cma), use_container_width=True)

    # --- Charts row 2 ---
    col3, col4 = st.columns(2)
    with col3:
        st.plotly_chart(chart_vacancy_vs_rent_growth(cma), use_container_width=True)
    with col4:
        st.plotly_chart(chart_market_tightness(cma), use_container_width=True)

    # --- Forecasts ---
    if forecasts is not None:
        st.divider()
        st.subheader("Oct-26 Rent Forecasts (XGBoost)")
        filt = forecasts[forecasts["city"].isin(selected)]
        st.plotly_chart(chart_forecasts(filt, bedroom), use_container_width=True)
    else:
        st.info("Run `python src/model.py` to generate forecasts.")

    # --- Data table ---
    with st.expander("Raw CMA data"):
        st.dataframe(cma.drop(columns=["is_cma_total"]), use_container_width=True)


if __name__ == "__main__":
    main()
