"""Reusable filter components for Streamlit app."""

from datetime import date, datetime
from typing import Optional, Tuple, Union

import streamlit as st


def magnitude_filter(
    min_val: float = 0.0, max_val: float = 10.0, default_min: float = 2.5
) -> Tuple[float, float]:
    """Create magnitude range filter.

    Args:
        min_val: Minimum possible magnitude
        max_val: Maximum possible magnitude
        default_min: Default minimum value

    Returns:
        Tuple of (min_magnitude, max_magnitude)
    """
    st.subheader("🎚️ Magnitude Filter")

    mag_range = st.slider(
        "Select magnitude range",
        min_value=min_val,
        max_value=max_val,
        value=(default_min, max_val),
        step=0.1,
    )

    return mag_range


def date_filter(
    min_date: Optional[date] = None, max_date: Optional[date] = None
) -> Tuple[date, date]:
    """Create date range filter.

    Args:
        min_date: Minimum possible date
        max_date: Maximum possible date

    Returns:
        Tuple of (start_date, end_date)
    """
    st.subheader("📅 Date Filter")

    col1, col2 = st.columns(2)

    with col1:
        start_date_input = st.date_input(
            "Start date", value=min_date or date.today(), min_value=min_date, max_value=max_date
        )

    with col2:
        end_date_input = st.date_input(
            "End date", value=max_date or date.today(), min_value=min_date, max_value=max_date
        )

    # Handle the various return types from date_input and ensure we always have a date
    start_date: date
    end_date: date
    
    if isinstance(start_date_input, tuple):
        start_date = start_date_input[0] if len(start_date_input) > 0 else (min_date or date.today())
    else:
        start_date = start_date_input if start_date_input is not None else (min_date or date.today())

    if isinstance(end_date_input, tuple):
        end_date = end_date_input[0] if len(end_date_input) > 0 else (max_date or date.today())
    else:
        end_date = end_date_input if end_date_input is not None else (max_date or date.today())

    return start_date, end_date


def depth_category_filter() -> list:
    """Create depth category filter.

    Returns:
        List of selected depth categories
    """
    st.subheader("⬇️ Depth Category")

    categories = st.multiselect(
        "Select depth categories",
        options=["Shallow", "Intermediate", "Deep"],
        default=["Shallow", "Intermediate", "Deep"],
    )

    return categories


def region_filter(regions: list) -> list:
    """Create region filter.

    Args:
        regions: List of available regions

    Returns:
        List of selected regions
    """
    st.subheader("🌎 Region Filter")

    selected_regions = st.multiselect(
        "Select regions", options=regions, default=regions[:10] if len(regions) > 10 else regions
    )

    return selected_regions


def magnitude_category_filter() -> list:
    """Create magnitude category filter.

    Returns:
        List of selected magnitude categories
    """
    st.subheader("📊 Magnitude Category")

    categories = st.multiselect(
        "Select magnitude categories",
        options=["Minor", "Light", "Moderate", "Strong", "Major", "Great"],
        default=["Minor", "Light", "Moderate", "Strong", "Major", "Great"],
    )

    return categories


def create_filter_summary(filters: dict) -> None:
    """Display summary of active filters.

    Args:
        filters: Dictionary of active filters
    """
    st.sidebar.markdown("### 🔍 Active Filters")

    for key, value in filters.items():
        if value:
            st.sidebar.markdown(f"**{key}:** {value}")
