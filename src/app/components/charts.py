"""Reusable chart components for Streamlit app."""

from typing import Optional

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots


def create_magnitude_distribution_chart(df: pd.DataFrame) -> go.Figure:
    """Create magnitude distribution bar chart.

    Args:
        df: DataFrame with magnitude_category and total_events columns

    Returns:
        Plotly figure
    """
    fig = px.bar(
        df,
        x="magnitude_category",
        y="total_events",
        title="Earthquake Distribution by Magnitude Category",
        labels={
            "magnitude_category": "Magnitude Category",
            "total_events": "Number of Events",
        },
        color="magnitude_category",
        color_discrete_sequence=px.colors.sequential.Reds,
    )

    fig.update_layout(
        showlegend=False,
        height=400,
        xaxis_title="Magnitude Category",
        yaxis_title="Number of Events",
    )

    return fig


def create_temporal_trend_chart(df: pd.DataFrame) -> go.Figure:
    """Create temporal trend line chart.

    Args:
        df: DataFrame with date and daily_event_count columns

    Returns:
        Plotly figure
    """
    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=df["date"],
            y=df["daily_event_count"],
            mode="lines",
            name="Daily Events",
            line=dict(color="#1f77b4", width=2),
            fill="tozeroy",
            fillcolor="rgba(31, 119, 180, 0.2)",
        )
    )

    fig.update_layout(
        title="Daily Earthquake Activity Over Time",
        xaxis_title="Date",
        yaxis_title="Number of Events",
        height=400,
        hovermode="x unified",
    )

    return fig


def create_depth_analysis_chart(df: pd.DataFrame) -> go.Figure:
    """Create depth analysis chart.

    Args:
        df: DataFrame with depth_category and statistics

    Returns:
        Plotly figure
    """
    fig = make_subplots(
        rows=1,
        cols=2,
        subplot_titles=("Events by Depth", "Average Magnitude by Depth"),
        specs=[[{"type": "bar"}, {"type": "bar"}]],
    )

    # Events count
    fig.add_trace(
        go.Bar(
            x=df["depth_category"],
            y=df["total_events"],
            name="Events",
            marker_color="#2ca02c",
        ),
        row=1,
        col=1,
    )

    # Average magnitude
    fig.add_trace(
        go.Bar(
            x=df["depth_category"],
            y=df["avg_magnitude"],
            name="Avg Magnitude",
            marker_color="#ff7f0e",
        ),
        row=1,
        col=2,
    )

    fig.update_layout(height=400, showlegend=False)
    fig.update_xaxes(title_text="Depth Category", row=1, col=1)
    fig.update_xaxes(title_text="Depth Category", row=1, col=2)
    fig.update_yaxes(title_text="Number of Events", row=1, col=1)
    fig.update_yaxes(title_text="Average Magnitude", row=1, col=2)

    return fig


def create_hourly_pattern_chart(df: pd.DataFrame) -> go.Figure:
    """Create hourly pattern polar chart.

    Args:
        df: DataFrame with hour and total_events columns

    Returns:
        Plotly figure
    """
    fig = go.Figure()

    fig.add_trace(
        go.Barpolar(
            r=df["total_events"],
            theta=df["hour"] * 15,  # Convert hour to degrees (24h = 360°)
            marker_color=df["total_events"],
            marker_colorscale="Viridis",
            name="Events",
        )
    )

    fig.update_layout(
        title="Earthquake Activity by Hour of Day",
        polar=dict(
            radialaxis=dict(showticklabels=True, ticks=""),
            angularaxis=dict(
                tickmode="array",
                tickvals=[0, 45, 90, 135, 180, 225, 270, 315],
                ticktext=["00:00", "03:00", "06:00", "09:00", "12:00", "15:00", "18:00", "21:00"],
            ),
        ),
        height=500,
    )

    return fig


def create_magnitude_vs_depth_scatter(df: pd.DataFrame) -> go.Figure:
    """Create magnitude vs depth scatter plot.

    Args:
        df: DataFrame with magnitude, depth, and magnitude_category

    Returns:
        Plotly figure
    """
    fig = px.scatter(
        df,
        x="depth",
        y="magnitude",
        color="magnitude_category",
        title="Magnitude vs Depth Relationship",
        labels={"depth": "Depth (km)", "magnitude": "Magnitude"},
        color_discrete_sequence=px.colors.qualitative.Set2,
        opacity=0.6,
    )

    fig.update_layout(height=500, hovermode="closest")

    return fig


def create_regional_comparison_chart(df: pd.DataFrame, top_n: int = 10) -> go.Figure:
    """Create regional comparison horizontal bar chart.

    Args:
        df: DataFrame with region and event_count
        top_n: Number of top regions to show

    Returns:
        Plotly figure
    """
    df_top = df.nlargest(top_n, "event_count")

    fig = go.Figure()

    fig.add_trace(
        go.Bar(
            y=df_top["region"],
            x=df_top["event_count"],
            orientation="h",
            marker=dict(
                color=df_top["event_count"],
                colorscale="Blues",
                showscale=True,
            ),
        )
    )

    fig.update_layout(
        title=f"Top {top_n} Most Active Regions",
        xaxis_title="Number of Events",
        yaxis_title="Region",
        height=500,
        yaxis=dict(autorange="reversed"),
    )

    return fig


def create_energy_release_chart(df: pd.DataFrame) -> go.Figure:
    """Create energy release over time chart.

    Args:
        df: DataFrame with date and daily_total_energy

    Returns:
        Plotly figure
    """
    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=df["date"],
            y=df["daily_total_energy"],
            mode="lines",
            name="Energy Release",
            line=dict(color="#d62728", width=2),
            fill="tozeroy",
            fillcolor="rgba(214, 39, 40, 0.2)",
        )
    )

    fig.update_layout(
        title="Seismic Energy Release Over Time",
        xaxis_title="Date",
        yaxis_title="Energy (Joules)",
        yaxis_type="log",
        height=400,
        hovermode="x unified",
    )

    return fig


def create_moon_phase_polar_chart(df: pd.DataFrame) -> go.Figure:
    """Create polar chart showing earthquake distribution by moon phase.

    Args:
        df: DataFrame with moon_phase, magnitude_group, and event_count

    Returns:
        Plotly figure
    """
    import plotly.graph_objects as go
    
    # Moon phase order for proper circular display
    phase_order = [
        "New Moon",
        "Waxing Crescent",
        "First Quarter",
        "Waxing Gibbous",
        "Full Moon",
        "Waning Gibbous",
        "Last Quarter",
        "Waning Crescent",
    ]
    
    # Color mapping for magnitude groups
    color_map = {
        "1-3": "#2ecc71",   # Green
        "4": "#f1c40f",     # Yellow
        "5": "#e67e22",     # Orange
        "6-7": "#e74c3c",   # Red
        "8-9": "#9b59b6",   # Purple
    }
    
    # Get only magnitude groups that exist in the data
    mag_groups_in_data = df["magnitude_group"].unique().tolist()
    
    # Sort magnitude groups in proper order
    mag_group_order = ["1-3", "4", "5", "6-7", "8-9"]
    mag_groups = [mg for mg in mag_group_order if mg in mag_groups_in_data]
    
    fig = go.Figure()
    
    # Add trace for each magnitude group that exists in the data
    for mag_group in mag_groups:
        df_group = df[df["magnitude_group"] == mag_group].copy()
        
        # Prepare data for polar plot
        theta = []
        r = []
        hover_text = []
        
        for phase in phase_order:
            phase_data = df_group[df_group["moon_phase_name"] == phase]
            if not phase_data.empty:
                count = int(phase_data["event_count"].sum())  # Use sum in case of duplicates
                avg_mag = float(phase_data["avg_magnitude"].mean())
            else:
                count = 0
                avg_mag = 0
            
            theta.append(phase)
            r.append(count)
            hover_text.append(
                f"<b>{phase}</b><br>"
                f"Magnitude: {mag_group}<br>"
                f"Events: {count:,}<br>"
                f"Avg Mag: {avg_mag:.2f}"
            )
        
        fig.add_trace(
            go.Barpolar(
                r=r,
                theta=theta,
                name=f"Magnitude {mag_group}",
                marker_color=color_map[mag_group],
                marker_line_color="white",
                marker_line_width=1,
                opacity=0.8,
                hovertext=hover_text,
                hoverinfo="text",
            )
        )
    
    fig.update_layout(
        title="Earthquake Distribution by Moon Phase and Magnitude",
        polar=dict(
            radialaxis=dict(
                showticklabels=True,
                ticks="",
                title="Number of Events",
            ),
            angularaxis=dict(
                direction="clockwise",
                rotation=90,  # Start at top (New Moon)
            ),
        ),
        height=600,
        showlegend=True,
        legend=dict(
            orientation="v",
            yanchor="middle",
            y=0.5,
            xanchor="left",
            x=1.05,
        ),
        barmode='stack',  # Stack bars instead of overlaying
    )
    
    return fig


def create_day_of_week_chart(df: pd.DataFrame) -> go.Figure:
    """Create bar chart showing earthquake distribution by day of week.

    Args:
        df: DataFrame with temporal data including day_of_week

    Returns:
        Plotly figure
    """
    from plotly.subplots import make_subplots
    
    # Day names - DuckDB uses 1=Monday, 2=Tuesday, ..., 7=Sunday
    day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    
    # Aggregate data by day of week
    day_stats = df.groupby("day_of_week").agg({
        "daily_event_count": "sum",
        "daily_avg_magnitude": "mean"
    }).reset_index()
    
    # Build data for all 7 days (1-7)
    day_data = []
    for day_num in range(1, 8):  # 1 to 7
        day_row = day_stats[day_stats["day_of_week"] == day_num]
        if not day_row.empty:
            day_data.append({
                "day_name": day_names[day_num - 1],  # Convert 1-7 to 0-6 for indexing
                "event_count": int(day_row["daily_event_count"].iloc[0]),
                "avg_magnitude": float(day_row["daily_avg_magnitude"].iloc[0])
            })
        else:
            day_data.append({
                "day_name": day_names[day_num - 1],
                "event_count": 0,
                "avg_magnitude": 0.0
            })
    
    day_df = pd.DataFrame(day_data)
    
    # Create figure with secondary y-axis
    fig = make_subplots(
        rows=1, cols=1,
        specs=[[{"secondary_y": True}]]
    )
    
    # Add bar chart for event count
    fig.add_trace(
        go.Bar(
            x=day_df["day_name"],
            y=day_df["event_count"],
            name="Event Count",
            marker_color="#3498db",
            text=day_df["event_count"],
            textposition="outside",
            texttemplate="%{text:,}",
            hovertemplate="<b>%{x}</b><br>Events: %{y:,}<extra></extra>",
        ),
        secondary_y=False,
    )
    
    # Add line chart for average magnitude
    fig.add_trace(
        go.Scatter(
            x=day_df["day_name"],
            y=day_df["avg_magnitude"],
            name="Avg Magnitude",
            mode="lines+markers",
            line=dict(color="#e74c3c", width=3),
            marker=dict(size=10, color="#e74c3c"),
            text=day_df["avg_magnitude"].round(2),
            hovertemplate="<b>%{x}</b><br>Avg Magnitude: %{y:.2f}<extra></extra>",
        ),
        secondary_y=True,
    )
    
    # Update layout
    fig.update_layout(
        title="Earthquake Distribution by Day of Week",
        xaxis_title="Day of Week",
        height=450,
        hovermode="x unified",
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
    )
    
    # Update y-axes
    fig.update_yaxes(title_text="Number of Events", secondary_y=False)
    fig.update_yaxes(title_text="Average Magnitude", secondary_y=True)
    
    return fig

def create_seasonal_polar_chart(df: pd.DataFrame) -> go.Figure:
    """Create polar chart showing earthquake distribution by season.

    Args:
        df: DataFrame with season, total_events, and avg_magnitude

    Returns:
        Plotly figure
    """
    import plotly.graph_objects as go
    
    # Season order (starting from Spring at top, clockwise)
    season_order = ["Spring", "Summer", "Fall", "Winter"]
    
    # Color mapping for seasons
    season_colors = {
        "Spring": "#2ecc71",   # Green
        "Summer": "#f39c12",   # Orange/Yellow
        "Fall": "#e67e22",     # Orange
        "Winter": "#3498db",   # Blue
    }
    
    # Prepare data in correct order
    ordered_data = []
    for season in season_order:
        season_row = df[df["season"] == season]
        if not season_row.empty:
            ordered_data.append({
                "season": season,
                "total_events": int(season_row["total_events"].iloc[0]),
                "avg_magnitude": float(season_row["avg_magnitude"].iloc[0])
            })
        else:
            ordered_data.append({
                "season": season,
                "total_events": 0,
                "avg_magnitude": 0.0
            })
    
    ordered_df = pd.DataFrame(ordered_data)
    
    # Create polar bar chart
    fig = go.Figure()
    
    # Add bars for each season
    colors = [season_colors[season] for season in ordered_df["season"]]
    
    fig.add_trace(
        go.Barpolar(
            r=ordered_df["total_events"],
            theta=ordered_df["season"],
            marker=dict(
                color=colors,
                line=dict(color="white", width=2)
            ),
            opacity=0.8,
            text=ordered_df["total_events"],
            hovertemplate="<b>%{theta}</b><br>" +
                         "Events: %{r:,}<br>" +
                         "Avg Magnitude: %{customdata:.2f}<extra></extra>",
            customdata=ordered_df["avg_magnitude"],
        )
    )
    
    fig.update_layout(
        title="Earthquake Distribution by Season",
        polar=dict(
            radialaxis=dict(
                showticklabels=True,
                ticks="",
                title="Number of Events",
            ),
            angularaxis=dict(
                direction="clockwise",
                rotation=90,  # Start at top (Spring)
            ),
        ),
        height=500,
        showlegend=False,
    )
    
    return fig
