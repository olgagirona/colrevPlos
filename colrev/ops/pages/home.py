#! /usr/bin/env python3
"""Burn Down Chart is created here"""
from __future__ import annotations

from datetime import datetime

import dash
import pandas as pd
import plotly.express as px
from dash import dcc
from dash import html

import colrev.review_manager

dash.register_page(__name__, path="/")


def analytics():
    """function creating Burn Down Chart"""
    review_manager = colrev.review_manager.ReviewManager()
    status_operation = review_manager.get_status_operation()
    analytic_results = status_operation.get_analytics()

    analytics_df = pd.DataFrame(analytic_results)
    analytics_df = analytics_df.transpose()

    analytics_df["committed_date"] = analytics_df["committed_date"].apply(
        timestamp_to_date
    )

    max_y_lab = max(analytics_df["atomic_steps"])

    if max_y_lab == 0:
        raise Exception("Die Datei 'records.bib' ist leer.")

    analytics_df["scaled_progress"] = analytics_df["completed_atomic_steps"].apply(
        scale_completed_atomic_steps, max=max_y_lab
    )

    analytics_df2 = analytics_df.iloc[::-1]

    fig = px.line(
        analytics_df2,
        x="committed_date",
        y="scaled_progress",
        template="simple_white",
        title="Burn-Out Chart",
    )
    fig.update_traces(marker_color="#2596be")
    fig.update_layout(
        title=dict(
            text="<b>Burn-Down Chart</b>", font=dict(size=30), automargin=True, x=0.5
        )
    )
    fig.update_xaxes(
        title_text="Date of Commit",
        type="category",
        title_font={"size": 20},
        tickangle=25,
        tickfont=dict(size=20),
    )
    fig.update_yaxes(
        title_text="Atomic Steps Completed in %",
        title_font={"size": 20},
        tickfont=dict(size=20),
    )

    return fig


def timestamp_to_date(timestamp) -> datetime:
    """convert the timestamp to a datetime object in the local timezone"""
    date = datetime.fromtimestamp(timestamp)
    return date


def scale_completed_atomic_steps(steps, max) -> int:
    """scales completed atomic steps for burn down chart"""
    steps = 100 - (steps / max) * 100
    return steps


layout = html.Div([dcc.Graph(figure=analytics())], style={"margin": "auto"}), html.Div(
    className="navigation-button",
    children=[
        html.A(
            html.Button("detailed information on synthesized records"),
            href="http://127.0.0.1:8050/synthesizedrecords",
        )
    ],
)
