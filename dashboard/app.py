from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

GOLD_PATH = Path("data/processed/gold/coveo")


@st.cache_data
def load_gold_tables():
    funnel = pd.read_parquet(GOLD_PATH / "funnel")
    categories = pd.read_parquet(GOLD_PATH / "categories")
    price_buckets = pd.read_parquet(GOLD_PATH / "price_buckets")

    return funnel, categories, price_buckets


st.set_page_config(
    page_title="Luxury Retail Streaming Analytics",
    page_icon="◆",
    layout="wide",
)

st.title("Luxury Retail Streaming Analytics")
st.caption(
    "Real anonymized e-commerce events processed with Kafka "
    "and Spark Structured Streaming."
)

if not GOLD_PATH.exists():
    st.error("Gold data was not found. Run the Coveo Gold builder first.")
    st.stop()

funnel, categories, price_buckets = load_gold_tables()
metrics = funnel.iloc[0]

metric_columns = st.columns(4)

metric_columns[0].metric(
    "Events",
    f"{int(metrics['total_events']):,}",
)
metric_columns[1].metric(
    "Sessions",
    f"{int(metrics['total_sessions']):,}",
)
metric_columns[2].metric(
    "Products",
    f"{int(metrics['total_products']):,}",
)
metric_columns[3].metric(
    "Purchases",
    f"{int(metrics['purchases']):,}",
)

st.divider()

left_column, right_column = st.columns(2)

with left_column:
    st.subheader("Customer journey funnel")

    funnel_chart = pd.DataFrame(
        {
            "Stage": [
                "Sessions",
                "Added to cart",
                "Purchased",
            ],
            "Count": [
                metrics["total_sessions"],
                metrics["cart_sessions"],
                metrics["purchase_sessions"],
            ],
        }
    )

    figure = px.funnel(
        funnel_chart,
        x="Count",
        y="Stage",
        color="Stage",
        color_discrete_sequence=[
            "#292929",
            "#8A7355",
            "#C7A76B",
        ],
    )
    figure.update_layout(
        showlegend=False,
        height=380,
        margin=dict(l=20, r=20, t=20, b=20),
        )
    st.plotly_chart(figure, width="stretch")

with right_column:
    st.subheader("Conversion rates")

    st.metric(
        "View to cart",
        f"{metrics['view_to_cart_rate']:.2f}%",
    )
    st.metric(
        "Session purchase conversion",
        f"{metrics['purchase_conversion_rate']:.2f}%",
    )
    st.metric(
        "Cart to purchase",
        f"{metrics['cart_to_purchase_rate']:.2f}%",
    )

st.divider()
st.subheader("Purchases by anonymized price segment")

price_chart_data = price_buckets.copy()
price_chart_data["price_segment"] = price_chart_data["price_segment"].astype(str)

price_figure = px.bar(
    price_chart_data,
    x="price_segment",
    y="purchases",
    labels={
        "price_segment": "Price bucket",
        "purchases": "Purchases",
    },
    color="purchases",
    color_continuous_scale=[
        "#E8E0D2",
        "#8A7355",
        "#292929",
    ],
)

price_figure.update_layout(coloraxis_showscale=False)
st.plotly_chart(price_figure, width="stretch")

st.divider()
st.subheader("Top anonymized categories")

top_categories = categories.head(10).copy()
top_categories["category_display"] = top_categories["category"].apply(
    lambda value: "Unknown" if value == "unknown" else f"{str(value)[:12]}…"
)

category_figure = px.bar(
    top_categories.sort_values("purchases"),
    x="purchases",
    y="category_display",
    orientation="h",
    labels={
        "purchases": "Purchases",
        "category_display": "Anonymized category",
    },
    color="purchase_per_view_rate",
    color_continuous_scale=[
        "#E8E0D2",
        "#C7A76B",
        "#292929",
    ],
)

category_figure.update_layout(coloraxis_colorbar_title="Purchase/view %")
st.plotly_chart(category_figure, width="stretch")

with st.expander("Data-quality notes"):
    st.write(
        """
        - Seven exact duplicate events were removed in Silver.
        - Some products lack catalogue category or price metadata.
        - Price buckets are anonymized relative segments, not monetary prices.
        - Missing values are preserved rather than estimated.
        """
    )
