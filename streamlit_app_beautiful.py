# -*- coding: utf-8 -*-
"""
BIG DATA STREAMING DASHBOARD
Amazon Fashion Customer Sentiment Analysis
Streamlit Community Cloud Edition
"""

import gzip
import json
import time
import uuid
from datetime import datetime

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import streamlit as st
import streamlit.components.v1 as components

# ============================================================
# OPTIONAL LIBRARIES
# ============================================================
try:
    from confluent_kafka import Consumer, Producer
    HAS_KAFKA = True
except ImportError:
    HAS_KAFKA = False

try:
    from transformers import pipeline
    HAS_TRANSFORMERS = True
except ImportError:
    HAS_TRANSFORMERS = False


# ============================================================
# 1. PAGE CONFIG
# ============================================================
st.set_page_config(
    page_title="Big Data Streaming | Amazon Fashion",
    page_icon="📡",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ============================================================
# 2. GLOBAL DESIGN SYSTEM
# ============================================================
PRIMARY = "#2563EB"
NAVY = "#0F172A"
CYAN = "#06B6D4"
GREEN = "#16A34A"
GREEN_LIGHT = "#DCFCE7"
YELLOW = "#F59E0B"
RED = "#DC2626"
RED_LIGHT = "#FEE2E2"
GRAY = "#64748B"
LIGHT = "#F8FAFC"
BORDER = "#E2E8F0"

SENTIMENT_ORDER = [
    "Rất tích cực",
    "Tích cực",
    "Trung lập",
    "Tiêu cực",
    "Rất tiêu cực",
]

SENTIMENT_COLORS = {
    "Rất tích cực": "#15803D",
    "Tích cực": "#22C55E",
    "Trung lập": "#94A3B8",
    "Tiêu cực": "#F97316",
    "Rất tiêu cực": "#DC2626",
}

st.markdown(
    """
    <style>
    /* ---------- APP ---------- */
    .stApp {
        background:
            radial-gradient(circle at 5% 0%, rgba(37,99,235,.08), transparent 26%),
            radial-gradient(circle at 95% 0%, rgba(6,182,212,.06), transparent 24%),
            #F8FAFC;
    }

    .block-container {
        padding-top: 1.2rem;
        padding-bottom: 2.5rem;
        max-width: 1550px;
    }

    /* ---------- SIDEBAR ---------- */
    section[data-testid="stSidebar"] {
        background: linear-gradient(180deg, #071225 0%, #0B1730 52%, #102A56 100%);
        border-right: 1px solid rgba(255,255,255,.08);
    }

    section[data-testid="stSidebar"] * {
        color: #EAF2FF;
    }

    section[data-testid="stSidebar"] .stSlider label,
    section[data-testid="stSidebar"] .stRadio label,
    section[data-testid="stSidebar"] .stCheckbox label,
    section[data-testid="stSidebar"] .stTextInput label {
        font-weight: 650;
    }

    /* ---------- HERO ---------- */
    .hero {
        position: relative;
        overflow: hidden;
        background:
            radial-gradient(circle at 88% 18%, rgba(255,255,255,.19), transparent 20%),
            radial-gradient(circle at 75% 85%, rgba(6,182,212,.18), transparent 28%),
            linear-gradient(135deg, #08172D 0%, #123E8A 50%, #2563EB 100%);
        padding: 32px 36px;
        border-radius: 24px;
        color: white;
        box-shadow: 0 18px 45px rgba(30,64,175,.20);
        margin-bottom: 18px;
    }

    .hero-badge {
        display: inline-flex;
        gap: 8px;
        align-items: center;
        padding: 6px 11px;
        background: rgba(255,255,255,.12);
        border: 1px solid rgba(255,255,255,.18);
        border-radius: 999px;
        font-weight: 800;
        font-size: 11px;
        letter-spacing: 1.3px;
        margin-bottom: 12px;
    }

    .hero h1 {
        color: white !important;
        font-size: 2.15rem;
        line-height: 1.14;
        margin: 0 0 10px 0;
        letter-spacing: -.5px;
    }

    .hero p {
        max-width: 1000px;
        margin: 0;
        color: rgba(255,255,255,.88);
        font-size: 1rem;
        line-height: 1.65;
    }

    /* ---------- SECTION ---------- */
    .section-label {
        color: #2563EB;
        font-size: 11px;
        font-weight: 900;
        letter-spacing: 1.55px;
        text-transform: uppercase;
        margin-bottom: 2px;
    }

    .section-title {
        color: #0F172A;
        font-size: 1.48rem;
        font-weight: 850;
        margin-bottom: 2px;
    }

    .section-desc {
        color: #64748B;
        font-size: .92rem;
        margin-bottom: 13px;
    }

    /* ---------- PIPELINE ---------- */
    .pipeline-card {
        min-height: 118px;
        background: rgba(255,255,255,.94);
        border: 1px solid #E2E8F0;
        border-radius: 18px;
        padding: 17px 12px;
        text-align: center;
        box-shadow: 0 7px 22px rgba(15,23,42,.045);
        transition: transform .15s ease, box-shadow .15s ease;
    }

    .pipeline-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 10px 28px rgba(15,23,42,.08);
    }

    .pipeline-icon {
        font-size: 29px;
        margin-bottom: 4px;
    }

    .pipeline-title {
        color: #0F172A;
        font-size: 13px;
        font-weight: 850;
    }

    .pipeline-sub {
        color: #64748B;
        font-size: 11px;
        line-height: 1.35;
        margin-top: 3px;
    }

    /* ---------- STATUS ---------- */
    .status-wrap {
        background: white;
        border: 1px solid #E2E8F0;
        border-radius: 16px;
        padding: 13px 17px;
        box-shadow: 0 5px 18px rgba(15,23,42,.04);
        margin: 7px 0 16px 0;
    }

    .status-live {
        display: inline-flex;
        align-items: center;
        gap: 7px;
        background: #DCFCE7;
        color: #166534;
        border: 1px solid #BBF7D0;
        border-radius: 999px;
        padding: 5px 10px;
        font-weight: 850;
        font-size: 12px;
    }

    .status-idle {
        display: inline-flex;
        align-items: center;
        gap: 7px;
        background: #E0F2FE;
        color: #075985;
        border: 1px solid #BAE6FD;
        border-radius: 999px;
        padding: 5px 10px;
        font-weight: 850;
        font-size: 12px;
    }

    /* ---------- KPI ---------- */
    div[data-testid="stMetric"] {
        background: rgba(255,255,255,.98);
        border: 1px solid #E2E8F0;
        border-radius: 17px;
        padding: 13px 15px;
        box-shadow: 0 7px 21px rgba(15,23,42,.045);
    }

    div[data-testid="stMetricLabel"] {
        color: #64748B;
        font-weight: 700;
    }

    div[data-testid="stMetricValue"] {
        color: #0F172A;
        font-weight: 850;
    }

    /* ---------- INFO CARDS ---------- */
    .insight-card {
        background: white;
        border: 1px solid #E2E8F0;
        border-radius: 16px;
        padding: 16px 18px;
        min-height: 118px;
        box-shadow: 0 6px 18px rgba(15,23,42,.04);
    }

    .insight-title {
        color: #64748B;
        font-size: 11px;
        font-weight: 850;
        letter-spacing: .8px;
        text-transform: uppercase;
    }

    .insight-value {
        color: #0F172A;
        font-size: 1.25rem;
        font-weight: 900;
        margin-top: 4px;
    }

    .insight-note {
        color: #64748B;
        font-size: 12px;
        margin-top: 5px;
        line-height: 1.4;
    }

    /* ---------- BUTTON ---------- */
    .stButton > button {
        border-radius: 11px !important;
        min-height: 42px;
        font-weight: 800 !important;
    }

    /* ---------- TABS ---------- */
    button[data-baseweb="tab"] {
        font-weight: 800;
    }

    /* ---------- FOOTER ---------- */
    .footer {
        margin-top: 28px;
        border-top: 1px solid #E2E8F0;
        padding-top: 16px;
        color: #64748B;
        font-size: 12px;
        text-align: center;
    }

    @media (max-width: 800px) {
        .hero {
            padding: 24px 22px;
        }
        .hero h1 {
            font-size: 1.65rem;
        }
    }
    </style>
    """,
    unsafe_allow_html=True,
)


# ============================================================
# 3. AI SENTIMENT MODEL
# ============================================================
@st.cache_resource(show_spinner="Đang nạp mô hình AI RoBERTa Sentiment...")
def load_sentiment_model():
    """Load HuggingFace sentiment model once."""
    if not HAS_TRANSFORMERS:
        return None

    try:
        return pipeline(
            "sentiment-analysis",
            model="cardiffnlp/twitter-roberta-base-sentiment-latest",
            truncation=True,
            max_length=512,
        )
    except Exception:
        return None


sentiment_pipeline = load_sentiment_model()


def analyze_sentiment(text: str, rating: float):
    """
    Analyze sentiment using RoBERTa when available.
    Fall back to Amazon rating if model is unavailable.
    """
    effective_sentiment = "neutral"

    if sentiment_pipeline:
        try:
            pred = sentiment_pipeline(text[:500])[0]
            label = str(pred["label"]).lower()

            if rating <= 2.0 and "pos" in label:
                effective_sentiment = "negative"
            elif rating >= 4.0 and "neg" in label:
                effective_sentiment = "negative"
            else:
                if "pos" in label:
                    effective_sentiment = "positive"
                elif "neg" in label:
                    effective_sentiment = "negative"
                else:
                    effective_sentiment = "neutral"
        except Exception:
            effective_sentiment = (
                "positive" if rating >= 4.0
                else "negative" if rating <= 2.0
                else "neutral"
            )
    else:
        effective_sentiment = (
            "positive" if rating >= 4.0
            else "negative" if rating <= 2.0
            else "neutral"
        )

    if effective_sentiment == "positive":
        if rating >= 4.5:
            return "Rất tích cực", "#14532D", "#DCFCE7"
        return "Tích cực", "#166534", "#DCFCE7"

    if effective_sentiment == "negative":
        if rating <= 1.5:
            return "Rất tiêu cực", "#7F1D1D", "#FEE2E2"
        return "Tiêu cực", "#991B1B", "#FEE2E2"

    return "Trung lập", "#475569", "#F1F5F9"


# ============================================================
# 4. SESSION STATE
# ============================================================
if "streaming_active" not in st.session_state:
    st.session_state.streaming_active = False

if "data_records" not in st.session_state:
    st.session_state.data_records = []

if "stats" not in st.session_state:
    st.session_state.stats = {
        "generated": 0,
        "delivered": 0,
        "consumed": 0,
        "start_time": None,
        "last_update": None,
    }


# ============================================================
# 5. SIDEBAR
# ============================================================
with st.sidebar:
    st.markdown("## 📡 STREAM CONTROL")
    st.caption("Điều khiển phiên phân tích dữ liệu realtime")

    try:
        secrets_oci = st.secrets.get("oci_kafka", {})
    except Exception:
        secrets_oci = {}

    st.markdown("### ① Nguồn dữ liệu")
    data_source_mode = st.radio(
        "Chọn nguồn:",
        [
            "Amazon Fashion Online",
            "Dữ liệu mẫu dự phòng",
        ],
        index=0,
        label_visibility="collapsed",
    )

    st.markdown("### ② OCI / Kafka")
    use_oci_kafka = st.checkbox(
        "Kích hoạt Oracle Cloud Streaming",
        value=False,
    )

    bootstrap_server = st.text_input(
        "Bootstrap Server",
        value=secrets_oci.get(
            "bootstrap_servers",
            "cell-1.streaming.sa-saopaulo-1.oci.oraclecloud.com:9092",
        ),
        disabled=not use_oci_kafka,
    )

    kafka_topic = st.text_input(
        "Kafka Topic",
        value=secrets_oci.get("topic", "DemoStreamingFashion"),
        disabled=not use_oci_kafka,
    )

    sasl_user = st.text_input(
        "SASL Username",
        value=secrets_oci.get("sasl_username", ""),
        type="password",
        disabled=not use_oci_kafka,
    )

    auth_token = st.text_input(
        "Auth Token",
        value=secrets_oci.get("auth_token", ""),
        type="password",
        disabled=not use_oci_kafka,
    )

    st.markdown("### ③ Tham số demo")

    max_records = st.slider(
        "Số review tối đa",
        min_value=10,
        max_value=500,
        value=100,
        step=10,
    )

    stream_delay = st.slider(
        "Độ trễ mỗi event",
        min_value=0.0,
        max_value=2.0,
        value=0.10,
        step=0.05,
        format="%.2f giây",
    )

    st.markdown("---")

    b1, b2 = st.columns(2)
    with b1:
        start_btn = st.button(
            "▶ BẮT ĐẦU",
            type="primary",
            use_container_width=True,
        )
    with b2:
        stop_btn = st.button(
            "⏹ DỪNG",
            use_container_width=True,
        )

    reset_btn = st.button(
        "↻ LÀM MỚI DỮ LIỆU",
        use_container_width=True,
    )

    st.markdown("---")
    st.caption(
        "💡 Khi trình bày: chọn 50–100 review và delay 0.05–0.15 giây "
        "để dashboard cập nhật đủ nhanh nhưng vẫn nhìn rõ."
    )


# ============================================================
# 6. CONTROL ACTIONS
# ============================================================
if reset_btn:
    st.session_state.streaming_active = False
    st.session_state.data_records = []
    st.session_state.stats = {
        "generated": 0,
        "delivered": 0,
        "consumed": 0,
        "start_time": None,
        "last_update": None,
    }
    st.rerun()

if start_btn:
    st.session_state.streaming_active = True
    if st.session_state.stats["start_time"] is None:
        st.session_state.stats["start_time"] = time.time()

if stop_btn:
    st.session_state.streaming_active = False


# ============================================================
# 7. HERO HEADER
# ============================================================
st.markdown(
    """
    <div class="hero">
        <div class="hero-badge">📡 BIG DATA STREAMING · NLP · AMAZON FASHION</div>
        <h1>Phân tích độ hài lòng khách hàng theo thời gian thực</h1>
        <p>
            Hệ thống thu thập review Amazon Fashion, phân tích cảm xúc bằng mô hình
            RoBERTa, mô phỏng luồng Producer–Kafka–Consumer và trực quan hóa kết quả
            realtime trên một dashboard duy nhất.
        </p>
    </div>
    """,
    unsafe_allow_html=True,
)


# ============================================================
# 8. PIPELINE OVERVIEW
# ============================================================
st.markdown(
    """
    <div class="section-label">System Architecture</div>
    <div class="section-title">Luồng xử lý Big Data Streaming</div>
    <div class="section-desc">
        Người xem có thể hiểu toàn bộ hệ thống chỉ trong một hàng: dữ liệu đi từ nguồn
        review đến AI, streaming platform, consumer và dashboard.
    </div>
    """,
    unsafe_allow_html=True,
)

pipeline_cols = st.columns(6)
pipeline_items = [
    ("🛍️", "Amazon Fashion", "Customer reviews"),
    ("🧠", "RoBERTa NLP", "Sentiment AI"),
    ("📤", "Producer", "Event generation"),
    ("☁️", "OCI / Kafka", "Streaming layer"),
    ("📥", "Consumer", "Receive events"),
    ("📊", "Dashboard", "Realtime insight"),
]

for col, (icon, title, subtitle) in zip(pipeline_cols, pipeline_items):
    with col:
        st.markdown(
            f"""
            <div class="pipeline-card">
                <div class="pipeline-icon">{icon}</div>
                <div class="pipeline-title">{title}</div>
                <div class="pipeline-sub">{subtitle}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )


# ============================================================
# 9. STATUS BAR
# ============================================================
st.write("")

if st.session_state.streaming_active:
    status_badge = '<span class="status-live">● LIVE STREAMING</span>'
    status_text = "Hệ thống đang nhận và phân tích review theo thời gian thực."
else:
    status_badge = '<span class="status-idle">● READY</span>'
    status_text = "Hệ thống sẵn sàng. Bấm BẮT ĐẦU ở thanh điều khiển để chạy demo."

ai_status = "RoBERTa AI" if sentiment_pipeline else "Rating fallback"
stream_status = "OCI / Kafka" if use_oci_kafka else "Local simulation"

st.markdown(
    f"""
    <div class="status-wrap">
        {status_badge}
        <span style="color:#64748B;font-size:13px;margin-left:10px;">
            {status_text}
            &nbsp; · &nbsp; AI: <b>{ai_status}</b>
            &nbsp; · &nbsp; Streaming: <b>{stream_status}</b>
        </span>
    </div>
    """,
    unsafe_allow_html=True,
)


# ============================================================
# 10. PLACEHOLDERS
# ============================================================
kpi_placeholder = st.empty()
dashboard_placeholder = st.empty()


# ============================================================
# 11. DASHBOARD RENDERER
# ============================================================
def render_dashboard_content(records, stats):
    """Render KPI, insights, charts and recent reviews."""

    start_t = stats.get("start_time")
    elapsed_seconds = (time.time() - start_t) if start_t else 0

    generated = stats.get("generated", len(records))
    delivered = stats.get("delivered", len(records))
    consumed = stats.get("consumed", len(records))

    if records:
        df = pd.DataFrame(records)

        positive_count = df["emotion"].isin(
            ["Rất tích cực", "Tích cực"]
        ).sum()

        negative_count = df["emotion"].isin(
            ["Tiêu cực", "Rất tiêu cực"]
        ).sum()

        neutral_count = (df["emotion"] == "Trung lập").sum()

        positive_rate = positive_count / len(df) * 100
        negative_rate = negative_count / len(df) * 100
        avg_rating = df["amazon_rating"].mean()

        counts = (
            df["emotion"]
            .value_counts()
            .reindex(SENTIMENT_ORDER, fill_value=0)
        )

        dominant_sentiment = counts.idxmax()
        dominant_count = int(counts.max())
    else:
        df = pd.DataFrame()
        positive_count = 0
        negative_count = 0
        neutral_count = 0
        positive_rate = 0.0
        negative_rate = 0.0
        avg_rating = 0.0
        counts = pd.Series(
            [0] * len(SENTIMENT_ORDER),
            index=SENTIMENT_ORDER,
        )
        dominant_sentiment = "Chưa có dữ liệu"
        dominant_count = 0

    # --------------------------------------------------------
    # KPI
    # --------------------------------------------------------
    with kpi_placeholder.container():
        st.markdown(
            """
            <div class="section-label">Live Performance</div>
            <div class="section-title">Chỉ số vận hành realtime</div>
            <div class="section-desc">
                Theo dõi dữ liệu đi qua từng bước của pipeline và trạng thái cảm xúc khách hàng.
            </div>
            """,
            unsafe_allow_html=True,
        )

        k1, k2, k3, k4, k5, k6 = st.columns(6)

        k1.metric(
            "📥 GENERATED",
            f"{generated:,}",
            help="Số event đã được tạo từ review.",
        )
        k2.metric(
            "🚀 DELIVERED",
            f"{delivered:,}",
            help="Số event được ghi nhận là đã chuyển qua lớp streaming.",
        )
        k3.metric(
            "✅ CONSUMED",
            f"{consumed:,}",
            help="Số event đã được consumer xử lý.",
        )
        k4.metric(
            "⭐ POSITIVE",
            f"{positive_rate:.1f}%",
            help="Tỷ lệ review tích cực và rất tích cực.",
        )
        k5.metric(
            "🌟 AVG RATING",
            f"{avg_rating:.2f}/5" if records else "—",
        )
        k6.metric(
            "⏱ ELAPSED",
            f"{elapsed_seconds:.1f}s",
        )

    # --------------------------------------------------------
    # MAIN DASHBOARD
    # --------------------------------------------------------
    with dashboard_placeholder.container():

        tabs = st.tabs(
            [
                "📊 Tổng quan realtime",
                "🧠 Sentiment Analytics",
                "🧾 Review Stream",
                "⚙️ Thông tin hệ thống",
            ]
        )

        # ====================================================
        # TAB 1
        # ====================================================
        with tabs[0]:
            c1, c2, c3 = st.columns(3)

            with c1:
                st.markdown(
                    f"""
                    <div class="insight-card">
                        <div class="insight-title">Cảm xúc chủ đạo</div>
                        <div class="insight-value">{dominant_sentiment}</div>
                        <div class="insight-note">
                            {dominant_count:,} review đang thuộc nhóm cảm xúc này.
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

            with c2:
                st.markdown(
                    f"""
                    <div class="insight-card">
                        <div class="insight-title">Tỷ lệ tiêu cực</div>
                        <div class="insight-value">{negative_rate:.1f}%</div>
                        <div class="insight-note">
                            {negative_count:,} review cần được chú ý trong dữ liệu đã xử lý.
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

            with c3:
                st.markdown(
                    f"""
                    <div class="insight-card">
                        <div class="insight-title">Thông lượng demo</div>
                        <div class="insight-value">
                            {(consumed / elapsed_seconds):.1f} event/s
                            {"" if elapsed_seconds > 0 else ""}
                        </div>
                        <div class="insight-note">
                            Tốc độ consumer xử lý event trong phiên hiện tại.
                        </div>
                    </div>
                    """ if elapsed_seconds > 0 else """
                    <div class="insight-card">
                        <div class="insight-title">Thông lượng demo</div>
                        <div class="insight-value">—</div>
                        <div class="insight-note">
                            Chỉ số sẽ xuất hiện sau khi streaming bắt đầu.
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

            st.write("")

            left, right = st.columns([1.35, 1])

            with left:
                st.markdown("#### 📊 Phân phối cảm xúc khách hàng")

                if records:
                    df_chart = pd.DataFrame(
                        {
                            "Cảm xúc": SENTIMENT_ORDER,
                            "Số lượng": [int(counts[x]) for x in SENTIMENT_ORDER],
                        }
                    )

                    fig = px.bar(
                        df_chart,
                        x="Cảm xúc",
                        y="Số lượng",
                        color="Cảm xúc",
                        color_discrete_map=SENTIMENT_COLORS,
                        text="Số lượng",
                        category_orders={"Cảm xúc": SENTIMENT_ORDER},
                    )

                    fig.update_traces(
                        textposition="outside",
                        marker_line_width=0,
                    )

                    fig.update_layout(
                        height=355,
                        showlegend=False,
                        margin=dict(l=10, r=10, t=15, b=20),
                        xaxis_title="",
                        yaxis_title="Số review",
                        plot_bgcolor="white",
                        paper_bgcolor="white",
                    )

                    st.plotly_chart(
                        fig,
                        use_container_width=True,
                        key=f"sentiment_bar_{len(records)}",
                    )
                else:
                    st.info("Biểu đồ sẽ xuất hiện khi có review được xử lý.")

            with right:
                st.markdown("#### 🎯 Cơ cấu cảm xúc")

                if records:
                    pie_df = pd.DataFrame(
                        {
                            "Cảm xúc": SENTIMENT_ORDER,
                            "Số lượng": [int(counts[x]) for x in SENTIMENT_ORDER],
                        }
                    )

                    fig_pie = px.pie(
                        pie_df,
                        names="Cảm xúc",
                        values="Số lượng",
                        hole=0.63,
                        color="Cảm xúc",
                        color_discrete_map=SENTIMENT_COLORS,
                    )

                    fig_pie.update_layout(
                        height=355,
                        margin=dict(l=10, r=10, t=15, b=20),
                        legend_title="",
                    )

                    fig_pie.add_annotation(
                        text=f"<b>{len(records)}</b><br>reviews",
                        x=0.5,
                        y=0.5,
                        font_size=18,
                        showarrow=False,
                    )

                    st.plotly_chart(
                        fig_pie,
                        use_container_width=True,
                        key=f"sentiment_pie_{len(records)}",
                    )
                else:
                    st.info("Cơ cấu cảm xúc sẽ xuất hiện khi có dữ liệu.")

        # ====================================================
        # TAB 2
        # ====================================================
        with tabs[1]:
            if records:
                a, b = st.columns(2)

                with a:
                    rating_counts = (
                        df["amazon_rating"]
                        .round()
                        .astype(int)
                        .value_counts()
                        .reindex([1, 2, 3, 4, 5], fill_value=0)
                        .reset_index()
                    )
                    rating_counts.columns = ["Rating", "Số review"]

                    fig_rating = px.bar(
                        rating_counts,
                        x="Rating",
                        y="Số review",
                        text="Số review",
                        title="Phân phối Amazon Rating",
                    )
                    fig_rating.update_layout(
                        height=340,
                        margin=dict(l=10, r=10, t=55, b=20),
                        plot_bgcolor="white",
                        xaxis=dict(
                            tickmode="array",
                            tickvals=[1, 2, 3, 4, 5],
                            ticktext=["⭐", "⭐⭐", "⭐⭐⭐", "⭐⭐⭐⭐", "⭐⭐⭐⭐⭐"],
                        ),
                    )
                    st.plotly_chart(fig_rating, use_container_width=True)

                with b:
                    rating_sentiment = (
                        df.groupby("emotion")["amazon_rating"]
                        .mean()
                        .reindex(SENTIMENT_ORDER)
                        .dropna()
                        .reset_index()
                    )

                    fig_avg = px.bar(
                        rating_sentiment,
                        x="emotion",
                        y="amazon_rating",
                        color="emotion",
                        color_discrete_map=SENTIMENT_COLORS,
                        category_orders={"emotion": SENTIMENT_ORDER},
                        title="Rating trung bình theo nhóm cảm xúc",
                    )
                    fig_avg.update_layout(
                        height=340,
                        margin=dict(l=10, r=10, t=55, b=20),
                        showlegend=False,
                        xaxis_title="",
                        yaxis_title="Rating trung bình",
                        yaxis_range=[0, 5],
                        plot_bgcolor="white",
                    )
                    st.plotly_chart(fig_avg, use_container_width=True)

                st.markdown("#### 🔎 Insight từ dữ liệu hiện tại")

                pos_n = positive_count
                neg_n = negative_count

                insight_text = (
                    f"Trong **{len(records):,} review** đã xử lý, "
                    f"có **{pos_n:,} review tích cực** và **{neg_n:,} review tiêu cực**. "
                    f"Rating trung bình đạt **{avg_rating:.2f}/5**. "
                    f"Nhóm cảm xúc xuất hiện nhiều nhất là **{dominant_sentiment}**."
                )
                st.info(insight_text)
            else:
                st.info("Chưa có dữ liệu để thực hiện sentiment analytics.")

        # ====================================================
        # TAB 3
        # ====================================================
        with tabs[2]:
            st.markdown("#### 🧾 10 review mới nhất trong luồng")

            if records:
                recent_records = records[-10:][::-1]
                rows_code = ""

                for r in recent_records:
                    rating_val = float(r["amazon_rating"])
                    stars = "★" * max(1, min(5, int(round(rating_val))))
                    title_clean = (
                        str(r["title"])
                        .replace("&", "&amp;")
                        .replace("<", "&lt;")
                        .replace(">", "&gt;")
                    )

                    if len(title_clean) > 200:
                        title_clean = title_clean[:200] + "..."

                    emotion_val = r["emotion"]
                    bg_col = r.get("b_color", "#F1F5F9")
                    txt_col = r.get("t_color", "#475569")
                    timestamp = r.get("timestamp", "—")

                    rows_code += f"""
                    <tr>
                        <td class="time">{timestamp}</td>
                        <td class="rating">
                            <b>{rating_val:.1f}/5</b><br>
                            <span class="stars">{stars}</span>
                        </td>
                        <td class="review">{title_clean}</td>
                        <td class="emotion-cell">
                            <span class="emotion"
                                  style="background:{bg_col};color:{txt_col};">
                                {emotion_val}
                            </span>
                        </td>
                    </tr>
                    """

                html = f"""
                <!DOCTYPE html>
                <html>
                <head>
                <meta charset="utf-8">
                <style>
                    * {{ box-sizing:border-box; }}
                    body {{
                        margin:0;
                        padding:0;
                        font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Arial,sans-serif;
                        color:#0F172A;
                        background:white;
                    }}
                    .table-shell {{
                        border:1px solid #E2E8F0;
                        border-radius:15px;
                        overflow:hidden;
                        box-shadow:0 7px 20px rgba(15,23,42,.045);
                    }}
                    table {{
                        width:100%;
                        border-collapse:collapse;
                    }}
                    th {{
                        background:#0F172A;
                        color:white;
                        text-transform:uppercase;
                        letter-spacing:.55px;
                        font-size:11px;
                        padding:12px 11px;
                        text-align:left;
                    }}
                    td {{
                        border-bottom:1px solid #EEF2F7;
                        padding:11px;
                        font-size:12px;
                        vertical-align:middle;
                    }}
                    tr:last-child td {{
                        border-bottom:none;
                    }}
                    tr:hover {{
                        background:#F8FAFC;
                    }}
                    .time {{
                        width:10%;
                        color:#64748B;
                        font-weight:700;
                    }}
                    .rating {{
                        width:15%;
                        text-align:center;
                    }}
                    .stars {{
                        color:#F59E0B;
                        letter-spacing:1px;
                        font-size:11px;
                    }}
                    .review {{
                        width:55%;
                        line-height:1.45;
                        color:#334155;
                    }}
                    .emotion-cell {{
                        width:20%;
                        text-align:center;
                    }}
                    .emotion {{
                        display:inline-block;
                        padding:5px 10px;
                        border-radius:999px;
                        font-weight:800;
                        font-size:11px;
                    }}
                </style>
                </head>
                <body>
                    <div class="table-shell">
                        <table>
                            <thead>
                                <tr>
                                    <th>Time</th>
                                    <th style="text-align:center">Rating</th>
                                    <th>Customer Review</th>
                                    <th style="text-align:center">AI Sentiment</th>
                                </tr>
                            </thead>
                            <tbody>{rows_code}</tbody>
                        </table>
                    </div>
                </body>
                </html>
                """

                components.html(
                    html,
                    height=530,
                    scrolling=True,
                )

                export_df = df[
                    [
                        "timestamp",
                        "amazon_rating",
                        "emotion",
                        "title",
                    ]
                ].copy()

                export_df.columns = [
                    "Time",
                    "Rating",
                    "Sentiment",
                    "Review",
                ]

                csv = export_df.to_csv(
                    index=False,
                    encoding="utf-8-sig",
                )

                st.download_button(
                    "⬇️ TẢI KẾT QUẢ CSV",
                    data=csv,
                    file_name="amazon_fashion_sentiment_stream.csv",
                    mime="text/csv",
                    use_container_width=True,
                )
            else:
                st.info(
                    "Chưa có event trong luồng. Hãy bấm BẮT ĐẦU để xem review realtime."
                )

        # ====================================================
        # TAB 4
        # ====================================================
        with tabs[3]:
            sys1, sys2, sys3, sys4 = st.columns(4)

            sys1.metric(
                "Transformers",
                "Available" if HAS_TRANSFORMERS else "Fallback",
            )
            sys2.metric(
                "Kafka library",
                "Available" if HAS_KAFKA else "Unavailable",
            )
            sys3.metric(
                "Streaming mode",
                "OCI/Kafka" if use_oci_kafka else "Local",
            )
            sys4.metric(
                "Max records",
                f"{max_records:,}",
            )

            st.markdown("#### 🧩 Thành phần hệ thống")

            system_df = pd.DataFrame(
                [
                    ["Data Source", data_source_mode, "Review đầu vào"],
                    ["AI Model", "RoBERTa Sentiment" if sentiment_pipeline else "Rating fallback", "Phân loại cảm xúc"],
                    ["Producer", "Streaming Event Generator", "Tạo event"],
                    ["Streaming Layer", "OCI / Kafka" if use_oci_kafka else "Local Simulation", "Truyền dữ liệu"],
                    ["Consumer", "Stream Processor", "Nhận và xử lý event"],
                    ["Visualization", "Streamlit + Plotly", "Dashboard realtime"],
                ],
                columns=["Layer", "Technology", "Role"],
            )

            st.dataframe(
                system_df,
                use_container_width=True,
                hide_index=True,
            )

            with st.expander("📘 Cách đọc dashboard"):
                st.markdown(
                    """
                    **GENERATED** là số review đã được chuyển thành event.  
                    **DELIVERED** là số event được ghi nhận qua lớp streaming.  
                    **CONSUMED** là số event consumer đã nhận và xử lý.  
                    **POSITIVE** là tỷ lệ `Tích cực + Rất tích cực`.  
                    **AVG RATING** là rating trung bình của dữ liệu đã xử lý.
                    """
                )


# ============================================================
# 12. DATA SOURCE
# ============================================================
DATA_URL = (
    "https://mcauleylab.ucsd.edu/public_datasets/data/"
    "amazon_v2/categoryFiles/AMAZON_FASHION.json.gz"
)

SAMPLE_REVIEWS = [
    {
        "overall": 5.0,
        "reviewText": "Great quality and fit perfectly. Highly recommend this brand.",
    },
    {
        "overall": 5.0,
        "reviewText": "Very comfortable shoes, good cushion for long walks all day.",
    },
    {
        "overall": 4.0,
        "reviewText": "Nice color, good material and the size fits as expected.",
    },
    {
        "overall": 4.0,
        "reviewText": "Good product for the price. Packaging was also very careful.",
    },
    {
        "overall": 3.0,
        "reviewText": "Normal quality, acceptable for the discount price.",
    },
    {
        "overall": 3.0,
        "reviewText": "The item is okay, but delivery took longer than expected.",
    },
    {
        "overall": 2.0,
        "reviewText": "Fabric quality is lower than described and stitching feels weak.",
    },
    {
        "overall": 2.0,
        "reviewText": "Not very satisfied. The size is smaller than expected.",
    },
    {
        "overall": 1.0,
        "reviewText": "Terrible experience. Size is completely wrong and material feels cheap.",
    },
    {
        "overall": 1.0,
        "reviewText": "Very disappointed. The item was damaged after the first wash.",
    },
]


# ============================================================
# 13. INITIAL RENDER
# ============================================================
render_dashboard_content(
    st.session_state.data_records,
    st.session_state.stats,
)


# ============================================================
# 14. STREAMING LOOP
# ============================================================
if st.session_state.streaming_active:

    run_id = uuid.uuid4().hex[:8]

    progress_placeholder = st.empty()
    live_note = st.empty()

    try:
        if data_source_mode == "Dữ liệu mẫu dự phòng":
            lines_iterator = [
                json.dumps(r).encode("utf-8")
                for r in SAMPLE_REVIEWS * 100
            ]
        else:
            response = requests.get(
                DATA_URL,
                stream=True,
                timeout=20,
                headers={"User-Agent": "Mozilla/5.0"},
            )
            response.raise_for_status()

            gz_file = gzip.GzipFile(fileobj=response.raw)
            lines_iterator = gz_file

        for raw_line in lines_iterator:

            if not st.session_state.streaming_active:
                break

            if len(st.session_state.data_records) >= max_records:
                st.session_state.streaming_active = False
                live_note.success(
                    f"✅ Phiên streaming hoàn tất: {max_records:,} review đã được xử lý."
                )
                break

            if not raw_line.strip():
                continue

            try:
                record = json.loads(raw_line.decode("utf-8"))
            except Exception:
                continue

            try:
                rating = float(record.get("overall", 5.0))
            except Exception:
                rating = 5.0

            text = str(
                record.get("reviewText", "")
            ).strip()

            if not text:
                text = str(
                    record.get(
                        "summary",
                        "Standard fashion product review",
                    )
                ).strip()

            if not text:
                continue

            emotion_text, text_col, bg_col = analyze_sentiment(
                text,
                rating,
            )

            event = {
                "run_id": run_id,
                "amazon_rating": rating,
                "title": text,
                "emotion": emotion_text,
                "b_color": bg_col,
                "t_color": text_col,
                "timestamp": datetime.now().strftime("%H:%M:%S"),
            }

            st.session_state.data_records.append(event)
            st.session_state.stats["generated"] += 1

            # The original app treats delivered/consumed as realtime
            # counters for the simulated pipeline.
            st.session_state.stats["delivered"] += 1
            st.session_state.stats["consumed"] += 1
            st.session_state.stats["last_update"] = time.time()

            current = len(st.session_state.data_records)

            progress_placeholder.progress(
                min(current / max_records, 1.0),
                text=f"Streaming progress: {current:,}/{max_records:,} reviews",
            )

            render_dashboard_content(
                st.session_state.data_records,
                st.session_state.stats,
            )

            if stream_delay > 0:
                time.sleep(stream_delay)

    except Exception as exc:

        live_note.warning(
            f"⚠️ Không đọc được dữ liệu Amazon Online. "
            f"Hệ thống chuyển sang dữ liệu mẫu dự phòng."
        )

        for r in SAMPLE_REVIEWS * 100:

            if (
                not st.session_state.streaming_active
                or len(st.session_state.data_records) >= max_records
            ):
                break

            rating = float(r.get("overall", 5.0))
            text = str(r.get("reviewText", "")).strip()

            emotion_text, text_col, bg_col = analyze_sentiment(
                text,
                rating,
            )

            event = {
                "run_id": run_id,
                "amazon_rating": rating,
                "title": text,
                "emotion": emotion_text,
                "b_color": bg_col,
                "t_color": text_col,
                "timestamp": datetime.now().strftime("%H:%M:%S"),
            }

            st.session_state.data_records.append(event)
            st.session_state.stats["generated"] += 1
            st.session_state.stats["delivered"] += 1
            st.session_state.stats["consumed"] += 1
            st.session_state.stats["last_update"] = time.time()

            current = len(st.session_state.data_records)

            progress_placeholder.progress(
                min(current / max_records, 1.0),
                text=f"Fallback stream: {current:,}/{max_records:,} reviews",
            )

            render_dashboard_content(
                st.session_state.data_records,
                st.session_state.stats,
            )

            time.sleep(stream_delay if stream_delay > 0 else 0.08)

        st.session_state.streaming_active = False


# ============================================================
# 15. FOOTER
# ============================================================
st.markdown(
    """
    <div class="footer">
        <b>Big Data Streaming Dashboard</b> · Amazon Fashion · RoBERTa NLP ·
        OCI/Kafka Architecture · Streamlit Visualization
    </div>
    """,
    unsafe_allow_html=True,
)
