# -*- coding: utf-8 -*-
"""
BIG DATA STREAMING — AMAZON FASHION
===================================
Streamlit dashboard converted from the original Google Colab notebook.

Pipeline:
Amazon Fashion Dataset
    → Batch RoBERTa Sentiment Analysis
    → Kafka Producer
    → Oracle Cloud Infrastructure Streaming
    → Kafka Consumer
    → Realtime Streamlit Dashboard

Security:
- OCI credentials are read from Streamlit Secrets or environment variables.
- Never hard-code credentials in this file.
"""

from __future__ import annotations

import gzip
import json
import os
import queue
import threading
import time
import uuid
import warnings
from datetime import datetime, timezone
from typing import Any

import certifi
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import streamlit as st
import torch
from confluent_kafka import Consumer, Producer
from transformers import pipeline

warnings.filterwarnings("ignore")

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
# 2. GLOBAL CONSTANTS
# ============================================================

DATA_URL = (
    "https://mcauleylab.ucsd.edu/public_datasets/data/"
    "amazon_v2/categoryFiles/AMAZON_FASHION.json.gz"
)

DEFAULT_BOOTSTRAP = "cell-1.streaming.sa-saopaulo-1.oci.oraclecloud.com:9092"
DEFAULT_TOPIC = "DemoStreamingFashion"
MODEL_NAME = "cardiffnlp/twitter-roberta-base-sentiment-latest"

EMOTION_ORDER = [
    "Rất tích cực",
    "Tích cực",
    "Trung lập",
    "Tiêu cực",
    "Rất tiêu cực",
]

EMOTION_COLORS = {
    "Rất tích cực": "#16A34A",
    "Tích cực": "#4ADE80",
    "Trung lập": "#94A3B8",
    "Tiêu cực": "#FB7185",
    "Rất tiêu cực": "#DC2626",
}

# ============================================================
# 3. CUSTOM UI
# ============================================================

st.markdown(
    """
    <style>
    .stApp {
        background:
            radial-gradient(circle at 0% 0%, rgba(37,99,235,.08), transparent 28%),
            radial-gradient(circle at 100% 10%, rgba(124,58,237,.07), transparent 26%),
            #F8FAFC;
    }

    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #071225 0%, #0B1730 60%, #0F2244 100%);
    }

    [data-testid="stSidebar"] * {
        color: #E5EEF9;
    }

    .hero {
        padding: 30px 34px;
        border-radius: 24px;
        color: white;
        background:
            radial-gradient(circle at 86% 20%, rgba(255,255,255,.16), transparent 25%),
            linear-gradient(135deg, #071225 0%, #123E8A 48%, #2563EB 100%);
        box-shadow: 0 20px 45px rgba(15, 45, 100, .18);
        margin-bottom: 18px;
        overflow: hidden;
    }

    .eyebrow {
        font-size: 12px;
        letter-spacing: 2.1px;
        font-weight: 800;
        opacity: .75;
        margin-bottom: 8px;
    }

    .hero h1 {
        font-size: 36px;
        line-height: 1.15;
        margin: 0 0 10px 0;
    }

    .hero p {
        max-width: 920px;
        font-size: 16px;
        line-height: 1.65;
        opacity: .88;
        margin: 0;
    }

    .flow-box {
        background: white;
        border: 1px solid #E2E8F0;
        border-radius: 18px;
        padding: 17px 18px;
        box-shadow: 0 8px 20px rgba(15,23,42,.04);
        text-align: center;
        min-height: 112px;
    }

    .flow-icon {
        font-size: 28px;
        margin-bottom: 4px;
    }

    .flow-title {
        color: #0F172A;
        font-weight: 800;
        font-size: 14px;
    }

    .flow-sub {
        color: #64748B;
        font-size: 12px;
        margin-top: 3px;
    }

    .status-pill {
        display: inline-block;
        padding: 7px 12px;
        border-radius: 999px;
        font-size: 12px;
        font-weight: 800;
        background: #DCFCE7;
        color: #166534;
        border: 1px solid #BBF7D0;
    }

    .status-local {
        background: #FEF3C7;
        color: #92400E;
        border-color: #FDE68A;
    }

    .info-panel {
        background: #FFFFFF;
        border: 1px solid #E2E8F0;
        border-radius: 18px;
        padding: 18px 20px;
        box-shadow: 0 8px 24px rgba(15,23,42,.04);
    }

    .section-kicker {
        color: #2563EB;
        font-size: 12px;
        font-weight: 900;
        letter-spacing: 1.4px;
        text-transform: uppercase;
        margin-bottom: 3px;
    }

    .section-title {
        color: #0F172A;
        font-size: 25px;
        font-weight: 850;
        margin-bottom: 4px;
    }

    .section-desc {
        color: #64748B;
        font-size: 14px;
        margin-bottom: 14px;
    }

    .footer {
        margin-top: 28px;
        padding: 18px 20px;
        text-align: center;
        color: #64748B;
        font-size: 12px;
        border-top: 1px solid #E2E8F0;
    }

    div[data-testid="stMetric"] {
        background: white;
        border: 1px solid #E2E8F0;
        padding: 14px 16px;
        border-radius: 16px;
        box-shadow: 0 6px 18px rgba(15,23,42,.045);
    }

    div[data-testid="stMetricLabel"] {
        color: #64748B;
    }

    div[data-testid="stMetricValue"] {
        color: #0F172A;
    }

    .stButton > button {
        width: 100%;
        border: none;
        border-radius: 12px;
        padding: .7rem 1rem;
        font-weight: 800;
        background: linear-gradient(135deg, #2563EB, #4F46E5);
        color: white;
        box-shadow: 0 7px 20px rgba(37,99,235,.24);
    }

    .stButton > button:hover {
        border: none;
        color: white;
        transform: translateY(-1px);
    }

    @media (max-width: 800px) {
        .hero h1 { font-size: 28px; }
        .hero { padding: 24px 22px; }
    }
    </style>
    """,
    unsafe_allow_html=True,
)


# ============================================================
# 4. HELPERS
# ============================================================

def get_secret(name: str, default: str | None = None) -> str | None:
    """Read a value from Streamlit Secrets first, then environment variables."""
    try:
        if name in st.secrets:
            value = st.secrets[name]
            return str(value) if value is not None else default
    except Exception:
        pass
    return os.getenv(name, default)


def normalize_sentiment(amazon_rating: float, ai_label: str) -> str:
    """
    Preserve the rule used in the original notebook:
    combine Amazon star rating with the AI sentiment label.
    """
    ai_label = str(ai_label).lower()

    if amazon_rating <= 2.0 and "pos" in ai_label:
        sentiment = "negative"
    elif amazon_rating >= 4.0 and "neg" in ai_label:
        sentiment = "negative"
    else:
        if "pos" in ai_label:
            sentiment = "positive"
        elif "neg" in ai_label:
            sentiment = "negative"
        else:
            sentiment = "neutral"

    if sentiment == "positive":
        return "Rất tích cực" if amazon_rating >= 4.5 else "Tích cực"
    if sentiment == "negative":
        return "Rất tiêu cực" if amazon_rating <= 1.5 else "Tiêu cực"
    return "Trung lập"


@st.cache_resource(show_spinner=False)
def load_sentiment_model():
    """Load RoBERTa once per Streamlit process."""
    device = 0 if torch.cuda.is_available() else -1
    analyzer = pipeline(
        "sentiment-analysis",
        model=MODEL_NAME,
        device=device,
    )
    return analyzer, device


def device_name(device: int) -> str:
    if device == 0 and torch.cuda.is_available():
        return f"GPU · {torch.cuda.get_device_name(0)}"
    return "CPU"


def build_kafka_configs(
    bootstrap_servers: str,
    topic: str,
    username: str,
    password: str,
    run_id: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    common = {
        "bootstrap.servers": bootstrap_servers,
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.username": username,
        "sasl.password": password,
        "ssl.ca.location": certifi.where(),
    }

    producer_conf = {
        **common,
        "client.id": f"prod_{run_id}",
        "linger.ms": 20,
        "batch.num.messages": 1000,
        "acks": "1",
    }

    consumer_conf = {
        **common,
        "client.id": f"cons_{run_id}",
        "group.id": f"fashion_stream_{run_id}",
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
    }

    return producer_conf, consumer_conf


def sentiment_summary(df: pd.DataFrame) -> pd.DataFrame:
    counts = (
        df["emotion"]
        .value_counts()
        .reindex(EMOTION_ORDER, fill_value=0)
        .rename_axis("Cảm xúc")
        .reset_index(name="Số review")
    )
    return counts


def make_sentiment_bar(df: pd.DataFrame):
    summary = sentiment_summary(df)
    fig = px.bar(
        summary,
        x="Cảm xúc",
        y="Số review",
        color="Cảm xúc",
        color_discrete_map=EMOTION_COLORS,
        category_orders={"Cảm xúc": EMOTION_ORDER},
        text_auto=True,
        title="Phân phối cảm xúc của khách hàng",
    )
    fig.update_layout(
        height=390,
        showlegend=False,
        margin=dict(l=10, r=10, t=60, b=20),
        plot_bgcolor="white",
        paper_bgcolor="white",
        xaxis_title=None,
        yaxis_title="Số review",
    )
    fig.update_traces(marker_line_width=0)
    return fig


def make_rating_donut(df: pd.DataFrame):
    rating_group = (
        df.assign(
            rating_group=pd.cut(
                df["amazon_rating"],
                bins=[0, 2, 3, 5],
                labels=["1–2 sao", "3 sao", "4–5 sao"],
                include_lowest=True,
            )
        )["rating_group"]
        .value_counts()
        .reindex(["1–2 sao", "3 sao", "4–5 sao"], fill_value=0)
        .reset_index()
    )
    rating_group.columns = ["Nhóm rating", "Số review"]

    fig = px.pie(
        rating_group,
        values="Số review",
        names="Nhóm rating",
        hole=0.62,
        title="Cơ cấu rating Amazon",
    )
    fig.update_layout(
        height=390,
        margin=dict(l=10, r=10, t=60, b=20),
        paper_bgcolor="white",
        legend_title=None,
    )
    return fig


def make_confidence_hist(df: pd.DataFrame):
    fig = px.histogram(
        df,
        x="ai_score",
        nbins=20,
        title="Độ tin cậy của mô hình AI",
        labels={"ai_score": "AI confidence"},
    )
    fig.update_layout(
        height=330,
        showlegend=False,
        margin=dict(l=10, r=10, t=60, b=20),
        plot_bgcolor="white",
        paper_bgcolor="white",
        xaxis_tickformat=".0%",
        yaxis_title="Số review",
    )
    return fig


def recent_review_table(df: pd.DataFrame, n: int = 8) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["Rating", "Cảm xúc", "AI confidence", "Review"])

    out = df.tail(n).iloc[::-1].copy()
    out["Rating"] = out["amazon_rating"].map(lambda x: f"{float(x):.1f}/5")
    out["AI confidence"] = out["ai_score"].map(lambda x: f"{float(x):.1%}")
    out["Review"] = out["title"].astype(str).str.slice(0, 220)
    out["Cảm xúc"] = out["emotion"]
    return out[["Rating", "Cảm xúc", "AI confidence", "Review"]]


# ============================================================
# 5. HEADER
# ============================================================

st.markdown(
    """
    <div class="hero">
        <div class="eyebrow">BIG DATA STREAMING · AMAZON FASHION · OCI KAFKA · AI</div>
        <h1>Phân tích độ hài lòng khách hàng theo thời gian thực</h1>
        <p>
            Hệ thống đọc Amazon Fashion reviews, phân tích cảm xúc bằng RoBERTa,
            phát sự kiện qua Oracle Cloud Infrastructure Streaming tương thích Kafka,
            tiêu thụ luồng dữ liệu và trực quan hóa kết quả ngay trên Streamlit.
        </p>
    </div>
    """,
    unsafe_allow_html=True,
)

# Architecture ribbon
arch_cols = st.columns(6)
architecture = [
    ("🛍️", "Amazon Fashion", "Review dataset"),
    ("🤖", "RoBERTa AI", "Batch sentiment"),
    ("📤", "Producer", "Kafka event"),
    ("☁️", "OCI Streaming", "SASL_SSL"),
    ("📥", "Consumer", "Realtime poll"),
    ("📊", "Dashboard", "KPI & insight"),
]
for col, (icon, title, sub) in zip(arch_cols, architecture):
    with col:
        st.markdown(
            f"""
            <div class="flow-box">
                <div class="flow-icon">{icon}</div>
                <div class="flow-title">{title}</div>
                <div class="flow-sub">{sub}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

st.write("")

# ============================================================
# 6. SIDEBAR CONFIG
# ============================================================

with st.sidebar:
    st.markdown("## 📡 STREAM CONTROL")
    st.caption("Thiết lập phiên demo trước khi chạy")

    st.markdown("### Dữ liệu & AI")
    max_records = st.slider(
        "Số review tối đa",
        min_value=50,
        max_value=500,
        value=200,
        step=50,
        help="Notebook gốc dùng tối đa 500 review.",
    )

    ai_batch_size = st.select_slider(
        "AI batch size",
        options=[4, 8, 16, 24, 32],
        value=16,
        help="Batch nhỏ phù hợp hơn với Streamlit Cloud CPU.",
    )

    duration_seconds = st.slider(
        "Thời gian tối đa (giây)",
        min_value=30,
        max_value=180,
        value=120,
        step=15,
    )

    fallback_after_seconds = st.slider(
        "Fallback sau (giây)",
        min_value=4,
        max_value=30,
        value=10,
        step=2,
        help="Nếu OCI không xác nhận delivery, app chuyển sang local queue để demo không bị đứng.",
    )

    refresh_seconds = st.select_slider(
        "Refresh dashboard (giây)",
        options=[1, 2, 3, 4, 5],
        value=2,
    )

    st.markdown("---")
    st.markdown("### OCI Streaming")

    bootstrap_servers = get_secret("BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP)
    topic = get_secret("TOPIC", DEFAULT_TOPIC)
    sasl_username = get_secret("OCI_SASL_USERNAME")
    auth_token = get_secret("OCI_AUTH_TOKEN")

    st.caption(f"Topic: `{topic}`")
    st.caption(f"Broker: `{bootstrap_servers}`")

    credentials_ready = bool(sasl_username and auth_token)
    if credentials_ready:
        st.success("OCI Secrets đã được nhận.")
    else:
        st.warning(
            "Chưa có OCI Secrets. App vẫn có thể chạy ở Local Demo Mode."
        )

    st.markdown("---")
    start_button = st.button(
        "▶ CHẠY STREAMING DEMO",
        type="primary",
        use_container_width=True,
    )

    if st.button("🧹 Xóa kết quả hiện tại", use_container_width=True):
        st.session_state.pop("results_df", None)
        st.session_state.pop("run_summary", None)
        st.rerun()

# ============================================================
# 7. MODEL STATUS
# ============================================================

status_col1, status_col2, status_col3 = st.columns([1.2, 1, 1])

with status_col1:
    st.markdown(
        """
        <div class="section-kicker">AI ENGINE</div>
        <div class="section-title">RoBERTa Sentiment</div>
        <div class="section-desc">
            Model: cardiffnlp/twitter-roberta-base-sentiment-latest
        </div>
        """,
        unsafe_allow_html=True,
    )

with status_col2:
    st.metric("Giới hạn phiên demo", f"{max_records:,} reviews")

with status_col3:
    st.metric(
        "Kết nối ưu tiên",
        "OCI Streaming" if credentials_ready else "Local Demo",
    )

# ============================================================
# 8. STREAMING ENGINE
# ============================================================

def run_stream_demo() -> tuple[pd.DataFrame, dict[str, Any]]:
    """
    Run one bounded streaming demo.

    Unlike a notebook's endless visual update loop, this function runs only after
    a button click, updates Streamlit placeholders, and finishes automatically.
    """
    run_id = uuid.uuid4().hex[:8]
    use_fallback = not credentials_ready
    mode_text = "LOCAL DEMO" if use_fallback else "OCI STREAMING"

    local_queue: queue.Queue[bytes] = queue.Queue()
    producer_done = threading.Event()
    stop_event = threading.Event()
    producer_lock = threading.Lock()

    producer_stats = {
        "generated": 0,
        "delivered": 0,
        "failed": 0,
        "error": "Đang khởi tạo...",
    }

    producer = None
    consumer = None

    analyzer, device = load_sentiment_model()

    if credentials_ready:
        producer_conf, consumer_conf = build_kafka_configs(
            bootstrap_servers=bootstrap_servers,
            topic=topic,
            username=sasl_username,
            password=auth_token,
            run_id=run_id,
        )
        try:
            producer = Producer(producer_conf)
            consumer = Consumer(consumer_conf)
            consumer.subscribe([topic])
        except Exception as exc:
            use_fallback = True
            mode_text = "LOCAL FALLBACK"
            producer_stats["error"] = f"Không khởi tạo được OCI: {exc}"

    def delivery_report(err, msg):
        with producer_lock:
            if err:
                producer_stats["failed"] += 1
                producer_stats["error"] = str(err)
            else:
                producer_stats["delivered"] += 1

    def process_batch(batch_records):
        nonlocal use_fallback, mode_text

        texts = [r["title"][:500] for r in batch_records]

        predictions = analyzer(
            texts,
            batch_size=min(ai_batch_size, len(texts)),
            truncation=True,
            max_length=512,
        )

        for raw_record, pred in zip(batch_records, predictions):
            rating = float(raw_record["amazon_rating"])
            emotion = normalize_sentiment(rating, pred["label"])

            event = {
                "run_id": run_id,
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                "amazon_rating": rating,
                "title": raw_record["title"],
                "ai_label": pred["label"],
                "ai_score": float(pred["score"]),
                "emotion": emotion,
            }

            payload = json.dumps(event, ensure_ascii=False).encode("utf-8")
            local_queue.put(payload)

            if not use_fallback and producer is not None:
                try:
                    producer.produce(
                        topic,
                        value=payload,
                        on_delivery=delivery_report,
                    )
                    producer.poll(0)
                except Exception as exc:
                    with producer_lock:
                        producer_stats["failed"] += 1
                        producer_stats["error"] = str(exc)

            with producer_lock:
                producer_stats["generated"] += 1

    def producer_worker():
        nonlocal use_fallback, mode_text
        batch = []

        try:
            response = requests.get(
                DATA_URL,
                stream=True,
                timeout=60,
                headers={"User-Agent": "Mozilla/5.0"},
            )
            response.raise_for_status()

            with gzip.GzipFile(fileobj=response.raw) as gz:
                for line in gz:
                    if stop_event.is_set():
                        break

                    with producer_lock:
                        if producer_stats["generated"] >= max_records:
                            break

                    if not line.strip():
                        continue

                    try:
                        raw = json.loads(line.decode("utf-8"))
                    except Exception:
                        continue

                    text = str(raw.get("reviewText", "")).strip()
                    if not text:
                        continue

                    try:
                        rating = float(raw.get("overall", 0.0))
                    except Exception:
                        rating = 0.0

                    batch.append(
                        {
                            "amazon_rating": rating,
                            "title": text,
                        }
                    )

                    with producer_lock:
                        remaining = max_records - producer_stats["generated"]

                    target_batch = max(1, min(ai_batch_size, remaining))

                    if len(batch) >= target_batch:
                        process_batch(batch[:target_batch])
                        batch = []

                if batch and not stop_event.is_set():
                    with producer_lock:
                        remaining = max_records - producer_stats["generated"]
                    if remaining > 0:
                        process_batch(batch[:remaining])

            if not use_fallback and producer is not None:
                producer.flush(8)

            with producer_lock:
                if producer_stats["failed"] == 0:
                    producer_stats["error"] = "Không có lỗi nghiêm trọng."

        except Exception as exc:
            with producer_lock:
                producer_stats["error"] = f"Lỗi producer: {exc}"
        finally:
            producer_done.set()

    # UI placeholders
    run_status = st.empty()
    progress = st.progress(0)
    metric_placeholder = st.empty()
    chart_placeholder = st.empty()
    table_placeholder = st.empty()
    technical_placeholder = st.empty()

    producer_thread = threading.Thread(
        target=producer_worker,
        daemon=True,
    )
    producer_thread.start()

    rows: list[dict[str, Any]] = []
    consumed_count = 0
    started_at = time.monotonic()
    last_render = 0.0

    try:
        while True:
            elapsed = time.monotonic() - started_at

            if elapsed >= duration_seconds:
                break

            # If OCI has not delivered anything after the threshold,
            # switch to local queue so the class demo continues.
            if not use_fallback and elapsed > fallback_after_seconds:
                with producer_lock:
                    if producer_stats["delivered"] == 0:
                        use_fallback = True
                        mode_text = "LOCAL FALLBACK"

            message_payload = None

            if not use_fallback and consumer is not None:
                msg = consumer.poll(0.12)
                if msg is not None and not msg.error():
                    message_payload = msg.value()
            else:
                try:
                    message_payload = local_queue.get(timeout=0.04)
                except queue.Empty:
                    message_payload = None

            if message_payload:
                try:
                    event = json.loads(message_payload.decode("utf-8"))
                    if event.get("run_id") == run_id:
                        rows.append(event)
                        consumed_count += 1
                except Exception:
                    pass

            with producer_lock:
                generated_now = int(producer_stats["generated"])
                delivered_now = int(producer_stats["delivered"])

            if producer_done.is_set() and generated_now >= max_records:
                if use_fallback and local_queue.empty():
                    break
                if (
                    not use_fallback
                    and consumed_count >= min(max_records, max(1, delivered_now))
                ):
                    break

            now = time.monotonic()
            if now - last_render >= refresh_seconds:
                current_df = pd.DataFrame(rows)

                mode_class = "status-local" if "LOCAL" in mode_text else ""
                run_status.markdown(
                    f"""
                    <div class="info-panel">
                        <span class="status-pill {mode_class}">
                            ● {mode_text}
                        </span>
                        &nbsp;&nbsp;
                        <b>Run ID:</b> {run_id}
                        &nbsp;·&nbsp;
                        <b>AI:</b> {device_name(device)}
                        &nbsp;·&nbsp;
                        <b>Elapsed:</b> {elapsed:.1f}s
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

                progress.progress(
                    min(100, int(generated_now / max_records * 100))
                )

                positive_count = (
                    current_df["emotion"].isin(["Rất tích cực", "Tích cực"]).sum()
                    if not current_df.empty else 0
                )
                negative_count = (
                    current_df["emotion"].isin(["Rất tiêu cực", "Tiêu cực"]).sum()
                    if not current_df.empty else 0
                )
                positive_rate = (
                    positive_count / len(current_df) * 100
                    if len(current_df) else 0
                )
                negative_rate = (
                    negative_count / len(current_df) * 100
                    if len(current_df) else 0
                )

                with metric_placeholder.container():
                    m1, m2, m3, m4, m5, m6 = st.columns(6)
                    m1.metric("Đã xử lý", f"{generated_now:,}")
                    m2.metric("OCI delivered", f"{delivered_now:,}")
                    m3.metric("Consumer nhận", f"{consumed_count:,}")
                    m4.metric("Positive", f"{positive_rate:.1f}%")
                    m5.metric("Negative", f"{negative_rate:.1f}%")
                    m6.metric("Thời gian", f"{elapsed:.1f}s")

                if not current_df.empty:
                    with chart_placeholder.container():
                        c1, c2 = st.columns([1.3, 1])
                        with c1:
                            st.plotly_chart(
                                make_sentiment_bar(current_df),
                                use_container_width=True,
                                key=f"live_sentiment_{len(current_df)}",
                            )
                        with c2:
                            st.plotly_chart(
                                make_rating_donut(current_df),
                                use_container_width=True,
                                key=f"live_rating_{len(current_df)}",
                            )

                    with table_placeholder.container():
                        st.markdown("#### 🧾 Phản hồi gần nhất")
                        st.dataframe(
                            recent_review_table(current_df),
                            use_container_width=True,
                            hide_index=True,
                        )

                with technical_placeholder.container():
                    with st.expander("⚙️ Trạng thái kỹ thuật", expanded=False):
                        st.write(
                            {
                                "mode": mode_text,
                                "run_id": run_id,
                                "generated": generated_now,
                                "oci_delivered": delivered_now,
                                "consumer_received": consumed_count,
                                "failed": producer_stats["failed"],
                                "message": producer_stats["error"],
                            }
                        )

                last_render = now

            time.sleep(0.01)

    finally:
        stop_event.set()
        producer_thread.join(timeout=3)

        if consumer is not None:
            try:
                consumer.close()
            except Exception:
                pass

        if producer is not None:
            try:
                producer.flush(2)
            except Exception:
                pass

    elapsed = time.monotonic() - started_at
    final_df = pd.DataFrame(rows)

    with producer_lock:
        stats = dict(producer_stats)

    summary = {
        "run_id": run_id,
        "mode": mode_text,
        "device": device_name(device),
        "elapsed_seconds": elapsed,
        "generated": int(stats["generated"]),
        "delivered": int(stats["delivered"]),
        "failed": int(stats["failed"]),
        "consumed": int(consumed_count),
        "message": stats["error"],
    }

    return final_df, summary


# ============================================================
# 9. START DEMO
# ============================================================

if start_button:
    st.markdown("---")
    st.markdown(
        """
        <div class="section-kicker">LIVE MONITOR</div>
        <div class="section-title">Realtime Streaming Dashboard</div>
        <div class="section-desc">
            Dashboard cập nhật trong lúc Producer, AI và Consumer đang xử lý dữ liệu.
        </div>
        """,
        unsafe_allow_html=True,
    )

    try:
        with st.spinner(
            "Đang chuẩn bị RoBERTa và khởi chạy pipeline streaming..."
        ):
            results_df, run_summary = run_stream_demo()

        st.session_state["results_df"] = results_df
        st.session_state["run_summary"] = run_summary

        st.success(
            f"Hoàn tất phiên demo · {run_summary['generated']:,} review đã xử lý · "
            f"{run_summary['consumed']:,} event được consumer nhận."
        )

    except Exception as exc:
        st.error("Không thể hoàn tất phiên streaming.")
        st.exception(exc)

# ============================================================
# 10. FINAL ANALYTICS
# ============================================================

results_df = st.session_state.get("results_df")
run_summary = st.session_state.get("run_summary")

if isinstance(results_df, pd.DataFrame) and not results_df.empty:
    st.markdown("---")
    st.markdown(
        """
        <div class="section-kicker">RESULTS</div>
        <div class="section-title">Tổng kết phiên phân tích</div>
        <div class="section-desc">
            Các KPI và biểu đồ bên dưới được tính từ những sự kiện consumer đã nhận.
        </div>
        """,
        unsafe_allow_html=True,
    )

    total = len(results_df)
    positive = results_df["emotion"].isin(["Rất tích cực", "Tích cực"]).sum()
    neutral = (results_df["emotion"] == "Trung lập").sum()
    negative = results_df["emotion"].isin(["Rất tiêu cực", "Tiêu cực"]).sum()
    avg_rating = results_df["amazon_rating"].mean()
    avg_conf = results_df["ai_score"].mean()

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Consumer events", f"{total:,}")
    c2.metric("Positive", f"{positive / total * 100:.1f}%")
    c3.metric("Neutral", f"{neutral / total * 100:.1f}%")
    c4.metric("Negative", f"{negative / total * 100:.1f}%")
    c5.metric("Rating TB", f"{avg_rating:.2f}/5")
    c6.metric("AI confidence", f"{avg_conf:.1%}")

    tab1, tab2, tab3 = st.tabs(
        ["📊 Tổng quan", "🧠 AI & Sentiment", "🧾 Dữ liệu chi tiết"]
    )

    with tab1:
        a, b = st.columns([1.3, 1])
        with a:
            st.plotly_chart(
                make_sentiment_bar(results_df),
                use_container_width=True,
            )
        with b:
            st.plotly_chart(
                make_rating_donut(results_df),
                use_container_width=True,
            )

        st.markdown("#### 💡 Insight nhanh")
        dominant = (
            results_df["emotion"].value_counts().idxmax()
            if not results_df.empty
            else "Không xác định"
        )
        dominant_pct = (
            results_df["emotion"].value_counts(normalize=True).max() * 100
            if not results_df.empty
            else 0
        )

        insight_cols = st.columns(3)
        insight_cols[0].info(
            f"**Cảm xúc chiếm ưu thế:** {dominant} ({dominant_pct:.1f}%)."
        )
        insight_cols[1].info(
            f"**Rating trung bình:** {avg_rating:.2f}/5."
        )
        insight_cols[2].info(
            f"**Độ tin cậy AI trung bình:** {avg_conf:.1%}."
        )

    with tab2:
        st.plotly_chart(
            make_confidence_hist(results_df),
            use_container_width=True,
        )

        cross = pd.crosstab(
            results_df["emotion"],
            pd.cut(
                results_df["amazon_rating"],
                bins=[0, 2, 3, 5],
                labels=["1–2 sao", "3 sao", "4–5 sao"],
                include_lowest=True,
            ),
        ).reindex(EMOTION_ORDER, fill_value=0)

        heatmap = go.Figure(
            data=go.Heatmap(
                z=cross.values,
                x=cross.columns.astype(str),
                y=cross.index,
                text=cross.values,
                texttemplate="%{text}",
                hovertemplate="Cảm xúc: %{y}<br>Rating: %{x}<br>Số review: %{z}<extra></extra>",
            )
        )
        heatmap.update_layout(
            title="Ma trận cảm xúc × nhóm rating",
            height=420,
            margin=dict(l=10, r=10, t=60, b=20),
        )
        st.plotly_chart(heatmap, use_container_width=True)

    with tab3:
        display_df = results_df.copy()
        display_df["timestamp_utc"] = pd.to_datetime(
            display_df["timestamp_utc"],
            errors="coerce",
        )
        display_df["AI confidence"] = display_df["ai_score"].map(
            lambda x: f"{x:.1%}"
        )
        display_df["Rating"] = display_df["amazon_rating"].map(
            lambda x: f"{x:.1f}/5"
        )

        st.dataframe(
            display_df[
                [
                    "timestamp_utc",
                    "Rating",
                    "emotion",
                    "AI confidence",
                    "title",
                ]
            ].rename(
                columns={
                    "timestamp_utc": "Timestamp UTC",
                    "emotion": "Cảm xúc",
                    "title": "Review",
                }
            ),
            use_container_width=True,
            hide_index=True,
            height=520,
        )

        csv_bytes = results_df.to_csv(index=False).encode("utf-8-sig")
        st.download_button(
            "⬇️ Tải kết quả CSV",
            data=csv_bytes,
            file_name=f"amazon_fashion_stream_{run_summary['run_id']}.csv",
            mime="text/csv",
            use_container_width=True,
        )

    if run_summary:
        with st.expander("🔧 Thông tin phiên chạy", expanded=False):
            st.json(run_summary)

else:
    st.markdown("---")
    st.markdown(
        """
        <div class="info-panel">
            <div class="section-kicker">READY TO DEMO</div>
            <div class="section-title">Chưa có dữ liệu phiên chạy</div>
            <div class="section-desc" style="margin-bottom:0">
                Chọn thông số ở sidebar và bấm <b>CHẠY STREAMING DEMO</b>.
                Nếu chưa cấu hình OCI Secrets, ứng dụng vẫn minh họa toàn bộ pipeline
                AI + producer/consumer bằng Local Demo Mode.
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

# ============================================================
# 11. DEPLOYMENT HELP
# ============================================================

with st.expander("🚀 Hướng dẫn cấu hình Streamlit Cloud"):
    st.markdown(
        """
        **Repository nên có:**
        ```text
        streamlit_app.py
        requirements.txt
        .gitignore
        ```

        **Trong Streamlit Cloud → App settings → Secrets, nhập:**
        ```toml
        BOOTSTRAP_SERVERS = "cell-1.streaming.sa-saopaulo-1.oci.oraclecloud.com:9092"
        TOPIC = "DemoStreamingFashion"
        OCI_SASL_USERNAME = "YOUR_USERNAME"
        OCI_AUTH_TOKEN = "YOUR_AUTH_TOKEN"
        ```

        Không đưa `OCI_AUTH_TOKEN` vào GitHub.
        """
    )

st.markdown(
    """
    <div class="footer">
        Big Data Streaming Demo · Amazon Fashion · RoBERTa · OCI Streaming / Kafka · Streamlit
    </div>
    """,
    unsafe_allow_html=True,
)
