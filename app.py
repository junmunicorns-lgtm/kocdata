import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
import re
import glob
from datetime import datetime, timedelta
from pathlib import Path

# ============================================================
# 页面配置
# ============================================================
st.set_page_config(
    page_title="抖音作者数据看板",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

DATA_DIR = Path("data")

# ============================================================
# 衍生指标定义（名称 + 公式说明）
# ============================================================
DERIVED_METRICS_INFO = {
    "直播有效率(%)": "开播有效时长 ÷ 开播总时长 × 100",
    "feed效率(%)": "feed看播时长 ÷ 开播有效时长 × 100",
    "直播涨粉率(人/小时)": "涨粉 ÷ 开播有效时长(小时)",
    "单条播放量": "视频播放量 ÷ 作品数",
    "互动率(%)": "(视频点赞量+视频评论量) ÷ 视频播放量 × 100",
    "评论深度比": "视频评论量 ÷ 视频点赞量 (评论>50才计算, >1=深度讨论)",
    "涨粉效率(%)": "涨粉 ÷ 粉丝数 × 100",
    "涨粉/播放转化(%)": "涨粉 ÷ 视频播放量 × 100",
    "每小时feed看播(分钟)": "feed看播时长(分钟) ÷ 开播有效时长(小时)",
    "每粉丝feed贡献(分钟)": "feed看播时长(分钟) ÷ 粉丝数",
}

# ============================================================
# 工具函数
# ============================================================

def parse_duration_to_minutes(text):
    """将 '5小时59分钟53秒' 格式转为总分钟数"""
    if pd.isna(text) or str(text).strip() in ('', '-', '0'):
        return 0.0
    text = str(text)
    hours, minutes, seconds = 0, 0, 0
    h = re.search(r'(\d+)小时', text)
    m = re.search(r'(\d+)分钟', text)
    s = re.search(r'(\d+)秒', text)
    if h: hours = int(h.group(1))
    if m: minutes = int(m.group(1))
    if s: seconds = int(s.group(1))
    return hours * 60 + minutes + seconds / 60


def safe_div(a, b):
    """安全除法，分母为0返回0"""
    result = a / b
    result = result.where(b != 0, 0)
    result = result.fillna(0)
    return result


def safe_numeric(series):
    return pd.to_numeric(series, errors='coerce').fillna(0)


# ============================================================
# 数据加载
# ============================================================
@st.cache_data
def load_all_data():
    all_files = []
    for ext in ['*.xlsx', '*.csv']:
        all_files.extend(glob.glob(str(DATA_DIR / ext)))

    if not all_files:
        return pd.DataFrame()

    dfs = []
    for fp in sorted(all_files):
        fname = os.path.basename(fp)
        date_match = re.search(r'(\d{4})[-_]?(\d{2})[-_]?(\d{2})', fname)
        if date_match:
            file_date = f"{date_match.group(1)}-{date_match.group(2)}-{date_match.group(3)}"
        else:
            mtime = os.path.getmtime(fp)
            file_date = datetime.fromtimestamp(mtime).strftime('%Y-%m-%d')

        try:
            if fp.endswith('.xlsx'):
                df = pd.read_excel(fp, dtype=str)
            else:
                df = pd.read_csv(fp, dtype=str, encoding='utf-8-sig')
        except Exception as e:
            st.warning(f"读取文件失败: {fname} - {e}")
            continue

        df['数据日期'] = file_date
        dfs.append(df)

    if not dfs:
        return pd.DataFrame()

    combined = pd.concat(dfs, ignore_index=True)
    combined['数据日期'] = pd.to_datetime(combined['数据日期'], errors='coerce')
    return combined


# ============================================================
# 数据预处理 —— 匹配你的实际列名
# ============================================================
def preprocess(df):
    df = df.copy()

    # ---- 数值列转换（匹配你的真实列名） ----
    num_cols = [
        '粉丝数', '涨粉', '人气峰值',
        '视频播放量', '视频点赞量', '视频评论量',
        '作品数', 'feed_acu', '视频个播有效天',
        '分成比-直播音浪收益',
    ]
    for col in num_cols:
        if col in df.columns:
            df[col] = safe_numeric(df[col])

    # ---- 时长列转换为分钟 ----
    duration_cols_map = {
        '开播总时长': '总时长_分钟',
        '开播有效时长': '有效时长_分钟',
        'feed看播时长': 'feed看播_分钟',
    }
    for raw_col, new_col in duration_cols_map.items():
        if raw_col in df.columns:
            df[new_col] = df[raw_col].apply(parse_duration_to_minutes)

    return df


# ============================================================
# 按时间范围聚合数据
# ============================================================
def aggregate_by_author(df, days=None):
    if df.empty:
        return pd.DataFrame(), ''

    # 时间筛选
    if days is not None and '数据日期' in df.columns:
        latest_date = df['数据日期'].max()
        start_date = latest_date - timedelta(days=days - 1)
        df = df[df['数据日期'] >= start_date].copy()

    if df.empty:
        return pd.DataFrame(), ''

    # 确定作者标识列
    author_col = None
    for candidate in ['主播昵称', '昵称', '作者', '主播名称']:
        if candidate in df.columns:
            author_col = candidate
            break
    if author_col is None:
        st.error("❌ 找不到作者/主播昵称列")
        st.stop()

    # ---------- 构建聚合规则 ----------
    agg_dict = {}

    # 求和列（累计量 + 计算用时长）
    sum_cols = [
        '涨粉', '视频播放量', '视频点赞量', '视频评论量', '作品数',
        '总时长_分钟', '有效时长_分钟', 'feed看播_分钟',
        '视频个播有效天',
    ]
    for col in sum_cols:
        if col in df.columns:
            agg_dict[col] = 'sum'

    # 取最后一天的值（存量）
    last_cols = ['粉丝数']
    for col in last_cols:
        if col in df.columns:
            agg_dict[col] = 'last'

    # 取最大值
    max_cols = ['人气峰值', 'feed_acu']
    for col in max_cols:
        if col in df.columns:
            agg_dict[col] = 'max'

    # 文本类 —— 取第一个非空值（保留信息用）
    text_cols = [
        '主播ID', '抖音号', '抖音号（原）', '火山号', '火山号（原）',
        '上次开播时间', '签约类型', '运营经纪人', '招募经纪人',
        '首播时间', '入会时间', '西瓜号', '备注',
        '分成比-直播音浪收益', '主播标签',
        '开播总时长', '开播有效时长', 'feed看播时长',  # 保留原始文本
    ]
    for col in text_cols:
        if col in df.columns:
            agg_dict[col] = 'first'

    # 日期计数
    if '数据日期' in df.columns:
        agg_dict['数据日期'] = 'nunique'

    if not agg_dict:
        return df, author_col

    # 按日期排序确保 last 取到最新值
    if '数据日期' in df.columns:
        df = df.sort_values('数据日期')

    grouped = df.groupby(author_col, as_index=False).agg(agg_dict)

    if '数据日期' in grouped.columns:
        grouped = grouped.rename(columns={'数据日期': '统计天数'})

    return grouped, author_col


# ============================================================
# 计算衍生指标 —— 用你的真实列名
# ============================================================
def calc_derived(df):
    df = df.copy()

    total_min = df.get('总时长_分钟', pd.Series(0, index=df.index))
    valid_min = df.get('有效时长_分钟', pd.Series(0, index=df.index))
    feed_min = df.get('feed看播_分钟', pd.Series(0, index=df.index))
    valid_hours = valid_min / 60

    fans = df.get('粉丝数', pd.Series(0, index=df.index))
    grow = df.get('涨粉', pd.Series(0, index=df.index))
    views = df.get('视频播放量', pd.Series(0, index=df.index))
    likes = df.get('视频点赞量', pd.Series(0, index=df.index))
    comments = df.get('视频评论量', pd.Series(0, index=df.index))
    works = df.get('作品数', pd.Series(0, index=df.index))

    # --- 直播维度 ---
    df['直播有效率(%)'] = (safe_div(valid_min, total_min) * 100).round(2)
    df['feed效率(%)'] = (safe_div(feed_min, valid_min) * 100).round(2)
    df['直播涨粉率(人/小时)'] = safe_div(grow, valid_hours).round(2)

    # --- 内容维度 ---
    df['单条播放量'] = safe_div(views, works).round(0)
    df['互动率(%)'] = (safe_div(likes + comments, views) * 100).round(2)

    # --- 评论深度比（核心修复） ---
    # 条件：评论量 > 50 且 点赞量 > 0 且 评论量 >= 0 才计算，否则为 0
    raw_depth = safe_div(comments, likes)
    mask_valid = (comments > 50) & (likes > 0) & (comments >= 0)
    df['评论深度比'] = raw_depth.where(mask_valid, 0).round(3)

    # --- 成长维度 ---
    df['涨粉效率(%)'] = (safe_div(grow, fans) * 100).round(4)
    df['涨粉/播放转化(%)'] = (safe_div(grow, views) * 100).round(4)

    # --- 综合效率 ---
    df['每小时feed看播(分钟)'] = safe_div(feed_min, valid_hours).round(2)
    df['每粉丝feed贡献(分钟)'] = safe_div(feed_min, fans).round(4)

    # NaN填0
    for col in DERIVED_METRICS_INFO.keys():
        if col in df.columns:
            df[col] = df[col].fillna(0)

    return df


# ============================================================
# 获取作者标识列名
# ============================================================
def get_author_col(df):
    for candidate in ['主播昵称', '昵称', '作者', '主播名称']:
        if candidate in df.columns:
            return candidate
    return None


# ============================================================
# 主程序
# ============================================================
def main():
    # ---------- 侧边栏 ----------
    st.sidebar.title("📊 抖音作者数据看板")
    page = st.sidebar.radio(
        "选择页面",
        ["🏠 总览仪表盘", "🏆 作者排行榜", "📈 作者走势", "📋 明细数据"]
    )

    raw_df = load_all_data()
    if raw_df.empty:
        st.error("❌ data/ 文件夹中没有数据文件！请放入 .xlsx 或 .csv 文件。")
        st.stop()

    df = preprocess(raw_df)

    # 日期范围
    if '数据日期' in df.columns:
        min_date = df['数据日期'].min()
        max_date = df['数据日期'].max()
        total_days = (max_date - min_date).days + 1
        st.sidebar.markdown(f"📅 数据: **{min_date.strftime('%Y-%m-%d')}** ~ **{max_date.strftime('%Y-%m-%d')}**")
        st.sidebar.markdown(f"共 **{total_days}** 天，**{df['数据日期'].nunique()}** 个文件")

    # ============================================================
    # 🏠 总览仪表盘
    # ============================================================
    if page == "🏠 总览仪表盘":
        st.title("🏠 总览仪表盘")

        # --- 时间筛选 ---
        col_t1, col_t2 = st.columns([1, 3])
        with col_t1:
            time_range = st.selectbox(
                "⏱ 时间范围",
                ["近1天", "近7天", "近30天", "全部"],
                index=1,
                key="dash_time"
            )
        days_map = {"近1天": 1, "近7天": 7, "近30天": 30, "全部": None}
        selected_days = days_map[time_range]

        agg_df, author_col = aggregate_by_author(df, days=selected_days)
        if agg_df.empty:
            st.warning("所选时间范围内没有数据")
            st.stop()
        agg_df = calc_derived(agg_df)

        # --- 可选指标列表 ---
        base_metrics = ['粉丝数', '涨粉', '人气峰值', '视频播放量',
                        '视频点赞量', '视频评论量', '作品数', 'feed_acu']
        base_metrics = [m for m in base_metrics if m in agg_df.columns]
        derived_metrics = [m for m in DERIVED_METRICS_INFO.keys() if m in agg_df.columns]
        all_metrics = base_metrics + derived_metrics

        st.markdown("---")
        selected_top_metrics = st.multiselect(
            "🎯 选择要展示的TOP指标卡片",
            all_metrics,
            default=[m for m in ['涨粉', '人气峰值', '视频播放量', 'feed_acu'] if m in all_metrics][:4],
            key="top_metrics"
        )

        # --- TOP指标卡片 ---
        if selected_top_metrics:
            n_cols = min(len(selected_top_metrics), 4)
            rows_needed = (len(selected_top_metrics) + n_cols - 1) // n_cols
            idx = 0
            for row in range(rows_needed):
                cols = st.columns(n_cols)
                for c in range(n_cols):
                    if idx >= len(selected_top_metrics):
                        break
                    metric = selected_top_metrics[idx]
                    with cols[c]:
                        if metric in agg_df.columns and not agg_df.empty:
                            top_idx = agg_df[metric].idxmax()
                            top_author = agg_df.loc[top_idx, author_col]
                            top_value = agg_df.loc[top_idx, metric]

                            if isinstance(top_value, float):
                                if abs(top_value) >= 10000:
                                    display_val = f"{top_value:,.0f}"
                                elif abs(top_value) >= 1:
                                    display_val = f"{top_value:.2f}"
                                else:
                                    display_val = f"{top_value:.4f}"
                            else:
                                display_val = f"{top_value:,}"

                            st.metric(
                                label=f"🏆 {metric}",
                                value=display_val,
                                delta=f"👤 {top_author}"
                            )
                            formula = DERIVED_METRICS_INFO.get(metric, "")
                            if formula:
                                st.caption(f"📐 {formula}")
                    idx += 1

        st.markdown("---")

        # --- 全员概览图表 ---
        st.subheader("📊 全员数据概览")
        chart_metric = st.selectbox("选择图表指标", all_metrics, index=0, key="chart_m")

        if chart_metric in agg_df.columns:
            chart_df = agg_df.sort_values(chart_metric, ascending=False).head(20)
            fig = px.bar(
                chart_df, x=author_col, y=chart_metric,
                title=f"TOP 20 - {chart_metric} ({time_range})",
                text=chart_metric, color=chart_metric,
                color_continuous_scale="Blues"
            )
            fig.update_traces(texttemplate='%{text:.2s}', textposition='outside')
            fig.update_layout(xaxis_tickangle=-45, height=500)
            st.plotly_chart(fig, use_container_width=True)

        # --- 汇总表格 ---
        st.subheader("📈 汇总统计")
        summary_cols = [author_col]
        if '统计天数' in agg_df.columns:
            summary_cols.append('统计天数')
        summary_cols += base_metrics
        summary_cols = [c for c in summary_cols if c in agg_df.columns]
        st.dataframe(
            agg_df[summary_cols].sort_values(
                base_metrics[0] if base_metrics else author_col, ascending=False
            ),
            use_container_width=True, height=400
        )

    # ============================================================
    # 🏆 作者排行榜
    # ============================================================
    elif page == "🏆 作者排行榜":
        st.title("🏆 作者排行榜")

        col1, col2, col3 = st.columns([1, 1, 2])
        with col1:
            time_range = st.selectbox(
                "⏱ 时间范围",
                ["近1天", "近7天", "近30天", "全部"],
                index=0, key="rank_time"
            )
        days_map = {"近1天": 1, "近7天": 7, "近30天": 30, "全部": None}
        selected_days = days_map[time_range]

        agg_df, author_col = aggregate_by_author(df, days=selected_days)
        if agg_df.empty:
            st.warning("所选时间范围内没有数据")
            st.stop()
        agg_df = calc_derived(agg_df)

        # --- 所有可排序指标 ---
        base_metrics = ['粉丝数', '涨粉', '人气峰值', '视频播放量',
                        '视频点赞量', '视频评论量', '作品数', 'feed_acu']
        base_metrics = [m for m in base_metrics if m in agg_df.columns]

        duration_cols = ['总时长_分钟', '有效时长_分钟', 'feed看播_分钟']
        duration_cols = [m for m in duration_cols if m in agg_df.columns]

        derived_metrics = [m for m in DERIVED_METRICS_INFO.keys() if m in agg_df.columns]
        all_sortable = base_metrics + duration_cols + derived_metrics

        with col2:
            sort_by = st.selectbox("📊 排序指标", all_sortable, index=0, key="rank_sort")

        rank_df = agg_df.sort_values(sort_by, ascending=False).reset_index(drop=True)
        rank_df.index = rank_df.index + 1
        rank_df.index.name = "排名"

        # --- 公式说明 ---
        st.markdown("### 📐 衍生指标公式说明")
        formula_lines = []
        for name, formula in DERIVED_METRICS_INFO.items():
            if name in rank_df.columns:
                formula_lines.append(f"- **{name}** = `{formula}`")
        st.markdown("\n".join(formula_lines))
        st.markdown("---")

        # --- 构建可选列 ---
        show_cols = [author_col]
        if '统计天数' in rank_df.columns:
            show_cols.append('统计天数')

        # 保留所有原始文本列
        text_info_cols = ['主播ID', '签约类型', '运营经纪人', '招募经纪人',
                          '备注', '主播标签', '上次开播时间',
                          '开播总时长', '开播有效时长', 'feed看播时长']
        text_info_cols = [c for c in text_info_cols if c in rank_df.columns]

        show_cols += base_metrics + duration_cols + text_info_cols + derived_metrics
        # 去重保持顺序
        seen = set()
        show_cols_unique = []
        for c in show_cols:
            if c not in seen:
                show_cols_unique.append(c)
                seen.add(c)
        show_cols = show_cols_unique

        # 默认显示
        default_show = [author_col]
        for c in ['粉丝数', '涨粉', '人气峰值', '视频播放量', 'feed_acu',
                   '直播有效率(%)', 'feed效率(%)', '评论深度比']:
            if c in show_cols:
                default_show.append(c)

        selected_show = st.multiselect(
            "选择要显示的列",
            show_cols,
            default=default_show,
            key="rank_cols"
        )

        if selected_show:
            display_df = rank_df[selected_show].copy()

            # 衍生指标列名加公式
            rename_map = {}
            for col in selected_show:
                if col in DERIVED_METRICS_INFO:
                    rename_map[col] = f"{col}\n({DERIVED_METRICS_INFO[col]})"

            display_renamed = display_df.rename(columns=rename_map)
            st.dataframe(display_renamed, use_container_width=True, height=600)

            csv_data = display_df.to_csv(index=True, encoding='utf-8-sig')
            st.download_button(
                "📥 导出排行榜CSV",
                csv_data,
                file_name=f"排行榜_{time_range}_{sort_by}.csv",
                mime="text/csv"
            )
        else:
            st.info("请至少选择一列")

    # ============================================================
    # 📈 作者走势（新增页面）
    # ============================================================
    elif page == "📈 作者走势":
        st.title("📈 单个作者走势分析")

        author_col = get_author_col(df)
        if author_col is None:
            st.error("❌ 找不到作者/主播昵称列")
            st.stop()

        # ---------- 控制面板 ----------
        ctrl_col1, ctrl_col2 = st.columns([1, 1])

        with ctrl_col1:
            # 作者列表（按昵称排序）
            all_authors = sorted(df[author_col].dropna().unique().tolist())
            if not all_authors:
                st.warning("没有找到任何作者数据")
                st.stop()
            selected_author = st.selectbox(
                "👤 选择作者",
                all_authors,
                index=0,
                key="trend_author"
            )

        with ctrl_col2:
            trend_time = st.radio(
                "⏱ 时间范围",
                ["近7天", "近30天", "全部"],
                index=0,
                horizontal=True,
                key="trend_time"
            )

        # ---------- 筛选该作者数据 ----------
        author_df = df[df[author_col] == selected_author].copy()

        if '数据日期' not in author_df.columns or author_df.empty:
            st.warning(f"作者「{selected_author}」没有数据")
            st.stop()

        # 按时间范围过滤
        latest_date = author_df['数据日期'].max()
        trend_days_map = {"近7天": 7, "近30天": 30, "全部": None}
        trend_days = trend_days_map[trend_time]

        if trend_days is not None:
            start_date = latest_date - timedelta(days=trend_days - 1)
            author_df = author_df[author_df['数据日期'] >= start_date]

        if author_df.empty:
            st.warning(f"所选时间范围内「{selected_author}」没有数据")
            st.stop()

        # 按日期排序
        author_df = author_df.sort_values('数据日期').reset_index(drop=True)

        # ---------- 计算衍生指标（逐日） ----------
        author_df = calc_derived(author_df)

        # ---------- 作者基本信息卡片 ----------
        st.markdown("---")
        info_cols_display = {
            '粉丝数': '👥 粉丝数',
            '签约类型': '📝 签约类型',
            '运营经纪人': '👔 运营经纪人',
            '招募经纪人': '🤝 招募经纪人',
            '主播标签': '🏷️ 主播标签',
            '主播ID': '🆔 主播ID',
        }
        info_items = []
        latest_row = author_df.iloc[-1]  # 取最新一天的信息
        for col, label in info_cols_display.items():
            if col in author_df.columns:
                val = latest_row[col]
                if pd.notna(val) and str(val).strip() not in ('', '0', '0.0'):
                    if col == '粉丝数':
                        try:
                            val = f"{int(float(val)):,}"
                        except (ValueError, TypeError):
                            pass
                    info_items.append(f"**{label}**: {val}")

        if info_items:
            st.markdown(" | ".join(info_items))

        st.markdown(
            f"📅 数据范围: **{author_df['数据日期'].min().strftime('%Y-%m-%d')}** "
            f"~ **{author_df['数据日期'].max().strftime('%Y-%m-%d')}** "
            f"（共 **{author_df['数据日期'].nunique()}** 天）"
        )
        st.markdown("---")

        # ---------- 可选指标 ----------
        # 原始数值指标
        base_trend_metrics = [
            '粉丝数', '涨粉', '人气峰值', '视频播放量',
            '视频点赞量', '视频评论量', '作品数', 'feed_acu',
            '总时长_分钟', '有效时长_分钟', 'feed看播_分钟',
            '视频个播有效天',
        ]
        base_trend_metrics = [m for m in base_trend_metrics if m in author_df.columns]

        # 衍生指标
        derived_trend_metrics = [m for m in DERIVED_METRICS_INFO.keys() if m in author_df.columns]

        all_trend_metrics = base_trend_metrics + derived_trend_metrics

        # 默认选中的指标
        default_trend = []
        for m in ['粉丝数', '涨粉', '视频播放量', 'feed_acu']:
            if m in all_trend_metrics:
                default_trend.append(m)

        selected_metrics = st.multiselect(
            "📊 选择要查看的指标（可多选，每个指标独立一张图）",
            all_trend_metrics,
            default=default_trend,
            key="trend_metrics"
        )

        if not selected_metrics:
            st.info("👆 请至少选择一个指标")
            st.stop()

        # ---------- 绘制走势图 ----------
        # 每个指标独立一张折线图，避免量纲差异导致看不清
        # 每行放2张图
        charts_per_row = 2
        for i in range(0, len(selected_metrics), charts_per_row):
            row_metrics = selected_metrics[i:i + charts_per_row]
            cols = st.columns(len(row_metrics))
            for j, metric in enumerate(row_metrics):
                with cols[j]:
                    plot_df = author_df[['数据日期', metric]].copy()
                    plot_df = plot_df.dropna(subset=[metric])

                    # 公式说明
                    formula = DERIVED_METRICS_INFO.get(metric, "")
                    subtitle = f"<br><sup>{formula}</sup>" if formula else ""

                    fig = go.Figure()
                    fig.add_trace(go.Scatter(
                        x=plot_df['数据日期'],
                        y=plot_df[metric],
                        mode='lines+markers',
                        name=metric,
                        line=dict(width=2.5),
                        marker=dict(size=6),
                        hovertemplate='%{x|%Y-%m-%d}<br>' + metric + ': %{y:.2f}<extra></extra>'
                    ))

                    fig.update_layout(
                        title=dict(
                            text=f"{metric}{subtitle}",
                            font=dict(size=14),
                        ),
                        xaxis_title="日期",
                        yaxis_title=metric,
                        height=350,
                        margin=dict(l=20, r=20, t=60, b=40),
                        hovermode='x unified',
                        xaxis=dict(
                            tickformat='%m-%d',
                            tickangle=-45,
                        ),
                    )

                    st.plotly_chart(fig, use_container_width=True)

        # ---------- 多指标叠加对比图（可选） ----------
        if len(selected_metrics) >= 2:
            st.markdown("---")
            with st.expander("📊 多指标叠加对比（归一化后）", expanded=False):
                st.caption("将所有选中指标归一化到 0~100 区间，便于对比趋势走向")

                norm_df = author_df[['数据日期'] + selected_metrics].copy()
                norm_df = norm_df.set_index('数据日期')

                # Min-Max 归一化
                for col in selected_metrics:
                    col_min = norm_df[col].min()
                    col_max = norm_df[col].max()
                    if col_max != col_min:
                        norm_df[col] = (norm_df[col] - col_min) / (col_max - col_min) * 100
                    else:
                        norm_df[col] = 50  # 常量值放中间

                norm_df = norm_df.reset_index()

                fig_norm = go.Figure()
                colors = px.colors.qualitative.Set2
                for k, metric in enumerate(selected_metrics):
                    fig_norm.add_trace(go.Scatter(
                        x=norm_df['数据日期'],
                        y=norm_df[metric],
                        mode='lines+markers',
                        name=metric,
                        line=dict(width=2, color=colors[k % len(colors)]),
                        marker=dict(size=5),
                    ))

                fig_norm.update_layout(
                    title="多指标归一化趋势对比",
                    xaxis_title="日期",
                    yaxis_title="归一化值 (0~100)",
                    height=450,
                    hovermode='x unified',
                    xaxis=dict(tickformat='%m-%d', tickangle=-45),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                )

                st.plotly_chart(fig_norm, use_container_width=True)

        # ---------- 明细数据表 ----------
        st.markdown("---")
        st.subheader(f"📋 {selected_author} 逐日明细")

        # 构建展示列
        detail_show_cols = ['数据日期'] + selected_metrics
        # 加上一些常用原始列
        extra_detail_cols = [
            '开播总时长', '开播有效时长', 'feed看播时长',
            '签约类型', '运营经纪人', '备注',
        ]
        for ec in extra_detail_cols:
            if ec in author_df.columns and ec not in detail_show_cols:
                detail_show_cols.append(ec)

        detail_display = author_df[detail_show_cols].copy()
        detail_display['数据日期'] = detail_display['数据日期'].dt.strftime('%Y-%m-%d')
        detail_display = detail_display.sort_values('数据日期', ascending=False)

        st.dataframe(detail_display, use_container_width=True, height=400)

        # 导出
        csv_data = detail_display.to_csv(index=False, encoding='utf-8-sig')
        st.download_button(
            "📥 导出该作者走势数据",
            csv_data,
            file_name=f"走势_{selected_author}_{trend_time}.csv",
            mime="text/csv",
            key="trend_export"
        )

    # ============================================================
    # 📋 明细数据
    # ============================================================
    elif page == "📋 明细数据":
        st.title("📋 明细数据（原始逐日记录）")

        time_range = st.selectbox(
            "⏱ 时间范围",
            ["近1天", "近7天", "近30天", "全部"],
            index=0, key="detail_time"
        )
        days_map = {"近1天": 1, "近7天": 7, "近30天": 30, "全部": None}
        selected_days = days_map[time_range]

        display_df = df.copy()
        if selected_days and '数据日期' in display_df.columns:
            latest = display_df['数据日期'].max()
            start = latest - timedelta(days=selected_days - 1)
            display_df = display_df[display_df['数据日期'] >= start]

        # 作者筛选
        author_col = get_author_col(display_df)

        if author_col:
            authors = ['全部'] + sorted(display_df[author_col].dropna().unique().tolist())
            selected_author = st.selectbox("👤 筛选作者", authors, key="detail_author")
            if selected_author != '全部':
                display_df = display_df[display_df[author_col] == selected_author]

        st.markdown(f"共 **{len(display_df)}** 条记录")
        st.dataframe(display_df, use_container_width=True, height=600)

        csv_data = display_df.to_csv(index=False, encoding='utf-8-sig')
        st.download_button(
            "📥 导出明细CSV",
            csv_data,
            file_name=f"明细_{time_range}.csv",
            mime="text/csv"
        )


if __name__ == "__main__":
    main()