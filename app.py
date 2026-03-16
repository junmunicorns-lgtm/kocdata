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
_local_tags = Path(r"D:\KOCdata\author_tags.csv")
if _local_tags.exists():
    TAGS_FILE = _local_tags
else:
    TAGS_FILE = Path(os.path.dirname(__file__)) / "author_tags.csv"

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
# 需要拆分多标签的字段列表
# ============================================================
MULTI_TAG_FIELDS = ['赛道', '标签', '归属', '地域', '特殊人设', '建联方式']

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


def extract_unique_tags(series):
    """
    从一个 Series 中提取所有去重的单标签。
    例如 Series 中有 '新服|其他|日常'，会拆成 ['新服', '其他', '日常']。
    返回排序后的列表。
    """
    all_tags = (
        series
        .dropna()
        .astype(str)
        .str.split(r'\|')
        .explode()
        .str.strip()
        .loc[lambda s: (s != '') & (s != 'nan')]
        .unique()
        .tolist()
    )
    all_tags.sort()
    return all_tags


def match_any_tag(cell, selected_tags):
    """
    检查单元格（可能含竖线分隔的多标签）是否包含任一选中标签。
    例如 cell='新服|其他|日常', selected_tags=['新服'] → True
    """
    if pd.isna(cell):
        return False
    cell_tags = [t.strip() for t in str(cell).split('|')]
    return any(t in cell_tags for t in selected_tags)


def explode_multi_tag_field(df, field):
    """
    将 DataFrame 中某一列按竖线拆分成多行。
    例如一行的 '标签' 是 '新服|其他|日常'，会变成3行，每行一个标签。
    用于分组聚合场景。
    """
    if field not in df.columns:
        return df
    result = df.copy()
    result[field] = result[field].astype(str).str.strip()
    result[field] = result[field].str.split(r'\|')
    result = result.explode(field, ignore_index=True)
    result[field] = result[field].str.strip()
    result = result[result[field].notna() & (result[field] != '') & (result[field] != 'nan')]
    return result


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
                # 自动尝试多种编码，避免编码错误提醒
                df = None
                for enc in ['utf-8-sig', 'utf-8', 'gbk', 'gb18030', 'latin1']:
                    try:
                        df = pd.read_csv(fp, dtype=str, encoding=enc)
                        break
                    except (UnicodeDecodeError, UnicodeError):
                        continue
                if df is None:
                    st.warning(f"读取文件失败（编码无法识别）: {fname}")
                    continue
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


@st.cache_data
def load_tags():
    """加载作者标签表，自动尝试多种编码"""
    if not TAGS_FILE.exists():
        return pd.DataFrame()

    for enc in ["gbk", "gb18030", "utf-8-sig", "utf-8", "latin1"]:
        try:
            tags = pd.read_csv(str(TAGS_FILE), dtype=str, encoding=enc)
            tags.columns = tags.columns.str.strip()
            if '抖音号' in tags.columns:
                tags['抖音号'] = tags['抖音号'].astype(str).str.strip()
            return tags
        except (UnicodeDecodeError, UnicodeError):
            pass

    st.warning("标签表编码无法识别，请手动转为UTF-8后重试")
    return pd.DataFrame()


def merge_tags(df, tags_df):
    """将标签表匹配到数据表上（通过抖音号）"""
    if tags_df.empty or '抖音号' not in df.columns or '抖音号' not in tags_df.columns:
        return df

    df = df.copy()
    df['抖音号'] = df['抖音号'].astype(str).str.strip()

    # ★ 修复1：标签表去重，防止 merge 导致数据行数翻倍
    tags_clean = tags_df.copy()
    tags_clean['抖音号'] = tags_clean['抖音号'].astype(str).str.strip()
    tags_clean = tags_clean.drop_duplicates(subset='抖音号', keep='first')

    tag_cols_to_merge = ['抖音号']
    for col in MULTI_TAG_FIELDS:
        if col in tags_clean.columns and col not in df.columns:
            tag_cols_to_merge.append(col)
        elif col in tags_clean.columns and col in df.columns:
            tags_clean = tags_clean.rename(columns={col: f'{col}_标签表'})
            tag_cols_to_merge.append(f'{col}_标签表')

    merged = df.merge(tags_clean[tag_cols_to_merge], on='抖音号', how='left')
    return merged


# ============================================================
# 数据预处理
# ============================================================
def preprocess(df):
    df = df.copy()

    num_cols = [
        '粉丝数', '涨粉', '人气峰值',
        '视频播放量', '视频点赞量', '视频评论量',
        '作品数', 'feed_acu', '视频个播有效天',
        '分成比-直播音浪收益',
    ]
    for col in num_cols:
        if col in df.columns:
            df[col] = safe_numeric(df[col])

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

    if days is not None and '数据日期' in df.columns:
        latest_date = df['数据日期'].max()
        start_date = latest_date - timedelta(days=days - 1)
        df = df[df['数据日期'] >= start_date].copy()

    if df.empty:
        return pd.DataFrame(), ''

    author_col = None
    for candidate in ['主播昵称', '昵称', '作者', '主播名称']:
        if candidate in df.columns:
            author_col = candidate
            break
    if author_col is None:
        st.error("❌ 找不到作者/主播昵称列")
        st.stop()

    agg_dict = {}

    sum_cols = [
        '涨粉', '视频播放量', '视频点赞量', '视频评论量', '作品数',
        '总时长_分钟', '有效时长_分钟', 'feed看播_分钟',
        '视频个播有效天',
    ]
    for col in sum_cols:
        if col in df.columns:
            agg_dict[col] = 'sum'

    # ★ 修复2：粉丝数改为 'max' 而不是 'last'，避免排序问题导致取到0
    last_cols = ['粉丝数']
    for col in last_cols:
        if col in df.columns:
            agg_dict[col] = 'max'

    max_cols = ['人气峰值', 'feed_acu']
    for col in max_cols:
        if col in df.columns:
            agg_dict[col] = 'max'

    text_cols = [
        '主播ID', '抖音号', '抖音号（原）', '火山号', '火山号（原）',
        '上次开播时间', '签约类型', '运营经纪人', '招募经纪人',
        '首播时间', '入会时间', '西瓜号', '备注',
        '分成比-直播音浪收益', '主播标签',
        '开播总时长', '开播有效时长', 'feed看播时长',
        '赛道', '归属', '地域', '标签', '特殊人设', '建联方式',
    ]
    for col in text_cols:
        if col in df.columns:
            agg_dict[col] = 'first'

    if '数据日期' in df.columns:
        agg_dict['数据日期'] = 'nunique'

    if not agg_dict:
        return df, author_col

    if '数据日期' in df.columns:
        df = df.sort_values('数据日期')

    grouped = df.groupby(author_col, as_index=False).agg(agg_dict)

    if '数据日期' in grouped.columns:
        grouped = grouped.rename(columns={'数据日期': '统计天数'})

    return grouped, author_col


# ============================================================
# 计算衍生指标
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

    df['直播有效率(%)'] = (safe_div(valid_min, total_min) * 100).round(2)
    df['feed效率(%)'] = (safe_div(feed_min, valid_min) * 100).round(2)
    df['直播涨粉率(人/小时)'] = safe_div(grow, valid_hours).round(2)

    df['单条播放量'] = safe_div(views, works).round(0)
    df['互动率(%)'] = (safe_div(likes + comments, views) * 100).round(2)

    raw_depth = safe_div(comments, likes)
    mask_valid = (comments > 50) & (likes > 0) & (comments >= 0)
    df['评论深度比'] = raw_depth.where(mask_valid, 0).round(3)

    df['涨粉效率(%)'] = (safe_div(grow, fans) * 100).round(4)
    df['涨粉/播放转化(%)'] = (safe_div(grow, views) * 100).round(4)

    df['每小时feed看播(分钟)'] = safe_div(feed_min, valid_hours).round(2)
    df['每粉丝feed贡献(分钟)'] = safe_div(feed_min, fans).round(4)

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
    st.sidebar.title("📊 抖音作者数据看板")
    page = st.sidebar.radio(
        "选择页面",
        ["🏠 总览仪表盘", "🏆 作者排行榜", "📈 作者走势", "🏷️ 赛道分析", "📋 明细数据"]
    )

    raw_df = load_all_data()
    if raw_df.empty:
        st.error("❌ data/ 文件夹中没有数据文件！请放入 .xlsx 或 .csv 文件。")
        st.stop()

    tags_df = load_tags()

    df = preprocess(raw_df)

    if not tags_df.empty:
        df = merge_tags(df, tags_df)

    if '数据日期' in df.columns:
        min_date = df['数据日期'].min()
        max_date = df['数据日期'].max()
        total_days = (max_date - min_date).days + 1
        st.sidebar.markdown(f"📅 数据: **{min_date.strftime('%Y-%m-%d')}** ~ **{max_date.strftime('%Y-%m-%d')}**")
        st.sidebar.markdown(f"共 **{total_days}** 天，**{df['数据日期'].nunique()}** 个文件")

    if not tags_df.empty:
        st.sidebar.success(f"🏷️ 标签表已加载: {len(tags_df)} 位作者")
    else:
        st.sidebar.warning("🏷️ 标签表未加载")

    # ============================================================
    # 🏠 总览仪表盘
    # ============================================================
    if page == "🏠 总览仪表盘":
        st.title("🏠 总览仪表盘")

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

        st.markdown("### 📐 衍生指标公式说明")
        formula_lines = []
        for name, formula in DERIVED_METRICS_INFO.items():
            if name in rank_df.columns:
                formula_lines.append(f"- **{name}** = `{formula}`")
        st.markdown("\n".join(formula_lines))
        st.markdown("---")

        show_cols = [author_col]
        if '统计天数' in rank_df.columns:
            show_cols.append('统计天数')

        text_info_cols = ['主播ID', '签约类型', '运营经纪人', '招募经纪人',
                          '备注', '主播标签', '上次开播时间',
                          '开播总时长', '开播有效时长', 'feed看播时长']
        text_info_cols = [c for c in text_info_cols if c in rank_df.columns]

        tag_info_cols = ['赛道', '归属', '地域', '标签', '特殊人设', '建联方式']
        tag_info_cols = [c for c in tag_info_cols if c in rank_df.columns]

        show_cols += tag_info_cols + base_metrics + duration_cols + text_info_cols + derived_metrics
        seen = set()
        show_cols_unique = []
        for c in show_cols:
            if c not in seen:
                show_cols_unique.append(c)
                seen.add(c)
        show_cols = show_cols_unique

        default_show = [author_col]
        for c in ['赛道', '标签', '粉丝数', '涨粉', '人气峰值', '视频播放量', 'feed_acu',
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
    # 📈 作者走势
    # ============================================================
    elif page == "📈 作者走势":
        st.title("📈 单个作者走势分析")

        author_col = get_author_col(df)
        if author_col is None:
            st.error("❌ 找不到作者/主播昵称列")
            st.stop()

        ctrl_col1, ctrl_col2 = st.columns([1, 1])

        with ctrl_col1:
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

        author_df = df[df[author_col] == selected_author].copy()

        if '数据日期' not in author_df.columns or author_df.empty:
            st.warning(f"作者「{selected_author}」没有数据")
            st.stop()

        latest_date = author_df['数据日期'].max()
        trend_days_map = {"近7天": 7, "近30天": 30, "全部": None}
        trend_days = trend_days_map[trend_time]

        if trend_days is not None:
            start_date = latest_date - timedelta(days=trend_days - 1)
            author_df = author_df[author_df['数据日期'] >= start_date]

        if author_df.empty:
            st.warning(f"所选时间范围内「{selected_author}」没有数据")
            st.stop()

        author_df = author_df.sort_values('数据日期').reset_index(drop=True)
        author_df = calc_derived(author_df)

        st.markdown("---")
        info_cols_display = {
            '粉丝数': '👥 粉丝数',
            '签约类型': '📝 签约类型',
            '运营经纪人': '👔 运营经纪人',
            '招募经纪人': '🤝 招募经纪人',
            '主播标签': '🏷️ 主播标签',
            '主播ID': '🆔 主播ID',
            '赛道': '🏁 赛道',
            '标签': '🔖 标签',
            '归属': '🏢 归属',
            '地域': '📍 地域',
            '特殊人设': '🎭 特殊人设',
        }
        info_items = []
        latest_row = author_df.iloc[-1]
        for col, label in info_cols_display.items():
            if col in author_df.columns:
                val = latest_row[col]
                if pd.notna(val) and str(val).strip() not in ('', '0', '0.0', 'nan'):
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

        base_trend_metrics = [
            '粉丝数', '涨粉', '人气峰值', '视频播放量',
            '视频点赞量', '视频评论量', '作品数', 'feed_acu',
            '总时长_分钟', '有效时长_分钟', 'feed看播_分钟',
            '视频个播有效天',
        ]
        base_trend_metrics = [m for m in base_trend_metrics if m in author_df.columns]
        derived_trend_metrics = [m for m in DERIVED_METRICS_INFO.keys() if m in author_df.columns]
        all_trend_metrics = base_trend_metrics + derived_trend_metrics

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

        charts_per_row = 2
        for i in range(0, len(selected_metrics), charts_per_row):
            row_metrics = selected_metrics[i:i + charts_per_row]
            cols = st.columns(len(row_metrics))
            for j, metric in enumerate(row_metrics):
                with cols[j]:
                    plot_df = author_df[['数据日期', metric]].copy()
                    plot_df = plot_df.dropna(subset=[metric])

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

        if len(selected_metrics) >= 2:
            st.markdown("---")
            with st.expander("📊 多指标叠加对比（归一化后）", expanded=False):
                st.caption("将所有选中指标归一化到 0~100 区间，便于对比趋势走向")

                norm_df = author_df[['数据日期'] + selected_metrics].copy()
                norm_df = norm_df.set_index('数据日期')

                for col in selected_metrics:
                    col_min = norm_df[col].min()
                    col_max = norm_df[col].max()
                    if col_max != col_min:
                        norm_df[col] = (norm_df[col] - col_min) / (col_max - col_min) * 100
                    else:
                        norm_df[col] = 50

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

        st.markdown("---")
        st.subheader(f"📋 {selected_author} 逐日明细")

        detail_show_cols = ['数据日期'] + selected_metrics
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

        csv_data = detail_display.to_csv(index=False, encoding='utf-8-sig')
        st.download_button(
            "📥 导出该作者走势数据",
            csv_data,
            file_name=f"走势_{selected_author}_{trend_time}.csv",
            mime="text/csv",
            key="trend_export"
        )

    # ============================================================
    # 🏷️ 赛道分析（已处理多标签竖线分隔）
    # ============================================================
    elif page == "🏷️ 赛道分析":
        st.title("🏷️ 赛道分析")

        if tags_df.empty:
            st.error("❌ 标签表未加载，请检查文件路径：" + str(TAGS_FILE))
            st.stop()

        if '赛道' not in df.columns:
            st.error("❌ 数据表中未匹配到「赛道」字段，请检查标签表中的抖音号是否与数据表一致")
            st.stop()

        author_col = get_author_col(df)
        if author_col is None:
            st.error("❌ 找不到作者/主播昵称列")
            st.stop()

        tab1, tab2 = st.tabs(["📊 赛道规模", "🏆 垂类TOP"])

        # --------------------------------------------------
        # Tab 1: 赛道规模
        # --------------------------------------------------
        with tab1:
            st.subheader("📊 赛道规模 — 按赛道/标签汇总播放数据")

            scale_col1, scale_col2, scale_col3 = st.columns([1, 1, 2])

            with scale_col1:
                scale_time = st.selectbox(
                    "⏱ 时间范围",
                    ["近1天", "近7天", "近30天", "全部"],
                    index=1,
                    key="scale_time"
                )
            days_map = {"近1天": 1, "近7天": 7, "近30天": 30, "全部": None}
            scale_days = days_map[scale_time]

            scale_df = df.copy()
            if scale_days is not None and '数据日期' in scale_df.columns:
                latest = scale_df['数据日期'].max()
                start = latest - timedelta(days=scale_days - 1)
                scale_df = scale_df[scale_df['数据日期'] >= start]

            if scale_df.empty:
                st.warning("所选时间范围内没有数据")
                st.stop()

            with scale_col2:
                group_by_field = st.selectbox(
                    "📂 分组维度",
                    ["赛道", "标签", "归属", "地域"],
                    index=0,
                    key="scale_group"
                )

            if group_by_field not in scale_df.columns:
                st.warning(f"数据中没有「{group_by_field}」字段")
                st.stop()

            # ★★★ 关键改动：拆分多标签后再分组 ★★★
            scale_exploded = explode_multi_tag_field(scale_df, group_by_field)

            if scale_exploded.empty:
                st.warning(f"没有作者匹配到「{group_by_field}」信息")
                st.stop()

            # 获取所有去重的单标签用于筛选下拉框
            all_groups = sorted(scale_exploded[group_by_field].dropna().unique().tolist())

            with scale_col3:
                selected_groups = st.multiselect(
                    f"🔍 筛选特定{group_by_field}（留空=全部）",
                    all_groups,
                    default=[],
                    key="scale_filter"
                )

            if selected_groups:
                scale_exploded = scale_exploded[scale_exploded[group_by_field].isin(selected_groups)]

            # --- 按分组维度聚合 ---
            agg_metrics = {
                '视频播放量': 'sum',
                '视频点赞量': 'sum',
                '视频评论量': 'sum',
                '涨粉': 'sum',
                '作品数': 'sum',
            }
            agg_metrics = {k: v for k, v in agg_metrics.items() if k in scale_exploded.columns}

            group_agg = scale_exploded.groupby(group_by_field).agg(
                **{k: (k, v) for k, v in agg_metrics.items()},
                作者数=(author_col, 'nunique'),
            ).reset_index()

            if '视频播放量' in group_agg.columns and '作品数' in group_agg.columns:
                group_agg['平均单条播放'] = safe_div(
                    group_agg['视频播放量'], group_agg['作品数']
                ).round(0)

            if '视频点赞量' in group_agg.columns and '视频播放量' in group_agg.columns:
                group_agg['互动率(%)'] = (safe_div(
                    group_agg['视频点赞量'] + group_agg.get('视频评论量', 0),
                    group_agg['视频播放量']
                ) * 100).round(2)

            sort_col = '视频播放量' if '视频播放量' in group_agg.columns else '作者数'
            group_agg = group_agg.sort_values(sort_col, ascending=False).reset_index(drop=True)

            st.markdown("---")

            total_views = group_agg['视频播放量'].sum() if '视频播放量' in group_agg.columns else 0
            total_authors = group_agg['作者数'].sum()
            total_works = group_agg['作品数'].sum() if '作品数' in group_agg.columns else 0
            total_fans_grow = group_agg['涨粉'].sum() if '涨粉' in group_agg.columns else 0

            kpi1, kpi2, kpi3, kpi4 = st.columns(4)
            with kpi1:
                st.metric("📺 总播放量", f"{total_views:,.0f}")
            with kpi2:
                st.metric("👥 覆盖作者数", f"{total_authors:,}")
            with kpi3:
                st.metric("📝 总作品数", f"{total_works:,.0f}")
            with kpi4:
                st.metric("📈 总涨粉", f"{total_fans_grow:,.0f}")

            st.markdown("---")

            chart_col1, chart_col2 = st.columns(2)

            with chart_col1:
                if '视频播放量' in group_agg.columns:
                    fig_bar = px.bar(
                        group_agg.head(20),
                        x=group_by_field,
                        y='视频播放量',
                        title=f"各{group_by_field}播放量 ({scale_time})",
                        text='视频播放量',
                        color='视频播放量',
                        color_continuous_scale='Blues',
                    )
                    fig_bar.update_traces(texttemplate='%{text:.2s}', textposition='outside')
                    fig_bar.update_layout(xaxis_tickangle=-45, height=450)
                    st.plotly_chart(fig_bar, use_container_width=True)

            with chart_col2:
                fig_pie = px.pie(
                    group_agg.head(15),
                    names=group_by_field,
                    values='视频播放量' if '视频播放量' in group_agg.columns else '作者数',
                    title=f"各{group_by_field}播放占比 ({scale_time})",
                    hole=0.3,
                )
                fig_pie.update_layout(height=450)
                st.plotly_chart(fig_pie, use_container_width=True)

            # --- 日期趋势图（按天+分组，也需要拆分标签） ---
            st.markdown("---")
            st.subheader(f"📈 {group_by_field}每日播放趋势")

            if '数据日期' in scale_exploded.columns and '视频播放量' in scale_exploded.columns:
                if selected_groups:
                    trend_groups = selected_groups
                else:
                    top5 = group_agg.head(5)[group_by_field].tolist()
                    trend_groups = top5

                trend_select = st.multiselect(
                    f"选择要查看趋势的{group_by_field}",
                    all_groups if not selected_groups else selected_groups,
                    default=trend_groups[:5],
                    key="scale_trend_select"
                )

                if trend_select:
                    trend_data = scale_exploded[scale_exploded[group_by_field].isin(trend_select)]
                    daily_group = trend_data.groupby(
                        ['数据日期', group_by_field]
                    )['视频播放量'].sum().reset_index()

                    fig_trend = px.line(
                        daily_group,
                        x='数据日期',
                        y='视频播放量',
                        color=group_by_field,
                        title=f"各{group_by_field}每日播放量趋势",
                        markers=True,
                    )
                    fig_trend.update_layout(
                        height=450,
                        xaxis=dict(tickformat='%m-%d', tickangle=-45),
                        hovermode='x unified',
                    )
                    st.plotly_chart(fig_trend, use_container_width=True)

            st.markdown("---")
            st.subheader(f"📋 {group_by_field}汇总表")
            st.dataframe(group_agg, use_container_width=True, height=400)

            csv_data = group_agg.to_csv(index=False, encoding='utf-8-sig')
            st.download_button(
                f"📥 导出{group_by_field}汇总CSV",
                csv_data,
                file_name=f"赛道规模_{group_by_field}_{scale_time}.csv",
                mime="text/csv",
                key="scale_export"
            )

        # --------------------------------------------------
        # Tab 2: 垂类TOP（已处理多标签竖线分隔）
        # --------------------------------------------------
        with tab2:
            st.subheader("🏆 垂类TOP — 按赛道/标签筛选作者并排序")

            filter_col1, filter_col2, filter_col3 = st.columns([1, 1, 1])

            with filter_col1:
                top_time = st.selectbox(
                    "⏱ 时间范围",
                    ["近1天", "近7天", "近30天", "全部"],
                    index=1,
                    key="top_time"
                )
                top_days = days_map[top_time]

            top_agg_df, top_author_col = aggregate_by_author(df, days=top_days)
            if top_agg_df.empty:
                st.warning("所选时间范围内没有数据")
                st.stop()
            top_agg_df = calc_derived(top_agg_df)

            with filter_col2:
                # ★★★ 关键改动：拆分多标签后获取去重的赛道列表 ★★★
                if '赛道' in top_agg_df.columns:
                    all_tracks = extract_unique_tags(top_agg_df['赛道'])
                    selected_track = st.multiselect(
                        "🏁 筛选赛道",
                        all_tracks,
                        default=[],
                        key="top_track"
                    )
                else:
                    selected_track = []

            with filter_col3:
                # ★★★ 关键改动：拆分多标签后获取去重的标签列表 ★★★
                if '标签' in top_agg_df.columns:
                    all_labels = extract_unique_tags(top_agg_df['标签'])
                    selected_label = st.multiselect(
                        "🔖 筛选标签",
                        all_labels,
                        default=[],
                        key="top_label"
                    )
                else:
                    selected_label = []

            # ★★★ 关键改动：用 match_any_tag 做模糊匹配筛选 ★★★
            filtered_df = top_agg_df.copy()
            if selected_track:
                filtered_df = filtered_df[
                    filtered_df['赛道'].apply(lambda x: match_any_tag(x, selected_track))
                ]
            if selected_label:
                filtered_df = filtered_df[
                    filtered_df['标签'].apply(lambda x: match_any_tag(x, selected_label))
                ]

            with st.expander("🔧 更多筛选条件", expanded=False):
                extra_col1, extra_col2, extra_col3 = st.columns(3)

                with extra_col1:
                    if '归属' in filtered_df.columns:
                        all_belong = extract_unique_tags(filtered_df['归属'])
                        selected_belong = st.multiselect("🏢 归属", all_belong, default=[], key="top_belong")
                        if selected_belong:
                            filtered_df = filtered_df[
                                filtered_df['归属'].apply(lambda x: match_any_tag(x, selected_belong))
                            ]

                with extra_col2:
                    if '地域' in filtered_df.columns:
                        all_regions = extract_unique_tags(filtered_df['地域'])
                        selected_region = st.multiselect("📍 地域", all_regions, default=[], key="top_region")
                        if selected_region:
                            filtered_df = filtered_df[
                                filtered_df['地域'].apply(lambda x: match_any_tag(x, selected_region))
                            ]

                with extra_col3:
                    if '特殊人设' in filtered_df.columns:
                        all_persona = extract_unique_tags(filtered_df['特殊人设'])
                        selected_persona = st.multiselect("🎭 特殊人设", all_persona, default=[], key="top_persona")
                        if selected_persona:
                            filtered_df = filtered_df[
                                filtered_df['特殊人设'].apply(lambda x: match_any_tag(x, selected_persona))
                            ]

            if filtered_df.empty:
                st.warning("当前筛选条件下没有作者数据")
                st.stop()

            st.markdown("---")

            filter_desc_parts = []
            if selected_track:
                filter_desc_parts.append(f"赛道: {', '.join(selected_track)}")
            if selected_label:
                filter_desc_parts.append(f"标签: {', '.join(selected_label)}")
            filter_desc = " | ".join(filter_desc_parts) if filter_desc_parts else "全部作者"

            st.markdown(f"🔍 **当前筛选**: {filter_desc} — 共 **{len(filtered_df)}** 位作者")

            sort_col1, sort_col2 = st.columns([1, 2])

            base_metrics = ['粉丝数', '涨粉', '人气峰值', '视频播放量',
                            '视频点赞量', '视频评论量', '作品数', 'feed_acu']
            base_metrics = [m for m in base_metrics if m in filtered_df.columns]

            duration_cols = ['总时长_分钟', '有效时长_分钟', 'feed看播_分钟']
            duration_cols = [m for m in duration_cols if m in filtered_df.columns]

            derived_metrics = [m for m in DERIVED_METRICS_INFO.keys() if m in filtered_df.columns]
            all_sortable = base_metrics + duration_cols + derived_metrics

            with sort_col1:
                top_sort_by = st.selectbox(
                    "📊 排序指标",
                    all_sortable,
                    index=all_sortable.index('视频播放量') if '视频播放量' in all_sortable else 0,
                    key="top_sort"
                )

            with sort_col2:
                top_n = st.slider(
                    "🔢 显示前N名",
                    min_value=5,
                    max_value=min(100, len(filtered_df)),
                    value=min(20, len(filtered_df)),
                    step=5,
                    key="top_n"
                )

            sorted_df = filtered_df.sort_values(top_sort_by, ascending=False).head(top_n)
            sorted_df = sorted_df.reset_index(drop=True)
            sorted_df.index = sorted_df.index + 1
            sorted_df.index.name = "排名"

            # 处理柱状图的赛道颜色：多标签时取第一个赛道作为颜色分组
            chart_top_df = sorted_df.head(20).copy()
            if '赛道' in chart_top_df.columns:
                chart_top_df['赛道_主'] = chart_top_df['赛道'].astype(str).str.split(r'\|').str[0].str.strip()
                color_field = '赛道_主'
            else:
                color_field = None

            fig_top = px.bar(
                chart_top_df,
                x=top_author_col,
                y=top_sort_by,
                title=f"垂类TOP — {filter_desc} — 按{top_sort_by}排序 ({top_time})",
                text=top_sort_by,
                color=color_field,
                barmode='group',
            )
            fig_top.update_traces(texttemplate='%{text:.2s}', textposition='outside')
            fig_top.update_layout(xaxis_tickangle=-45, height=500)
            st.plotly_chart(fig_top, use_container_width=True)

            st.markdown("---")

            st.subheader("📋 垂类排行表")

            table_cols = [top_author_col]
            tag_cols = ['赛道', '标签', '归属', '地域', '特殊人设', '建联方式']
            tag_cols = [c for c in tag_cols if c in sorted_df.columns]
            table_cols += tag_cols

            if '统计天数' in sorted_df.columns:
                table_cols.append('统计天数')

            table_cols += base_metrics + duration_cols + derived_metrics

            seen = set()
            table_cols_unique = []
            for c in table_cols:
                if c not in seen:
                    table_cols_unique.append(c)
                    seen.add(c)
            table_cols = table_cols_unique

            default_table = [top_author_col]
            for c in ['赛道', '标签', '粉丝数', '涨粉', '视频播放量',
                       '单条播放量', '互动率(%)', 'feed_acu', '直播涨粉率(人/小时)']:
                if c in table_cols:
                    default_table.append(c)

            selected_table_cols = st.multiselect(
                "选择要显示的列",
                table_cols,
                default=default_table,
                key="top_table_cols"
            )

            if selected_table_cols:
                display_top = sorted_df[selected_table_cols].copy()

                rename_map = {}
                for col in selected_table_cols:
                    if col in DERIVED_METRICS_INFO:
                        rename_map[col] = f"{col}\n({DERIVED_METRICS_INFO[col]})"
                display_renamed = display_top.rename(columns=rename_map)

                st.dataframe(display_renamed, use_container_width=True, height=600)

                csv_data = display_top.to_csv(index=True, encoding='utf-8-sig')
                st.download_button(
                    "📥 导出垂类排行CSV",
                    csv_data,
                    file_name=f"垂类TOP_{filter_desc}_{top_sort_by}_{top_time}.csv",
                    mime="text/csv",
                    key="top_export"
                )
            else:
                st.info("请至少选择一列")

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