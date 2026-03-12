import pandas as pd
import json
import os
from pathlib import Path
from datetime import datetime

# ========== 配置 ==========
DATA_DIR = "data"
API_DIR = "api"

# ========== 工具函数 ==========

def force_numeric(df):
    """把所有能转成数字的列都转成数字"""
    skip_cols = ["数据日期"]
    for col in df.columns:
        if col in skip_cols:
            continue
        try:
            cleaned = df[col].astype(str).str.replace(",", "").str.replace(" ", "").str.replace("，", "")
            converted = pd.to_numeric(cleaned, errors="coerce")
            if converted.notna().sum() > len(df) * 0.3:
                df[col] = converted
        except:
            pass
    return df


def safe_div(a, b):
    result = a / b
    result = result.replace([float('inf'), float('-inf')], 0).fillna(0)
    return result


def detect_author_col(df):
    for col in ["主播昵称", "昵称", "作者", "主播名称"]:
        if col in df.columns:
            return col
    return None


def sanitize_filename(name):
    """清洗文件名，去掉 Windows 不允许的字符"""
    safe = str(name).strip()
    for ch in ['\\', '/', ':', '*', '?', '"', '<', '>', '|', '\n', '\r']:
        safe = safe.replace(ch, '_')
    safe = safe.strip().strip('.')
    if not safe or safe == '_':
        safe = "unknown"
    return safe


def load_all_data():
    dfs = []
    for f in sorted(Path(DATA_DIR).glob("*.csv")):
        try:
            # 依次尝试多种编码
            df = None
            used_enc = ""
            for enc in ["utf-8-sig", "gbk", "gb2312", "gb18030", "latin1"]:
                try:
                    df = pd.read_csv(f, encoding=enc)
                    used_enc = enc
                    break
                except (UnicodeDecodeError, UnicodeError):
                    continue
            if df is None:
                print(f"  跳过 {f.name}: 所有编码都失败")
                continue
            df["数据日期"] = f.stem
            df = force_numeric(df)
            dfs.append(df)
            print(f"  OK {f.name}: {len(df)} 行 (编码: {used_enc})")
        except Exception as e:
            print(f"  跳过 {f.name}: {e}")

    if not dfs:
        for f in sorted(Path(DATA_DIR).glob("*.xlsx")):
            try:
                df = pd.read_excel(f)
                df["数据日期"] = f.stem
                df = force_numeric(df)
                dfs.append(df)
                print(f"  OK {f.name}: {len(df)} 行")
            except Exception as e:
                print(f"  跳过 {f.name}: {e}")

    if not dfs:
        print("没有找到数据文件！")
        return pd.DataFrame()

    merged = pd.concat(dfs, ignore_index=True)
    print(f"共加载 {len(dfs)} 个文件，{len(merged)} 行")
    return merged


def add_derived_metrics(df):
    if "人气峰值" in df.columns and "粉丝数" in df.columns:
        df["直播有效率"] = safe_div(df["人气峰值"], df["粉丝数"])
    if "feed_acu" in df.columns and "粉丝数" in df.columns:
        df["feed效率"] = safe_div(df["feed_acu"], df["粉丝数"])
    if "点赞量" in df.columns and "视频播放量" in df.columns:
        df["互动率"] = safe_div(df["点赞量"], df["视频播放量"])
    if "评论量" in df.columns and "点赞量" in df.columns:
        df["评论深度比"] = safe_div(df["评论量"], df["点赞量"])
    if "涨粉" in df.columns and "粉丝数" in df.columns:
        df["涨粉率"] = safe_div(df["涨粉"], df["粉丝数"])
    if "视频播放量" in df.columns and "作品数" in df.columns:
        df["单作品播放量"] = safe_div(df["视频播放量"], df["作品数"])
    if "点赞量" in df.columns and "作品数" in df.columns:
        df["单作品点赞量"] = safe_div(df["点赞量"], df["作品数"])
    return df


def fix_nan(obj):
    """递归把 NaN/NaT 转成 None，方便 JSON 序列化"""
    if isinstance(obj, float) and (pd.isna(obj)):
        return None
    if isinstance(obj, dict):
        return {k: fix_nan(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [fix_nan(i) for i in obj]
    return obj


def df_to_json_safe(df):
    """DataFrame 转 JSON 安全的 list[dict]"""
    records = df.to_dict(orient="records")
    return fix_nan(records)


# ========== 导出函数 ==========

def export_overview(df):
    """导出总览数据"""
    os.makedirs(API_DIR, exist_ok=True)

    author_col = detect_author_col(df)
    dates = sorted(df["数据日期"].unique().tolist())

    overview = {
        "更新时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "数据日期范围": dates,
        "总行数": len(df),
        "列名": df.columns.tolist(),
    }

    if author_col:
        overview["作者总数"] = int(df[author_col].nunique())

    out_path = os.path.join(API_DIR, "overview.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(overview, f, ensure_ascii=False, indent=2)
    print(f"  导出 overview.json")


def export_daily_summary(df):
    """按日期导出汇总"""
    daily_dir = os.path.join(API_DIR, "daily")
    os.makedirs(daily_dir, exist_ok=True)

    numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()

    for date, group in df.groupby("数据日期"):
        summary = {
            "日期": str(date),
            "记录数": len(group),
        }

        # 数值列汇总
        stats = {}
        for col in numeric_cols:
            stats[col] = {
                "总和": round(float(group[col].sum()), 2),
                "平均": round(float(group[col].mean()), 2),
                "最大": round(float(group[col].max()), 2),
                "最小": round(float(group[col].min()), 2),
            }
        summary["指标汇总"] = stats
        summary["明细"] = df_to_json_safe(group)

        safe_date = sanitize_filename(date)
        out_path = os.path.join(daily_dir, f"{safe_date}.json")
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)

    print(f"  导出 {df['数据日期'].nunique()} 个日期文件")


def export_author_list(df, author_details):
    """导出作者列表索引"""
    out_path = os.path.join(API_DIR, "author_list.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump({
            "更新时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "作者数": len(author_details),
            "作者": author_details,
        }, f, ensure_ascii=False, indent=2)
    print(f"  导出 author_list.json")


def export_author_files(df):
    """按作者导出逐日数据"""
    author_col = detect_author_col(df)
    if not author_col:
        print("  未找到作者列，跳过作者导出")
        return []

    author_dir = os.path.join(API_DIR, "authors")
    os.makedirs(author_dir, exist_ok=True)

    author_list = []

    for author, group in df.groupby(author_col):
        safe_name = sanitize_filename(author)

        out_path = os.path.join(author_dir, f"{safe_name}.json")

        records = df_to_json_safe(group.sort_values("数据日期"))

        with open(out_path, "w", encoding="utf-8") as f:
            json.dump({
                "作者": str(author),
                "安全文件名": safe_name,
                "记录数": len(records),
                "数据": records
            }, f, ensure_ascii=False, indent=2)

        author_list.append({
            "作者": str(author),
            "文件": f"authors/{safe_name}.json",
            "记录数": len(records)
        })

    print(f"  导出 {len(author_list)} 个作者文件")
    return author_list


def export_ranking(df):
    """导出排行榜（按最新日期）"""
    latest_date = sorted(df["数据日期"].unique())[-1]
    latest = df[df["数据日期"] == latest_date].copy()

    author_col = detect_author_col(latest)
    if not author_col:
        print("  未找到作者列，跳过排行榜")
        return

    rankings = {}
    numeric_cols = latest.select_dtypes(include=["number"]).columns.tolist()

    for col in numeric_cols:
        top = latest.nlargest(20, col)[[author_col, col, "数据日期"]]
        rankings[col] = df_to_json_safe(top)

    out_path = os.path.join(API_DIR, "ranking.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump({
            "基准日期": str(latest_date),
            "排行榜": rankings
        }, f, ensure_ascii=False, indent=2)
    print(f"  导出 ranking.json（基准日期: {latest_date}）")


def export_trend(df):
    """导出整体趋势数据（按日期汇总）"""
    numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()

    trend = {}
    for col in numeric_cols:
        daily = df.groupby("数据日期")[col].agg(["sum", "mean"]).reset_index()
        daily.columns = ["日期", "总和", "平均"]
        daily = daily.sort_values("日期")
        trend[col] = df_to_json_safe(daily)

    out_path = os.path.join(API_DIR, "trend.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(trend, f, ensure_ascii=False, indent=2)
    print(f"  导出 trend.json")


# ========== 主函数 ==========

def main():
    print("=" * 50)
    print("KOC 数据导出工具")
    print("=" * 50)

    print("\n[1/7] 加载数据...")
    df = load_all_data()
    if df.empty:
        return

    print(f"\n列名: {df.columns.tolist()}")

    print("\n[2/7] 添加衍生指标...")
    df = add_derived_metrics(df)

    print("\n[3/7] 导出总览...")
    export_overview(df)

    print("\n[4/7] 导出每日汇总...")
    export_daily_summary(df)

    print("\n[5/7] 导出作者文件...")
    author_list = export_author_files(df)

    print("\n[6/7] 导出作者列表...")
    export_author_list(df, author_list)

    print("\n[7/7] 导出排行榜和趋势...")
    export_ranking(df)
    export_trend(df)

    print("\n" + "=" * 50)
    print("全部完成！JSON 文件在 api/ 文件夹下")
    print("=" * 50)


if __name__ == "__main__":
    main()