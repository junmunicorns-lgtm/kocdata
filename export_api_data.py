import pandas as pd
import json
import os
from pathlib import Path
from datetime import datetime

def load_all_data(data_dir="data"):
    """加载 data/ 下所有 CSV 文件，合并为一个 DataFrame"""
    dfs = []
    for f in sorted(Path(data_dir).glob("*.csv")):
        try:
            df = pd.read_csv(f, encoding="utf-8-sig")
            # 从文件名提取日期
            date_str = f.stem  # 如 "20260216"
            df["数据日期"] = date_str
            dfs.append(df)
        except Exception as e:
            print(f"❌ 跳过 {f.name}: {e}")
    
    if not dfs:
        print("⚠️ data/ 下没有找到 CSV 文件")
        return pd.DataFrame()
    
    merged = pd.concat(dfs, ignore_index=True)
    print(f"✅ 加载 {len(dfs)} 个文件，共 {len(merged)} 行")
    return merged


def detect_author_col(df):
    """自动检测作者列名"""
    for col in ["主播昵称", "昵称", "作者", "主播名称"]:
        if col in df.columns:
            return col
    return None


def add_derived_metrics(df):
    """添加衍生指标（和你 app.py 里一致）"""
    
    # 安全除法
    def safe_div(a, b):
        return (a / b).replace([float('inf'), float('-inf')], 0).fillna(0)
    
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
    
    if "视频播放量" in df.columns and "粉丝数" in df.columns:
        df["播放粉丝比"] = safe_div(df["视频播放量"], df["粉丝数"])
    
    return df


def make_summary(df, author_col):
    """生成聚合摘要"""
    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    
    summary = {}
    
    # 全局汇总
    summary["全局均值"] = df[numeric_cols].mean().round(2).to_dict()
    summary["全局中位数"] = df[numeric_cols].median().round(2).to_dict()
    
    # 按作者汇总
    if author_col:
        author_summary = {}
        for name, group in df.groupby(author_col):
            author_summary[str(name)] = {
                "数据天数": len(group),
                "均值": group[numeric_cols].mean().round(2).to_dict(),
            }
        summary["按作者"] = author_summary
    
    return summary


def export():
    """主导出函数"""
    api_dir = Path("api")
    raw_dir = api_dir / "raw"
    author_dir = api_dir / "authors"
    
    # 创建目录
    for d in [api_dir, raw_dir, author_dir]:
        d.mkdir(parents=True, exist_ok=True)
    
    # ---------- 加载数据 ----------
    df = load_all_data("data")
    if df.empty:
        return
    
    author_col = detect_author_col(df)
    print(f"📌 作者列: {author_col}")
    print(f"📌 所有列: {list(df.columns)}")
    
    # 添加衍生指标
    df = add_derived_metrics(df)
    
    # ---------- 1. 导出原始数据（按日期） ----------
    raw_files = []
    for date_str, group in df.groupby("数据日期"):
        out_name = f"{date_str}.json"
        group.to_json(raw_dir / out_name, orient="records", force_ascii=False, indent=2)
        raw_files.append({
            "date": str(date_str),
            "file": f"raw/{out_name}",
            "rows": len(group)
        })
    print(f"✅ 导出 {len(raw_files)} 个原始数据文件")
    
    # ---------- 2. 导出按作者数据 ----------
    author_files = []
    if author_col:
        for name, group in df.groupby(author_col):
            safe_name = str(name).replace("/", "_").replace("\\", "_").replace(" ", "_")
            out_name = f"{safe_name}.json"
            group.sort_values("数据日期").to_json(
                author_dir / out_name, orient="records", force_ascii=False, indent=2
            )
            author_files.append({
                "author": str(name),
                "file": f"authors/{out_name}",
                "data_days": len(group)
            })
        print(f"✅ 导出 {len(author_files)} 个作者文件")
    
    # ---------- 3. 导出聚合摘要 ----------
    # 全量
    summary_all = make_summary(df, author_col)
    summary_all["日期范围"] = {
        "start": df["数据日期"].min(),
        "end": df["数据日期"].max(),
        "total_days": df["数据日期"].nunique()
    }
    with open(api_dir / "summary_all.json", "w", encoding="utf-8") as f:
        json.dump(summary_all, f, ensure_ascii=False, indent=2, default=str)
    
    # 近7天
    all_dates = sorted(df["数据日期"].unique())
    if len(all_dates) >= 7:
        recent_7 = all_dates[-7:]
        df_7 = df[df["数据日期"].isin(recent_7)]
        summary_7 = make_summary(df_7, author_col)
        summary_7["日期范围"] = {"start": recent_7[0], "end": recent_7[-1], "days": 7}
        with open(api_dir / "summary_7d.json", "w", encoding="utf-8") as f:
            json.dump(summary_7, f, ensure_ascii=False, indent=2, default=str)
        print("✅ 导出 summary_7d.json")
    
    # 近30天
    if len(all_dates) >= 30:
        recent_30 = all_dates[-30:]
        df_30 = df[df["数据日期"].isin(recent_30)]
        summary_30 = make_summary(df_30, author_col)
        summary_30["日期范围"] = {"start": recent_30[0], "end": recent_30[-1], "days": 30}
        with open(api_dir / "summary_30d.json", "w", encoding="utf-8") as f:
            json.dump(summary_30, f, ensure_ascii=False, indent=2, default=str)
        print("✅ 导出 summary_30d.json")
    
    # ---------- 4. 导出 index.json（Agent 入口） ----------
    index = {
        "project": "KOC 数据看板",
        "description": "KOC/主播数据的原始数据和聚合分析，含衍生指标",
        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "base_url": "https://raw.githubusercontent.com/junmunicorns-lgtm/kocdata/main/api/",
        "usage": "先读 index.json 获取目录，再按需请求具体文件",
        "endpoints": {
            "summary_all": {
                "file": "summary_all.json",
                "description": "全量数据聚合（全局均值/中位数 + 按作者均值）"
            },
            "summary_7d": {
                "file": "summary_7d.json",
                "description": "近7天数据聚合"
            },
            "summary_30d": {
                "file": "summary_30d.json",
                "description": "近30天数据聚合"
            },
            "raw_by_date": {
                "files": raw_files,
                "description": "按日期的原始数据（含衍生指标）"
            },
            "by_author": {
                "files": author_files,
                "description": "按作者的逐日走势数据"
            }
        },
        "columns_info": {
            "原始列": [c for c in df.columns if c != "数据日期" and c not in [
                "直播有效率","feed效率","互动率","评论深度比","涨粉率",
                "单作品播放量","单作品点赞量","播放粉丝比"
            ]],
            "衍生指标": ["直播有效率","feed效率","互动率","评论深度比","涨粉率",
                        "单作品播放量","单作品点赞量","播放粉丝比"]
        },
        "total_rows": len(df),
        "total_authors": df[author_col].nunique() if author_col else 0,
        "date_range": {
            "start": df["数据日期"].min(),
            "end": df["数据日期"].max()
        }
    }
    
    with open(api_dir / "index.json", "w", encoding="utf-8") as f:
        json.dump(index, f, ensure_ascii=False, indent=2, default=str)
    
    print(f"\n🎉 全部导出完成！")
    print(f"📂 输出目录: {api_dir.absolute()}")
    print(f"🔗 Agent 入口: https://raw.githubusercontent.com/junmunicorns-lgtm/kocdata/main/api/index.json")


if __name__ == "__main__":
    export()