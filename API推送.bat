@echo off
chcp 65001 >nul
cd /d D:\KOCdata

echo ========================================
echo   导出数据为 API JSON 文件
echo ========================================

python export_api_data.py

echo.
echo ========================================
echo   推送到 GitHub
echo ========================================

git add -A
git commit -m "更新 API 数据 %date% %time%"
git push

echo.
echo ✅ 完成！数据已导出并推送
pause