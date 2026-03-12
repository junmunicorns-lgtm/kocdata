@echo off
chcp 65001 >nul
cd /d D:\KOCdata

echo ========================================
echo    KOC 数据更新工具
echo    %date% %time%
echo ========================================
echo.

echo 正在生成 API 数据...
echo.

python export_api_data.py

echo.
echo ========================================
if %errorlevel%==0 (
    echo    更新完成！
) else (
    echo    出错了，请检查上面的报错信息
)
echo ========================================
echo.
pause