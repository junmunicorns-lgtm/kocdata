@echo off 
cd /d D:\KOCdata 
echo. 
echo ========================================== 
echo   正在推送代码到 GitHub... 
echo ========================================== 
echo. 
git add . 
git commit -m "update %2026/03/11 周三% %19:04:22.60%"
git push origin main 
echo. 
echo ========================================== 
echo   推送完成！ 
echo ========================================== 
echo. 
pause 
