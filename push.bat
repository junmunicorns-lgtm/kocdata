@echo off
cd /d D:\KOCdata
if exist .git\index.lock del .git\index.lock
git add .
git commit -m "update"
git push
echo done
pause