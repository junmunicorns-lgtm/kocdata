@echo on
cd /d D:\KOCdata
if exist .git\index.lock del .git\index.lock
echo adding files...
git add .
echo committing...
git commit -m "update"
echo pushing...
git push
echo done!
pause