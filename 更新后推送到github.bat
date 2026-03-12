@echo on
cd /d D:\KOCdata
if exist .git\index.lock del .git\index.lock
echo 正在添加文件...
git add .
echo 正在提交...
git commit -m "update"
echo 正在推送...
git push
echo 完成！
pause