@echo off
setlocal enabledelayedexpansion

echo Starting cURL Load Test...

echo Timestamp,Response Time (s) > "D:\KTPM-architecture-solution-main\CS2\upload-test-result.csv"

for /L %%s in (1,1,10) do (
    echo .
    echo Running time %%s.
    curl -s -X POST -F "image=@D:\KTPM-architecture-solution-main\CS2\sample-image\msedge_MgkLtHaimR.png" http://localhost:5000/upload -w "%%{time_total}" -o nul > temp.txt
    set /p RESPONSE_TIME=<temp.txt
    echo !time!,!RESPONSE_TIME! >> "D:\KTPM-architecture-solution-main\CS2\upload-test-result.csv"
)

echo Load Test Complete. Results saved to D:\KTPM-architecture-solution-main\CS2\upload-test-result.csv
del temp.txt
pause