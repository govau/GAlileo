cargo build --release

REM start /affinity 3 run.bat 1
REM start /affinity C run.bat 2
REM start /affinity 30 run.bat 3
REM start /affinity C0 run.bat 4
REM start /affinity 300 run.bat 5
REM start /affinity C0 run.bat 6
REM start /affinity 300 run.bat 7
REM start /affinity C000 run.bat 8

start /affinity F run.bat 2
start /affinity F0 run.bat 3
start /affinity F00 run.bat 6
start /affinity F000 run.bat 7
pause