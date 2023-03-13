
@ECHO OFF

REM Run Matlab script with output to stdout and correct exit codes
REM matlab.bat script.m

REM Arguments:

SET folder=%CD%
SET script=%folder%\%1

REM Settings:

SET binary="C:\Apps\MATLAB\R2016b\bin\matlab.exe"
SET output="%script%.tmp"
SET code="try; cd('%folder%'); run('%script%'); catch e; disp(e.message); exit(1); end; exit(0);"

REM Execute headless-Matlab and write output to log-file 
%binary% -automation -singleCompThread -nojvm -nodesktop -nosplash -wait -logfile %output% -r %code%

REM Catch exit code for proper error-propagation
SET result=%ERRORLEVEL%

REM Print output for the scheduler to pick-up
TYPE %output%

REM Delete output file
DEL %output%

REM Exit batch process with proper exit-code from Matlab
EXIT /B %result%
