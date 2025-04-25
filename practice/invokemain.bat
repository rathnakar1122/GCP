REM powershell -windowstyle minimized -c "powershell -executionpolicy bypass -c \\hbeu.adroot.hsbc\UK\Finance\application\Input_AltApp2\50_DBS\12_IntigrationScripts\GSCDesk\bin\sftpGSCDesk.ps1 -verbose >> \\hbeu.adroot.hsbc\UK\Finance\application\Input_AltApp2\50_DBS\12_IntigrationScripts\GSCDesk\logs\sftpGSCDesk.log 2>&1"
set mydate=%date:/=%
set mytime=%time::=%
set mytimestamp=%mydate: =_%_%mytime:.=_%
set datetime="%DATE:/=-%-%TIME::=_%"
set basedir=\\hbeu.adroot.hsbc\UK\Finance\application\Input_AltApp2\50_DBS
set workdir=29_IntegrationScripts
set proddir=sftp_out_jll_dev
set logfile1=%basedir%\%workdir%\%proddir%\logs\floor%datetime%.log
set logfile2=%basedir%\%workdir%\%proddir%\logs\floor-space%datetime%.log


powershell -windowstyle minimized -c "powershell -executionpolicy bypass -c \\hbeu.adroot.hsbc\UK\Finance\application\Input_AltApp2\50_DBS\29_IntegrationScripts\sftp_out_jll_dev\bin\manage_gcs_bucket.py floor" >> %logfile1% 2>&1
powershell -windowstyle minimized -c "powershell -executionpolicy bypass -c \\hbeu.adroot.hsbc\UK\Finance\application\Input_AltApp2\50_DBS\29_IntegrationScripts\sftp_out_jll_dev\bin\manage_gcs_bucket.py floor-space" >> %logfile2% 2>&1
REM exit