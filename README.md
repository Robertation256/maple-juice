# How to run
1. Install Go
2. Put project folder in the same directory with log directory and config file
3. Folder structure should look like
/home
-- /cs425-mp1
-- /log (put log files here)
-- /test_log (where test logs are written)
-- config.txt (a list of host addresses)
4. config.txt should be a list of comma separated host address (ie. hostname1:8000,hostname2:8000). The first entry will be considered as the address of the localhost.
5. cd into project folder and run "go run ./main.go"