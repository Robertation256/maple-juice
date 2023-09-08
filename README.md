# How to run
1. Install Go
2. Under the same directory you should have /cs425-mp1 (project directory), /log (put log files here), /test_log (where test logs are written), config.txt (a list of host addresses)
3. config.txt should be a list of comma separated host address (ie. hostname1:8000,hostname2:8000). The first entry will be considered as the address of the localhost.
4. cd into project folder and run "go run ./main.go"