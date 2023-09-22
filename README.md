# Instructions
## Running the program
1. Install Go
2. Clone the repo and cd into project folder: `cd cs-425-mp2`
3. Run the setup script with the following command

   `sh setup.sh`

   **Please make sure there isn't a folder named `log` under your home directory.**

4. Run the porgram with `go run main.go`
5. The program will ask you whether it should be started as a bootstrap server. 
    
   **Starting the program as a bootstrap server means the current vm will also act as an introducer.**
7. Follow the prompts to finish starting up services



## Please Note
1. By default the log server (used for retrieving logs from remote machines) will run on port 8000, so please avoid using port 8000 for introducer and membership list services
2. Program log file is named `log` and stored under the home directory. When the program starts, it will truncate `log` (overwriting logs from previous run).

