# About MapleJuice
MapleJuice, as its name infers, is a MapReduce-like distributed computing framework. The framework includes two parts: a simple distributed file system and a MapleJuice engine built on top of Go socket and rpc. This project also contains implementations of a gossip-style membership protocol and a quorum-based leader election protocol that help maintain the topology of the service cluster.

# Running the program
1. Install Go
2. Clone and cd into project folder
3. Run the setup script on each node with `sh setup.sh <server-id>` (server id can be any number and is used only for easy identification when retrieving logs from remote servers)

4. Make necessary changes to the generated config.txt file under home directory as indicated below.
5. Run the program with `go run main.go`. 
6. Follow the prompts to finish starting up services.
7. Enter a command or type `help` for a list of available commands

# Configuration
1. By default the log server (used for retrieving logs from remote machines) will run on port 8000. To use a different port, update the `LOG_SERVER_PORT=<port>` entry in /UserHome/config.txt
2. `LEADER_ELECTION_QUORUM_SIZE` and `REPLICATION_FACTOR` should be correctly configured according to cluster size and fault-tolerance guarantees.
3.  Set `IS_INTRODUCER`=`TRUE` for the introducer node and set the INTRODUCER_IP for other node correspondingly.

