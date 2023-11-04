cd ~

mkdir sdfs
touch config.txt

echo "MEMBERSHIP_SERVICE_PORT=8001" > config.txt
echo "MEMBERSHIP_PROTOCOL=G" >> config.txt
echo "IS_INTRODUCER=FALSE" >> config.txt
echo "INTRODUCER_IP=<xx.xx.xx.xx>" >> config.txt
echo "INTRODUCER_PORT=8002" >> config.txt


echo "LEADER_ELECTION_SERVER_PORT=8003" >> config.txt
echo "LEADER_ELECTION_QUORUM_SIZE=2" >> config.txt


echo "REPLICATION_FACTOR=2" >> config.txt

echo "RPC_SERVER_PORT=8004" >> config.txt


echo "LOG_FILE_NAME=log" >> config.txt
echo "LOG_SERVER_ID=vm$1" >> config.txt
echo "SERVER_HOSTNAMES=fa23-cs425-3801.cs.illinois.edu,fa23-cs425-3802.cs.illinois.edu,fa23-cs425-3803.cs.illinois.edu,fa23-cs425-3804.cs.illinois.edu,fa23-cs425-3805.cs.illinois.edu,fa23-cs425-3806.cs.illinois.edu,fa23-cs425-3807.cs.illinois.edu,fa23-cs425-3808.cs.illinois.edu,fa23-cs425-3809.cs.illinois.edu,fa23-cs425-3810.cs.illinois.edu" >> config.txt
echo "SSH_USERNAME=$2" >> config.txt
echo "SSH_PASSWORD=$3" >> config.txt


