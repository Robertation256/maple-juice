cd ~

mkdir -p sdfs
mkdir -p local
mkdir -p mr_job_manager
mkdir -p mr_node_manager
mkdir -p sql_template
touch config.txt

echo "MEMBERSHIP_SERVICE_PORT=8001" > config.txt
echo "MEMBERSHIP_PROTOCOL=G" >> config.txt
echo "IS_INTRODUCER=FALSE" >> config.txt
echo "INTRODUCER_IP=<xx.xx.xx.xx>" >> config.txt
echo "INTRODUCER_PORT=8002" >> config.txt


echo "LEADER_ELECTION_SERVER_PORT=8003" >> config.txt
echo "LEADER_ELECTION_QUORUM_SIZE=3" >> config.txt


echo "REPLICATION_FACTOR=3" >> config.txt

echo "RPC_SERVER_PORT=8004" >> config.txt
echo "FILE_RECEIVE_PORT=8005" >> config.txt

#configuration for sql layer executor num
echo "MAPLE_TASK_NUM=3" >> config.txt
echo "JUICE_TASK_NUM=3" >> config.txt

echo "LOG_FILE_NAME=log" >> config.txt
echo "LOG_SERVER_ID=vm$1" >> config.txt
echo "SERVER_HOSTNAMES=fa23-cs425-3801.cs.illinois.edu,fa23-cs425-3802.cs.illinois.edu,fa23-cs425-3803.cs.illinois.edu,fa23-cs425-3804.cs.illinois.edu,fa23-cs425-3805.cs.illinois.edu,fa23-cs425-3806.cs.illinois.edu,fa23-cs425-3807.cs.illinois.edu,fa23-cs425-3808.cs.illinois.edu,fa23-cs425-3809.cs.illinois.edu,fa23-cs425-3810.cs.illinois.edu" >> config.txt

cp ~/maple-juice/sql/filter_juice/* ~/sql_template/
cp ~/maple-juice/sql/filter_maple/* ~/sql_template/
cp ~/maple-juice/sql/join_juice/* ~/sql_template/
cp ~/maple-juice/sql/join_maple/* ~/sql_template/

cp ~/maple-juice/demo/maple1/maple1.go ~/sql_template/demo_maple1.go
cp ~/maple-juice/demo/maple2/maple2.go ~/sql_template/demo_maple2.go
cp ~/maple-juice/demo/juice1/juice1.go ~/sql_template/demo_juice1.go
cp ~/maple-juice/demo/juice2/juice2.go ~/sql_template/demo_juice2.go


