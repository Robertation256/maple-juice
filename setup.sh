cd ~

touch config.txt
echo "LOG_SERVER_PORT=8000" > config.txt
echo "LOG_FILE_NAME=log" >> config.txt
echo "LOG_SERVER_ID=vm$1" >> config.txt
echo "LOG_SERVER_HOSTNAMES=fa23-cs425-3801.cs.illinois.edu,fa23-cs425-3802.cs.illinois.edu,fa23-cs425-3803.cs.illinois.edu,fa23-cs425-3804.cs.illinois.edu,fa23-cs425-3805.cs.illinois.edu,fa23-cs425-3806.cs.illinois.edu,fa23-cs425-3807.cs.illinois.edu,fa23-cs425-3808.cs.illinois.edu,fa23-cs425-3809.cs.illinois.edu,fa23-cs425-3810.cs.illinois.edu" >> config.txt
echo "SSH_USERNAME=$2" >> config.txt
echo "SSH_PASSWORD=$3" >> config.txt


