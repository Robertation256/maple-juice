cd ~
wget https://courses.engr.illinois.edu/cs425/fa2023/MPDemos/MP1DemoDataFA23.zip

touch config.txt

# make sure hostname of current machine is the first one
echo "fa23-cs425-3801.cs.illinois.edu:8000,fa23-cs425-3802.cs.illinois.edu:8000,fa23-cs425-3803.cs.illinois.edu:8000,fa23-cs425-3804.cs.illinois.edu:8000,fa23-cs425-3805.cs.illinois.edu:8000,fa23-cs425-3806.cs.illinois.edu:8000,fa23-cs425-3807.cs.illinois.edu:8000,fa23-cs425-3808.cs.illinois.edu:8000,fa23-cs425-3809.cs.illinois.edu:8000,fa23-cs425-3810.cs.illinois.edu:8000" > config.txt

mkdir log
mv MP1DemoDataFA23.zip ./log
cd ./log
unzip MP1DemoDataFA23.zip
mv ./'MP1 Demo Data FA22'/vm$1.log ./
rm -rf ./'MP1 Demo Data FA22'
rm  MP1DemoDataFA23.zip

