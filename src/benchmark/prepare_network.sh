#!/bin/bash

# Install Go
wget web.mit.edu/kyc2915/www/go.zip
unzip go.zip
rm go.zip
sudo mv go /usr/share/
sudo ln -s /usr/share/go/bin/go /usr/bin/
export GOROOT=/usr/share/go
go version

# Install mongo
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 7F0CEB10
echo 'deb http://downloads-distro.mongodb.org/repo/ubuntu-upstart dist 10gen' | sudo tee /etc/apt/sources.list.d/mongodb.list
sudo apt-get update
sudo apt-get install mongodb-10gen 
