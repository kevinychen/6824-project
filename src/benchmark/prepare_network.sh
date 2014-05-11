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
# TODO
