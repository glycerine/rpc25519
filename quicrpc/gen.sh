#!/bin/bash

rm -rf static tmp
mkdir -p static/certs/
mkdir -p tmp

## warning/helpful: these are good for 100 years.

## This will be a self-signed certificate authority.
##    openssl verify -CAfile ca.crt ca.crt
## will return OK, demonstrating this.


openssl req -x509 -newkey ed25519 -days 36600 -keyout ca-key.pem -out ca-cert.pem -config ca.cnf -nodes

##openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -days 36600 -keyout ca-key.pem -out ca-cert.pem -config ca.cnf -nodes
## verify that CA:TRUE is there.
echo "verify that CA:TRUE is there on ca-cert.pem"
openssl x509 -in ca-cert.pem -text -noout | grep -A 1 "X509v3 Basic Constraints"

cp ca-cert.pem tmp/ca.crt
mv ca-cert.pem static/certs/ca.crt
mv ca-key.pem  tmp/ca.key

echo "verify that CA:TRUE is there on ca.crt"
openssl x509 -in static/certs/ca.crt -text -noout | grep -A 1 "X509v3 Basic Constraints"


openssl genpkey -algorithm ed25519 -out static/certs/client.key
##openssl pkey -in static/certs/client.key -pubout -out static/certs/client.crt

openssl genpkey -algorithm ed25519 -out static/certs/node.key

##openssl genpkey -algorithm ed25519 -out ed25519-private.pem
##openssl pkey -in ed25519-private.pem -pubout -out ed25519-public.pem

##openssl ecparam -genkey -name prime256v1 -out static/certs/client.key
##openssl ecparam -genkey -name prime256v1 -out static/certs/node.key

openssl req -new -key static/certs/client.key -out static/certs/client.csr  -config openssl-san.cnf
openssl req -new -key static/certs/node.key -out static/certs/node.csr -config openssl-san.cnf

openssl x509 -req -in static/certs/node.csr -CA tmp/ca.crt -CAkey tmp/ca.key -CAcreateserial -out static/certs/node.crt -days 36660 -extfile openssl-san.cnf -extensions req_ext

openssl x509 -req -in static/certs/client.csr -CA tmp/ca.crt -CAkey tmp/ca.key -CAcreateserial -out static/certs/client.crt -days 36600  -extfile openssl-san.cnf -extensions req_ext

cp tmp/ca.crt static/certs/
mv static/certs/node.csr tmp/
mv static/certs/client.csr tmp/

## can use the CA key to generate other keys for other clients or servers
## if need be; but they can also simply re-use these certs.
mv tmp my-keep-private-dir

## separate client and server so it is easy to see which goes to whom.

mkdir -p static/certs/client
mkdir -p static/certs/server
cp static/certs/ca.crt static/certs/client/
mv static/certs/ca.crt static/certs/server/
mv static/certs/client.* static/certs/client/
mv static/certs/node.* static/certs/server/

## make a 2nd client so we can check tofu hardening.
openssl genpkey -algorithm ed25519 -out static/certs/client/client2.key
openssl req -new -key static/certs/client/client2.key -out static/certs/client/client2.csr  -config openssl-san.cnf
openssl x509 -req -in static/certs/client/client2.csr -CA my-keep-private-dir/ca.crt -CAkey my-keep-private-dir/ca.key -CAcreateserial -out static/certs/client/client2.crt -days 36660 -extfile openssl-san.cnf -extensions req_ext
mv static/certs/client/client2.csr my-keep-private-dir/
