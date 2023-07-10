#!/bin/bash

set -e

DESTDIR=`dirname "$0"`/keys
test -d ${DESTDIR} || mkdir ${DESTDIR}
cd ${DESTDIR}

SSL_SUBJECT="/C=CA/ST=Winnipeg/L=Manitoba/O=Some Corp/OU=IT Department/CN=example.com"
which openssl &>/dev/null
if [ $? -ne 0 ]; then
	echo "No openssl binary present, exiting."
	exit 1
fi

openssl genrsa -out ca-key.pem 2048 &>/dev/null

openssl req -new -x509 -nodes -days 365000 \
   -key ca-key.pem \
   -out ca-cert.pem \
   -subj "${SSL_SUBJECT}" &>/dev/null

openssl req -newkey rsa:2048 -nodes -days 365000 \
   -keyout server-key.pem \
   -out server-req.pem \
   -subj "${SSL_SUBJECT}" &>/dev/null

openssl x509 -req -days 365000 -set_serial 01 \
   -in server-req.pem \
   -out server-cert.pem \
   -CA ca-cert.pem \
   -CAkey ca-key.pem &>/dev/null

openssl req -newkey rsa:2048 -nodes -days 365000 \
   -keyout client-key.pem \
   -out client-req.pem \
   -subj "${SSL_SUBJECT}" &>/dev/null

openssl x509 -req -days 365000 -set_serial 01 \
   -in client-req.pem \
   -out client-cert.pem \
   -CA ca-cert.pem \
   -CAkey ca-key.pem &>/dev/null

echo "Keys generated in ${DESTDIR}:"
ls
