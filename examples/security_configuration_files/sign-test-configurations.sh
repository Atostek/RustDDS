#!/bin/bash

# Sign test configurations
openssl smime -sign -in governance_unsigned.xml -text -out governance.p7s -signer permissions_ca.cert.pem -inkey permissions_ca_private_key.pem -passin file:password
openssl smime -sign -in permissions_unsigned.xml -text -out permissions.p7s -signer permissions_ca.cert.pem -inkey permissions_ca_private_key.pem -passin file:password

# For tests of participants that cannot be authenticated
openssl smime -sign -in allow_unauthenticated/governance_unsigned.xml -text -out allow_unauthenticated/governance.p7s -signer permissions_ca.cert.pem -inkey permissions_ca_private_key.pem -passin file:password
openssl smime -sign -in allow_unauthenticated/permissions_unsigned.xml -text -out allow_unauthenticated/permissions.p7s -signer permissions_ca.cert.pem -inkey permissions_ca_private_key.pem -passin file:password
