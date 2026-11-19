#!/bin/bash

# Generate EC-parameters
openssl ecparam -name prime256v1 -out ec_parameters.pem

# Create the permissions CA
openssl req -x509 -newkey param:ec_parameters.pem -keyout permissions_ca_private_key.pem -passout file:password  -out permissions_ca.cert.pem -days 999999 -subj "/O=Example Organization/CN=permissions_ca_common_name"

# Sign the configuration documents
openssl smime -sign -in governance_unsigned.xml -text -out governance.p7s -signer permissions_ca.cert.pem -inkey permissions_ca_private_key.pem -passin file:password
openssl smime -sign -in permissions_unsigned.xml -text -out permissions.p7s -signer permissions_ca.cert.pem -inkey permissions_ca_private_key.pem -passin file:password
openssl smime -sign -in allow_unauthenticated/governance_unsigned.xml -text -out allow_unauthenticated/governance.p7s -signer permissions_ca.cert.pem -inkey permissions_ca_private_key.pem -passin file:password
openssl smime -sign -in allow_unauthenticated/permissions_unsigned.xml -text -out allow_unauthenticated/permissions.p7s -signer permissions_ca.cert.pem -inkey permissions_ca_private_key.pem -passin file:password


# Create the identity CA
openssl req -x509 -newkey param:ec_parameters.pem -keyout identity_ca_private_key.pem -passout file:password -out identity_ca.cert.pem -days 999999 -subj "/O=Example Organization/CN=identity_ca_common_name"

# Create a certificate request for the participant identity certificate
openssl req -newkey param:ec_parameters.pem -keyout key.pem -nodes -out identity_certificate_request.pem -subj "/O=Example Organization/CN=participant1_common_name"

# Sign the certificate request
openssl x509 -req -days 999999 -in identity_certificate_request.pem -CA identity_ca.cert.pem -CAkey identity_ca_private_key.pem -passin file:password -out cert.pem -set_serial 1
