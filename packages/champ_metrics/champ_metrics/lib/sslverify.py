import os

def verification():
    # if we have a ca bundle in the current directory, use that as the certificate verification method,
    # otherwise don't do verification.  get your own by curling https://curl.haxx.se/ca/cacert.pem
    certFile = os.path.join(os.path.dirname(__file__), os.path.pardir, 'cacert.pem')

    if not os.path.isfile(certFile):
        return False

    return certFile