import os
import sys
from hashlib import md5
from argparse import ArgumentParser

def factor_of_1MB(filesize, num_parts):
    x = filesize / int(num_parts)
    y = x % 1048576
    return int(x + 1048576 - y)

def get_md5(inputfile):
    return md5(open(inputfile, 'rb').read()).hexdigest()

def calc_etag(inputfile, partsize):
    md5_digests = []
    with open(inputfile, 'rb') as f:
        for chunk in iter(lambda: f.read(partsize), b''):
            md5_digests.append(md5(chunk).digest())
    return md5(b''.join(md5_digests)).hexdigest() + '-' + str(len(md5_digests))

def possible_partsizes(filesize, num_parts):
    return lambda partsize: partsize < filesize and (float(filesize) / float(partsize)) <= num_parts

def checkEtag(inputfile, etag):
    filesize  = os.path.getsize(inputfile)
    if '-' in etag:
        num_parts = int(etag.split('-')[1])
        partsizes = [ ## Default Partsizes Map
            8388608, # aws_cli/boto3
            15728640, # s3cmd
            factor_of_1MB(filesize, num_parts) # Used by many clients to upload large files
        ]

        for partsize in filter(possible_partsizes(filesize, num_parts), partsizes):
            if etag == calc_etag(inputfile, partsize):
                return True
    else:
        return '"{}"'.format(get_md5(inputfile)) == etag

    return False

if __name__ == "__main__":
  parser = ArgumentParser(description='Compare an S3 etag to a local file')
  parser.add_argument('inputfile', help='The local file')
  parser.add_argument('etag', help='The etag from s3')
  args = parser.parse_args()  
  checkEtag(args.inputfile, args.etag)