import datetime
import boto3
import os

# TODO: Paths need to be reset
raise Exception('PATHS NEED TO BE RESET')

theDate = datetime.datetime.strptime('2000-01-01', '%Y-%m-%d')
url = '{}/{}1200.CHRTOUT_DOMAIN1.comp'.format(theDate.strftime('%Y'), theDate.strftime('%Y%m%d'))
print(url)

file_path = '/SOMEPATH/GISData/NWM/{}'.format(os.path.basename(url))
s3 = boto3.client('s3')
s3.download_file('nwm-archive', url, file_path)
