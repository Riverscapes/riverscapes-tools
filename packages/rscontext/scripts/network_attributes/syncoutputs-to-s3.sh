cd scripts/network_attributes
aws s3 sync . s3://riverscapes-athena/data_exchange/rs-context-neo --exclude ".DS_Store"
