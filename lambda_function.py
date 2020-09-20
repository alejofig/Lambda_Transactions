import json
import urllib.parse
import boto3
import re
import pandas as pd 
from email.parser import Parser
from botocore.vendored import requests

print('Loading function')

s3 = boto3.client('s3')


def lambda_handler(event, context):

    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        obj = response['Body'].read()
        obj = obj.decode("utf-8")
        with open('/tmp/bbva', 'w') as data:
            data.write(obj)
        p = Parser()
        msg = p.parse(open('/tmp/bbva'))
        table = msg.get_payload()      
        dfs = pd.read_html(table,header=None)
        df = dfs[0]
        df = df.iloc[1,:]
        for row in df:
            x = re.search("\d{4}-\d{2}-\d{2}",row)
            date = x.group()
            x = re.search("\d{2}:\d{2}",row)
            tx_time = x.group()
            x = re.findall("\(.*?\)",row)
            establecimiento = str(x[1])
            establecimiento = establecimiento.replace('(',"")
            establecimiento = establecimiento.replace(')',"")
            establecimiento = establecimiento.strip()
            x = re.search("((\d){1,3})+([,][\d]{3})*([.](\d)*)",row)
            valor = x.group()
            valor = valor.replace(",","")
            x = re.findall("\d{4}",row) 
            ultimos_cuatro = x[1]
            banco = "BBVA"
            vals = {'Tx_Date':date,"Tx_Time":tx_time,'Tx_Place':establecimiento,"Tx_Value":float(valor),"Last4":ultimos_cuatro,"Bank":banco}
            requests.post(ENDPOINT_ZAPIER, data=vals)
        return response['ContentType']
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e
