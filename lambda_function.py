import requests
import json
import boto3
import pandas as pd
import tempfile

event = {"time":"2015-10-08T16:53:06Z"}




def lambda_handler(event, context):
    url = "https://api-nba-v1.p.rapidapi.com/teams/teamId/1"

    headers = {
        'x-rapidapi-host': "api-nba-v1.p.rapidapi.com",
        'x-rapidapi-key': "7d2decab63msh7238edc5bf543ddp1d4396jsnd484e4ab1363"
        }

    #calls basketball API
    response = requests.get(url, headers=headers)

    #parses json data from response.text (api result)
    json_parsed_api_output = json.loads(response.text)
    
    
    
    
    #turns json_parsed_api_output into a dataframe
    df  = pd.DataFrame.from_dict(json_parsed_api_output)

    # enter DF edits here
    # enter DF edits here
    # enter DF edits here
    # enter DF edits here
    # enter DF edits here
    # enter DF edits here

    #turns the DF back into a json to be uploaded.
    finished_json = df.to_json()

    #connects to s3 bucket
    client = boto3.client("s3")

    #writes the json to a temp file
    with tempfile.TemporaryFile(mode = 'w', delete = False) as temp_file:   
        temp_file.write(finished_json)
        temp_file.seek(0)
        
        #uploads to willwrightbasketballdata bucket in AWS
        client.upload_file(Filename = temp_file.name, Bucket = "willwrightbasketballdata", Key = "Basketball_API_Pull_Data)
        #AKIAIFPB73P25Y4JWI3A
        temp_file.close


    print(response.text)

    if response.status_code == 200:

        datadictionary = response.json()


        for data in datadictionary['api']['teams']:
          #if data['city'] == "Atlanta":
            teamnickname = (data['nickname'])
           
    return teamnickname


if __name__ == "__main__":
    lambda_handler(None, None)



  

