import boto3
import json
import numpy as np
import ipywidgets as widgets
from IPython.display import display
import re

class prov_packages:

    def __init__(self) -> None:
        pass

    def select_bucket(self,description):
        s3 = boto3.client('s3')
        response = s3.list_buckets()
        buckets = [bucket['Name'] for bucket in response['Buckets']]

        # Create the dropdown widget
        bucket_dropdown = widgets.Dropdown(
            options=buckets,
            value = 'bce-car-flow-uat-testing',
            description=f'{description}:',
            disabled=False,
        )

        # Display the selected bucket
        def on_bucket_change(change):
            print(f'{description}: {change["new"]}')

        bucket_dropdown.observe(on_bucket_change, names='value')

        # Display the widget
        display(bucket_dropdown)

        return bucket_dropdown
    
    def dropdown_s3(self,response,description):
        drop_down = widgets.Dropdown(
            options=response,
            description = description,
            disabled=False,
            layout=widgets.Layout(width='300px')
        )
        
        # Display the selected bucket
        def on_bucket_change(change):
            print(f'{description}: {change["new"]}')

        drop_down.observe(on_bucket_change, names='value')

        # Display the widget
        display(drop_down)

        return drop_down
    
    def response_sub_bucket(self,bucket_name,title):
        s3 = boto3.client('s3')
        response_bucket =  s3.list_objects_v2(Bucket=bucket_name,Delimiter='/')
        response_bucket_sub_folder = [prefix['Prefix'] for prefix in response_bucket['CommonPrefixes'] if 'CommonPrefixes' in response_bucket]
        prefix_selection = self.dropdown_s3(response_bucket_sub_folder,description=title)

        return prefix_selection
    

    def get_filenames(self,BUCKET_NAME,prefix_name):
        try:
            s3 = boto3.client('s3')
            response = s3.list_objects_v2(Bucket=f"{BUCKET_NAME}",Prefix = prefix_name,Delimiter="/")
            
            rnf2_files = [prefix['Prefix'] for prefix in response['CommonPrefixes'] if 'CommonPrefixes' in response]
            if len(rnf2_files)>0:
                random_file_key = rnf2_files[np.random.randint(0,len(rnf2_files)+1)]
                response2 = s3.list_objects_v2(Bucket=f"{BUCKET_NAME}",Prefix = f"{prefix_name}{random_file_key.split('/')[-2]}",Delimiter="")
                keys_new = [prefix['Key'] for prefix in response2['Contents'] if 'Contents' in response2]
                return keys_new
        except Exception as e:
            print('No files available in the directory!!')
            print(f"Exception error: {str(e)}")
            return []
        
    
    def provenance_info_pull(self,file_key,BUCKET_NAME):
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket=BUCKET_NAME,Key=file_key)
        file_content = response['Body'].read().decode('utf-8')
        json_content = json.loads(file_content)
        if 'provenance' in json_content:
            if 'externalResourceVersion' in json_content['provenance'][0]:
                dict = json_content['provenance'][0]['externalResourceVersion']
                return dict
        else:
            return ['No provenance data!']
        
    def services_data(self,files,BUCKET_NAME):
        services = {}
        for file in files:
            try:
                services[file.split('/')[-2]] = self.provenance_info_pull(file,BUCKET_NAME=BUCKET_NAME)
                # print(f'Pass - {file}')
            except Exception as e:
                print(str(e))
                print(f'fail - {file}')
        return services
    
    def match_pattern(self,string_ls):
        pattern = re.compile(r'.*?\.tar\.gz')
        extracted_parts = set([pattern.match(s).group(0).split('/')[-1] if pattern.match(s) else None for s in string_ls])
        return extracted_parts

        

