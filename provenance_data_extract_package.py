import boto3
import json
import numpy as np
import ipywidgets as widgets
from IPython.display import display
import re
import pandas as pd
import os

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
    
    def sub_bucket_folders(self,BUCKET_NAME,Prefix):
        s3 = boto3.client('s3')
        response_2 = s3.list_objects_v2(Bucket=f"{BUCKET_NAME}",Prefix = Prefix,Delimiter="/")
        files_inter_backchannel = sorted([prefix['Prefix'].split('/')[1] for prefix in response_2['CommonPrefixes'] if 'CommonPrefixes' in response_2])

        intermediate_folder = [folder for folder in files_inter_backchannel if 'intermediate' in folder and (folder=='intermediate-responses' or folder=='intermediate-files')]
        return intermediate_folder[0]

    
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
            print(f'No files available in the directory! : {BUCKET_NAME} - {prefix_name}')
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

    def extract_data(self,testset_files,comparator_files,Testset_BUCKET_NAME,Comparator_BUCKET_NAME):

        exclude_services_list = ["field_extraction","format_conversion","text_to_attributes"]
        testset_files = [s for s in testset_files if all(sub not in s for sub in exclude_services_list)]
        comparator_files = [s for s in comparator_files if all(sub not in s for sub in exclude_services_list)]

        testset_services = self.services_data(testset_files,Testset_BUCKET_NAME)
        comparator_services = self.services_data(comparator_files,Comparator_BUCKET_NAME)
        services_list = ["termite","xpath_union","entity_tag","entity_subheading","label_and_weight","streaming_relevancy"]

        services_used_counts = {}
        for key in services_list:
                services_used_counts[key] = {"Services":key,"Comparator+Testset ID": f"{comparator_files[0].split('/')[2]}_{testset_files[0].split('/')[2]}",f"Comparator_{comparator_files[0].split('/')[0]}":["No diff"] if not list(self.match_pattern(comparator_services[key])-self.match_pattern(testset_services[key])) else list(self.match_pattern(comparator_services[key])-self.match_pattern(testset_services[key])),
                                f"Testset_{testset_files[0].split('/')[0]}": ["No diff"] if not list(self.match_pattern(testset_services[key])-self.match_pattern(comparator_services[key])) else list(self.match_pattern(testset_services[key])-self.match_pattern(comparator_services[key]))}


        dict_services = {}
        for service in services_used_counts.keys():
            dict_services[service] = pd.DataFrame(services_used_counts[service])

        return dict_services
    
    def standalone_file_data_extract(self,):
        files = [file for file in os.listdir('Input_files') if file!='.gitkeep']
        if len(files)>0:
            # file = os.listdir('Input_files')[0]
            with open(f'Input_files/{files[0]}','r') as file:
                data = json.loads(file.read())

            df_standalone_file = pd.DataFrame(self.match_pattern(data['provenance'][0]['externalResourceVersion']),columns=["Packages"])

            df_standalone_file = df_standalone_file.drop_duplicates().reset_index(drop=True)

            return df_standalone_file
        else:
            # print('No standalone file present inside Input_files directory!!')
            return pd.DataFrame()
        
    def highlight_true(self,cell):
        if cell == "No diff":
            return 'background-color: lightgreen'
        else:
            return 'background-color: red'
        return ''
    

