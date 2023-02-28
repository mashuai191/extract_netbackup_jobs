import requests
import pandas as pd
from datetime import datetime
import argparse


#The current operation being performed. Negative values should be ignored.
CurrentOperation = {
        '0':'MOUNTING', '1':'POSITIONING', '2':'CONNECTING', '3':'WRITING', '4':'VLT_INIT', '5':'VLT_DUPIMG', '6':'VLT_DUPCMP', '7':'VLT_BK_NBCAT', '8':'VLT_EJRPT',
        '9':'VLT_COMPLETE', '10':'READING', '11':'DUPLICATE', '12':'IMPORT', '13':'VERIFY', '14':'RESTORE', '15':'BACKUPDB', '16':'VAULT', '17':'LABEL',
        '18':'ERASE', '19':'SYNTH_DBQUERY', '20':'SYNTH_PROCESS_EXTENTS', '21':'SYNTH_PLAN', '22':'CREATE_SNAPSHOT', '23':'DELETE_SNAPSHOT', '24':'RECOVERDB',
        '25':'MCONTENTS', '26':'SYNTH_REQUERESOURCES', '27':'PARENT_JOB', '28':'INDEXING', '29':'REPLICATE', '30':'RUNNING', '31':'ASSEMBLING_FILE_LIST', '32':'ALLOCATING_MEDIA'
}

RetentionLevel = {
     "0": ["7", "1 week"],
     "1": ["14", "2 weeks"],
     "2": ["21", "3 weeks"],
     "3": ["31", "1 month"],
     "4": ["62", "2 months"],
     "5": ["93", "3 months"],
     "6": ["186", "6 months"],
     "7": ["279", "9 months"],
     "8": ["365", "1 year"],
     "9": ["Infinite", "Infinite"],
     "25": ["0", "Expires immediately"]
}

Compression = {
    '0': 'disabled',
    '1': 'enabled'
}

Exclude_columns = ['retentionLevel']

def get_api_token(host, username, password):
    api_base="https://"+host+":1556/netbackup/"
    api_login = api_base+"login"

    data={
        'userName': username,
        'password': password
    }

    response_login = requests.post(
        api_login,
        headers={'content-type': 'application/vnd.netbackup+json;version=1.0'},
        json=data,
        verify=False
    )   

    #print(response_login.json())
    return response_login.json()['token']

def get_jobs_details(host, token):
    api_base="https://"+host+":1556/netbackup/"
    api_jobs = api_base+"admin/jobs"

    response_jobs = requests.get(
        api_jobs,
        headers={'Accept': 'application/vnd.netbackup+json;version=2.0', 'Authorization':token},
        verify=False
        )
    #print (response_jobs.json()['data'])

    response_data = response_jobs.json()['data']

    jobs_list = []
    for item in response_data:
        jobs_list.append(item['attributes'])

    #print (jobs_list)


    df = pd.json_normalize(jobs_list)
    df = df.fillna('')

    return df

def convert_dataframe(df):
    def convert_current_operation(currentOpearation):
        return CurrentOperation.get(str(currentOpearation), '')

    def convert_retention_level(job_reten_level):
        return RetentionLevel.get(str(job_reten_level), '')[0], RetentionLevel.get(str(job_reten_level), '')[1]


    def add_client_os(policy_name):
        global master_host
        global access_token
        api_base="https://"+master_host+":1556/netbackup/"
        api_policies = api_base+"config/policies"
        #print (api_policies)
        try:
            if policy_name:
                response_policy = requests.get(
                    api_policies+"/"+policy_name,
                    headers={'Accept': 'application/vnd.netbackup+json;version=6.0', 'Authorization':access_token},
                    verify=False
                )
                return response_policy.json().get('data', {}).get('attributes',{}).get('policy', {}).get('clients', [])[0].get('OS', '')
            else:
                return ""
        except:
            return ""

    def convert_compression(compression):
        return Compression.get(str(int(compression)), '')

    def calc_duration(timestamp):
        date_format = "%Y-%m-%dT%H:%M:%S.%fZ"
        dt_start = datetime.strptime(timestamp[0], date_format)
        dt_end = datetime.strptime(timestamp[1], date_format)

        c = dt_end - dt_start

        hours = c.seconds / 3600
        durationH = "{:.4f}".format(hours)
        return durationH

    df['currentOperation'] = df['currentOperation'].apply(convert_current_operation)
    df['ClientOS'] = df['policyName'].apply(add_client_os)
    df['compression'] = df['compression'].apply(convert_compression)
    df['Retention(Days)'], df['Retention(Age)'] = zip(*df['retentionLevel'].apply(convert_retention_level))
    df['JobDurationH'] = df[['startTime', 'endTime']].apply(calc_duration, axis = 1)



def remove_unused_columns(df):
    df_chopped = df.drop(Exclude_columns, axis=1)
    return df_chopped


access_token = ''
master_host = ''

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--server", "-s", required=True, help="master server")
    parser.add_argument("--username", "-u", required=True, help="user name")
    parser.add_argument("--password", "-p", required=True, help="password")
    args = parser.parse_args()
    print (args)

    global master_host
    global access_token
    master_host = args.server
    master_username = args.username
    master_password = args.password

    print ("Start processing!")
    access_token = get_api_token(host=master_host, username=master_username, password=master_password)
    df = get_jobs_details(host=master_host, token=access_token)
    convert_dataframe(df)
    df = remove_unused_columns(df)

    output_file = 'output.csv'
    df.to_csv(output_file, index=False)
    print ("Processing done!")

main()

