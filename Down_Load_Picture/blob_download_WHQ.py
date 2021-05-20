import os, uuid
import time, datetime
import pandas as pd
import multiprocessing
from multiprocessing import cpu_count
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__
from azure.cosmos import CosmosClient

# blob information
container_name=""
connect_str = "

# cosmos information
cosmos_url = ''
master_key = ''
DatabaseName  = ""
TableName = '
# dest = "./data"

# Blob Interface
class DirectoryClient:
    def __init__(self, connect_str, container_name):
        service_client = BlobServiceClient.from_connection_string(connect_str)
        self.client = service_client.get_container_client(container_name)


    def get_blob_path(self, elv_id, begin_ts='2021/1/10', end_ts=None):
        if not end_ts:
            end_ts = datetime.datetime.now()
        # format like: '2021-02-03'
        ts_range = pd.period_range(begin_ts, end_ts).values.astype(str).tolist()
        ts_str_list = list(map(lambda x: [xx.lstrip('0') for xx in x.split('-')], ts_range))
        # format like: '2021/2/3'
        ts_str = ['/'.join(x) for x in ts_str_list]

        blob_start = [elv_id+'/'+t+'/' for t in ts_str]
        return blob_start


    def ls_files(self, path, recursive=False):
        '''
        List files under a path, optionally recursively
        '''
        if not path == '' and not path.endswith('/'):
            path += '/'

        blob_iter = self.client.list_blobs(name_starts_with=path)
        files = []
        for blob in blob_iter:
            relative_path = os.path.relpath(blob.name, path)
            if recursive or not '/' in relative_path:
                # files.append(relative_path)
                files.append(blob)
        return files

    def ls_dirs(self, path, recursive=False):
        '''
        List directories under a path, optionally recursively
        '''
        if not path == '' and not path.endswith('/'):
            path += '/'

        blob_iter = self.client.list_blobs(name_starts_with=path)
        dirs = []
        for blob in blob_iter:
            relative_dir = os.path.dirname(os.path.relpath(blob.name, path))
            if relative_dir and (recursive or not '/' in relative_dir) and not relative_dir in dirs:
                dirs.append(relative_dir)

        return dirs

    def download_job(self, item, dest):
        blob = item.name
        blob_name = blob.split('/')[-1]

        bc = self.client.get_blob_client(blob=blob)
        with open(dest+'/'+blob_name, 'wb') as file:
            data = bc.download_blob()
            file.write(data.readall())
            
        return blob_name

    def callback(self, blob_name):
        print('%s downloaded!'%blob_name)


# Cosmos interface
class CosmosDB(object):
    def __init__(self, cosmos_url, master_key):
        self.client = CosmosClient(cosmos_url, credential=master_key)
        self.database_name = DatabaseName
        self.database = self.client.get_database_client(self.database_name)
        self.container_name = TableName
        self.container = self.database.get_container_client(self.container_name)

    def insert_data(self, df):
        for i in range(len(df)):
            self.container.upsert_item({
                    'id': df.loc[i]['id'],
                    'UnitNumber': 'M',
                    'total': int(df.loc[i]['total']),
                    'current': int(df.loc[i]['current']),
                    'online': df.loc[i]['online'],
                    'ts': int(df.iloc[i]['ts'])
                }
            )
            
    def delete_data(self, partition_key='M'):
        for item in self.container.query_items(
                query='SELECT * FROM {0}'.format(self.container_name),
                enable_cross_partition_query=True):
            print(item['id'])
            for item1 in self.container.query_items(
                query='SELECT * FROM CameraImageStatus p where p.id="{0}"'.format(item['id']),
                enable_cross_partition_query=True):
                self.container.delete_item(item1, partition_key=partition_key)
            
    def search_by_elv(self, elv_id, partition_key='M'):
        for item in self.container.query_items(
                query='SELECT * FROM CameraImageStatus p where p.id="{0}"'.format(elv_id),
                enable_cross_partition_query=True):
            return item

    def search_elvs(self, partition_key='M'):
        elv_info = list(self.container.query_items(
                query='SELECT * FROM CameraImageStatus',
                enable_cross_partition_query=True))
        return elv_info

def download(dest, elv, begin_ts, end_ts=None):

    dest = dest+'/id_'+elv
    if not os.path.exists(dest):
        os.mkdir(dest)
    blob_client = DirectoryClient(connect_str, container_name)
    blob_paths = blob_client.get_blob_path(elv, begin_ts, end_ts)

    p_num = min(cpu_count()*2, 30)
    p = multiprocessing.Pool(processes=p_num)

    for blob_path in blob_paths:
        print(blob_path)
        blobs = blob_client.ls_files(blob_path, recursive=True)
        for blob in blobs:
            # blob_client.download_job(blob, dest)
            p.apply_async(blob_client.download_job, args=(blob, dest), callback=blob_client.callback)

    p.close()
    p.join()

def main():
    begin_ts = '2021/5/13'
    end_ts = '2020/5/18'
    # gw_id = '00000000009478'
    dest = './blob'
    elevators = ["D7SE7336"]

    print(datetime.datetime.now())

    # cosmos = CosmosDB(cosmos_url, master_key)
    for elv in elevators:
        # if gw_id!='60210117000118':
        #     continue
        print(elv)
        download(dest, elv, begin_ts)
        # if elv['ready']=='yes' and elv['online']=='no':
        #     download(dest, gw_id, begin_ts, end_ts)
    print(datetime.datetime.now())

    

if __name__ == '__main__':
    main()
