import os, uuid
import time, datetime
import pandas as pd
import multiprocessing
from multiprocessing import cpu_count
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__

container_name=""
connect_str = ""

# dest = "./data"

class DirectoryClient:
    def __init__(self, connect_str, container_name):
        service_client = BlobServiceClient.from_connection_string(connect_str)
        self.client = service_client.get_container_client(container_name)


    def get_blob_path(self, gw_id, begin_ts='2021/2/10', end_ts=None):
        if not end_ts:
            end_ts = datetime.datetime.now()
        # format like: '2021-02-03'
        ts_range = pd.period_range(begin_ts, end_ts).values.astype(str).tolist()
        ts_str_list = list(map(lambda x: [xx.lstrip('0') for xx in x.split('-')], ts_range))
        # format like: '2021/2/3'
        ts_str = ['/'.join(x) for x in ts_str_list]

        blob_start = [gw_id+'/'+t+'/' for t in ts_str]
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

# def get_dbfs():
#     pass

def main():
    begin_ts = '2021/3/23'
    end_ts = '2021/3/24'
    gw_id = '0000000000EB9B'#'60210117000118'
    dest = './blog'
    dest = dest+'/gw_'+gw_id[-4:]
    if not os.path.exists(dest):
        os.mkdir(dest)
    blob_client = DirectoryClient(connect_str, container_name)
    # dirs = blob_client.ls_dirs('', recursive=False)
    # print(dirs)
    blob_paths = blob_client.get_blob_path(gw_id, begin_ts, end_ts)

    print(datetime.datetime.now())
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

    print(datetime.datetime.now())

if __name__ == '__main__':
    main()
