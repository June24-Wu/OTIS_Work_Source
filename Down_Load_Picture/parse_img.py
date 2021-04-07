import os
import base64
import json
import datetime
import time
import argparse

def parse_args():
	parser = argparse.ArgumentParser(
		description='base64 decode for images')
	parser.add_argument('--path', '-p',
						help='the path of source json file')
	parser.add_argument('-sn',
						default="9A79",
						help='sn_flag, one of [9478,947C,9A79,EB9B]')
	parser.add_argument('--score', '-s',
						action="store_true",
						help='with score')
	args = parser.parse_args()
	return args

def decode(file_path, begin_date, dest_path):
	# file_name = os.path.basename(file_path).split('.')[0]
	with open(file_path, 'r') as load_f:
		try:
			load_dict = json.load(load_f)
			if load_dict['StorageTime']>=begin_date:
				sn_flag = load_dict['sn']
				t = time.localtime(load_dict['StorageTime'])
				
				img = base64.b64decode(load_dict['data'])
				img_new_name = "img-%02d_%02d--%02d_%02d_%02d.jpg"%(t.tm_mon, t.tm_mday, t.tm_hour, t.tm_min, t.tm_sec)
				img_path = dest_path + '/' + img_new_name

				with open(img_path, 'wb') as f_img:
					f_img.write(img)
		except Exception as err:
			print(err)


def decode_with_score(file_path, begin_date, dest_path):
	# file_name = os.path.basename(file_path).split('.')[0]
	with open(file_path, 'r') as load_f:
		try:
			load_dict = json.load(load_f)
			if load_dict['StorageTime']>=begin_date:
				sn_flag = load_dict['sn']
				t = time.localtime(load_dict['StorageTime'])
				score = load_dict['score']
				# print(t)
				# raise
				
				img = base64.b64decode(load_dict['data'])
				img_new_name = "img-%02d_%02d--%02d_%02d_%02d--%03d.jpg"%(t.tm_mon, t.tm_mday, t.tm_hour, t.tm_min, t.tm_sec, score)
				img_path = dest_path + '/' + img_new_name

				with open(img_path, 'wb') as f_img:
					f_img.write(img)
		except Exception as err:
			print(err)


def convert_json2img(path, dest_path, begin_date, with_score=False):
    t_8zone = datetime.datetime.strptime(begin_date, "%Y-%m-%d %H:%M:%S")
    # t_0zone = t_8zone+datetime.timedelta(hours=8)
    t_begin = int(time.mktime(t_8zone.timetuple()))

    for f in os.listdir(path):
        # for f in subpath:
        file_path = path+'/'+f
        if with_score:
            decode_with_score(file_path, t_begin, dest_path)
        else:
            decode(file_path, t_begin, dest_path)

def main():
	args = parse_args()
	# with_score = False
	path = "./blog/gw_EB9B"
		   #+ str(args.path)
	subpath = args.sn  # 9478,947C,9A79,EB9B
	# dest_path = "../../blob_raw/dest/01_07_xujiahui2"
	dest_path = "./picture"

    # begin_date = "2020-01-01 00:00:00"
	begin_date = "2021-01-07 00:00:00"
	if not os.path.exists(dest_path):
		os.mkdir(dest_path)
	if not os.path.exists(dest_path+'/'+subpath):
		os.mkdir(dest_path+'/'+subpath)

	convert_json2img(path, dest_path, begin_date, args.score)
	

if __name__=='__main__':
	main()