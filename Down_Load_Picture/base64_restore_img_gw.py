import os
import base64
import json
import datetime
import time


def decode(file_path, begin_date, dest_path):
	# file_name = os.path.basename(file_path).split('.')[0]
	with open(file_path, 'r') as load_f:
		try:
			load_dict = json.load(load_f)
			capture_time = load_dict['Messages'][0]['Data']['CaptureTime']
			if capture_time>=begin_date:
				unit_number = load_dict['UnitNumber']
				t = time.localtime(capture_time)
				img = base64.b64decode(load_dict['Messages'][0]['Data']['PictureData'])
				img_new_name = "img_%s-%02d_%02d_%02d--%02d_%02d_%02d.jpg"%(unit_number, t.tm_year, t.tm_mon, t.tm_mday, t.tm_hour, t.tm_min, t.tm_sec)
				img_path = dest_path + '/' + img_new_name

				with open(img_path, 'wb') as f_img:
					f_img.write(img)
		except Exception as err:
			print(file_path, err)


def decode_with_score(file_path, begin_date, dest_path):
	# file_name = os.path.basename(file_path).split('.')[0]
	with open(file_path, 'r') as load_f:
		try:
			load_dict = json.load(load_f)
			capture_time = load_dict['Messages'][0]['Data']['CaptureTime']
			if capture_time>=begin_date:
				unit_number = load_dict['UnitNumber']
				t = time.localtime(capture_time)
				score = load_dict['Messages'][0]['Data']['Score']
				img = base64.b64decode(load_dict['Messages'][0]['Data']['PictureData'])
				img_new_name = "img_%s-%02d%02d_%02d--%02d_%02d_%02d--%03d.jpg"%(unit_number, t.tm_year, t.tm_mon, t.tm_mday, t.tm_hour, t.tm_min, t.tm_sec, score)
				img_path = dest_path + '/' + img_new_name

				with open(img_path, 'wb') as f_img:
					f_img.write(img)
		except Exception as err:
			print(file_path, err)


def convert_json2img(path, begin_date, dest_path, with_score=False):
	t_8zone = datetime.datetime.strptime(begin_date, "%Y-%m-%d %H:%M:%S")
	# t_0zone = t_8zone+datetime.timedelta(hours=8)
	t_begin = int(time.mktime(t_8zone.timetuple()))

	for f in os.listdir(path):
		file_path = path+'/'+f
		print(file_path)
		if with_score:
			decode_with_score(file_path, t_begin, dest_path)
		else:
			decode(file_path, t_begin, dest_path)

def main():
	with_score = True
	path = "D://PyCharmproject/OTIS/访问图片/blob/id_D7SE7336"
	dest_path = "D://PyCharmproject/OTIS/访问图片/picture"

	begin_date = "2021-04-28 00:00:00"
	if not os.path.exists(dest_path):
		os.mkdir(dest_path)

	convert_json2img(path, begin_date, dest_path, with_score)

	# for subpath in os.listdir(path):
	# 	convert_json2img(path+'/'+subpath, begin_date, dest_path, with_score)
	

if __name__=='__main__':
	main()