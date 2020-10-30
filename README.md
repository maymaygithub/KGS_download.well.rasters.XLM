# KGS_download.well.rasters.XLM
KGS_download.well.rasters.XLM


import os, sys
from urllib import request, parse
from bs4 import BeautifulSoup
import math
from zipfile import ZipFile
# 

KGS_download_path = "D:\Kansas OG Database Logs\KGS_Finney County Zipped Raster"
unzipped_path = "D:\Kansas OG Database Logs\KGS_Finney County Raster"
root_url = "https://chasm.kgs.ku.edu/ords/elog.escnd6.SelectWells"

desired_state = 15
desired_county = 55 #KGS county code  

def request_soup(url,data=None):
    req =  request.Request(url, data=data)
    resp = request.urlopen(req)
    soup = BeautifulSoup(resp,features="lxml")
    return soup

if not os.path.exists(KGS_download_path):   
    os.makedirs(KGS_download_path + "/")   #如果KGS_download_path的資料夾在電腦裡不存在那就創這個資料夾
if not os.path.exists(unzipped_path):   
    os.makedirs(unzipped_path + "/")
'''
# 1. Downaload All SelectWells from all the counties from Kansas State
for i in range(1,211, 2):
    # well_filename = f"{KGS_download_path}/well{i}.html"
    # if os.path.exists(well_filename):
        # print(f"skip {well_filename}")
        # continue
'''
data = parse.urlencode({               #???
    "ew":"W",
    "f_st":desired_state,
    "f_c": desired_county,   #edited
    "f_pg": 1
    }).encode()

    # 2. Calculate the pages from total records divide by 50
soup = request_soup(root_url, data)
total_record_num = soup.find("hr").next_element.split(" ")[0] 
total_pages = math.ceil(int(total_record_num)/50)

print(f"Collecting county:{desired_county} info... total_record_num: {total_record_num}, total_pages: {total_pages}")
    #39-49 每個頁面中 table架構讀出來 
for p in range(1, total_pages + 1):
    print(f"**Processing county:{desired_county}, page:{p}/{total_pages}")
    data = parse.urlencode({
        "ew":"W",
        "f_st":desired_state,
        "f_c": desired_county,   #edited
        "f_pg": p
        }).encode()
    soup = request_soup(root_url, data)
    table = soup.find_all('table')[1]   #??? 1代表啥 位移1的表嗎?
    tr_all = table.find_all("tr",attrs={"valign": "top"})   #<tr valign="top"> #??? 為何是冒號空格來表示
    tr_index = 0                 #50-56 表格中的每一欄的井的超連結資料
    for tr in tr_all:
        tr_index = tr_index + 1
        link = tr.find_all("a")[0].get('href')    #??? 位移=0的a媽
        print(f"*Processing item{tr_index}/{50} {link} in county:{desired_county}, page:{p}/{total_pages}")
        soup = request_soup(link)
        #第一個表裡面抓取API 等下檔案命名用
        #API_t = soup.find("table", attrs={"align":"center", "border":"1"})
      #  print(API_t)
        API = soup.find("strong", text = "API: ").next_element.next_element.split('\n')[0]
        print("API =" + API)
        fixed_API = ''.join(API.split("-"))
        welline_log_t = soup.find("table",attrs={"align":"center", "width": "680"})  #<table aligh="center" width="680">   #每一個井資料頁面 把裡面結構抓出來
        h3_all = welline_log_t.find_all("h3")                                #57-60 找到wireline log header data
        for h3 in h3_all:                                    #???不能直接找到Wireline Log Header Data那個table(特定attribute)嗎??\
            if h3.text == "Wireline Log Header Data":            #找要得table title
                table_row_all = h3.findNext("table").find_all("tr",attrs={"valign": "top"})
                for tr in table_row_all: #tr = each row of the table
                   ## li_all = h3.findNext("table").find_all("li")  #li=網頁上的"Download Black and White Scan "的超連結的結構位置
                   ## for li in li_all:
                    if tr.find("a") != None:
                        wire_log_data_link = tr.find("a").get("href")
                        wire_log_data_filename = wire_log_data_link.split('/')[-1]
                        wire_log_path = f"{KGS_download_path}/{wire_log_data_filename}"
                        if os.path.exists(wire_log_path):
                                print(f"Skip {wire_log_path}") #即使python會自動跳過以下載的檔案 還是寫出來
                                continue     #??這功能是啥
                        print(f"Downloading: {wire_log_data_link}")
                        request.urlretrieve(wire_log_data_link,wire_log_path)   #下載並存成wire_log_data_filename黨名 這個函式需要給定檔案路徑跟名子
                        tool_filename = tr.find("strong", text = "Tool:").next_element.next_element.split('\n')[0].split('  ')[0]
                        tool_filename = tool_filename.replace("/","-")
                       #解壓縮:
                        with ZipFile(wire_log_path, 'r') as zipObj:
                            unzipped_filename = zipObj.namelist()[0]
                            print(unzipped_filename)
                            zipObj.extractall(unzipped_path + "/")

                        #zipObj.extractall(unzipped_path + "/" + API + "_" + tool_filename)

#                            if os.path.exists( f"{unzipped_path}/{wire_log_data_link.split('/')[-1]}.tif"):
#                                print(f"EXIST:{unzipped_path}/{wire_log_data_link.split('/')[-1]}.tif") #即使python會自動跳過以下載的檔案 還是寫出來
#                            continue 
                            #unzipped_filename = wire_log_data_filename.split('.')[0]
                            existfile = f"{unzipped_path}/{unzipped_filename}"
                            newfile= f"{unzipped_path}/{fixed_API}_{tool_filename}_{unzipped_filename}"
                            print('Changing name into ' + newfile + '\n')
                            os.rename(existfile, newfile)
