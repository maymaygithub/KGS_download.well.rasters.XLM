# KGS_download.well.rasters.XLM
# For download well raster (.XLM) files on Kansas Geological Survey: https://chasm.kgs.ku.edu/ords/elog.escnd6.SelectWells 


import os
from urllib import request, parse
from bs4 import BeautifulSoup
import math
from zipfile import ZipFile


KGS_download_path = "D:\Kansas OG Database Logs\KGS_Ness County Zipped Raster" #please change the folder name to the desired county name
unzipped_path = "D:\Kansas OG Database Logs\KGS_Ness County Raster"     #please change the folder name to the desired county name
root_url = "https://chasm.kgs.ku.edu/ords/elog.escnd6.SelectWells"   #The link where filterred well data have scanned rasters (.XLM) files

#desired_township =
#desired_range =
#disired_east_west =    #input "W" or "E" only
#desired_section = 
desired_state = 15  #Kansas sate code = 15, do not change
desired_county = 135 #please change the desited county code  

def request_soup(url,data=None):
    req =  request.Request(url, data=data)
    resp = request.urlopen(req)
    soup = BeautifulSoup(resp,features="lxml")
    return soup

if not os.path.exists(KGS_download_path):   
    os.makedirs(KGS_download_path + "/")   #if the folder of KGS_download_path does not exist, create one
if not os.path.exists(unzipped_path):   
    os.makedirs(unzipped_path + "/")
'''
# Additional Function: Downaload every well from all the counties from Kansas State
for county_codes in range(1, 209 + 2, 2):       #county_codes are 1, 3 ... ,209. odd numbers
    well_filename = f"{KGS_download_path}/well{county_codes}.html"
    if os.path.exists(well_filename):
        print(f"skip {well_filename}") 
        continue
'''

# Calculate total pages from total record amount, shown on page 1, and divide by 50 wells displayed in one page
data = parse.urlencode({               
    #"ew": disried_east_west,
    #"f_t": disired_township,
    #"f_r": disired_range,
    #"f_s": desired_section
    "f_st":desired_state,
    "f_c": desired_county,   #please use county_codes instead of desired_county if wish to download all the counties and adjust the code format
    "f_pg": 1
    }).encode()
soup = request_soup(root_url, data)
total_record_num = soup.find("hr").next_element.split(" ")[0] 
total_pages = math.ceil(int(total_record_num)/50)   
print(f"Collecting info of county:{desired_county} ..., total record number: {total_record_num}, total pages: {total_pages}")


# In each page, read the table's structure 
for page in range(1, total_pages + 1):
    print(f"** Processing county:{desired_county}, page:{page}/{total_pages}")
    data = parse.urlencode({
       #"ew": disried_east_west,
       #"f_t": disired_township,
       #"f_r": disired_range,
       #"f_s": desired_section,
        "f_st":desired_state,
        "f_c": desired_county,   #edited
        "f_pg": page
        }).encode()
    soup = request_soup(root_url, data)
    table = soup.find_all('table')[1]   #skip table 0, start from table 1
    tr_all = table.find_all("tr",attrs={"valign": "top"})   #<tr valign="top">
    tr_index = 0                 
    for tr in tr_all:
        tr_index = tr_index + 1
        link = tr.find_all("a")[0].get('href')    #start from the first one, a position 0.
        print(f"** Processing well:{tr_index}/{50} {link} in county:{desired_county}, page:{page}/{total_pages}")
        soup = request_soup(link)
        
# Grab API of the well for renaming the file later
        API = soup.find("strong", text = "API: ").next_element.next_element.split('\n')[0]
        print("API =" + API + '\n')
        fixed_API = ''.join(API.split("-"))
        
# In each well page, read the table's structure
        welline_log_t = soup.find("table",attrs={"align":"center", "width": "680"})  #<table aligh="center" width="680">  
        h3_all = welline_log_t.find_all("h3")           #h3 = table header 
        for h3 in h3_all:                                    
            if h3.text == "Wireline Log Header Data":            #find the desired table header = Wireline log Header Data
                table_row_all = h3.findNext("table").find_all("tr",attrs={"valign": "top"})

# Download raster file
                for tr in table_row_all:               #tr = each row of the table
                   ## li_all = h3.findNext("table").find_all("li")  #li is the where the "Download Black and White Scan" link located
                   ## for li in li_all:
                    if tr.find("a") != None:
                        wire_log_data_link = tr.find("a").get("href")
                        wire_log_data_filename = wire_log_data_link.split('/')[-1]
                        wire_log_path = f"{KGS_download_path}/{wire_log_data_filename}"
                        if os.path.exists(wire_log_path):
                                print(f"Skip {wire_log_path}") #show the auto-skipped file path
                                continue     
                        print(f"- Downloading: {wire_log_data_link}")
                        request.urlretrieve(wire_log_data_link,wire_log_path)   #download and save to the path (with file name). this function needs the name in the path
                        tool_filename = tr.find("strong", text = "Tool:").next_element.next_element.split('\n')[0].split('  ')[0]
                        tool_filename = tool_filename.replace("/","-")
                        
# Unzip the downloaded file
                        with ZipFile(wire_log_path, 'r') as zipObj: #'r' to read an existing file
                            unzipped_filename = zipObj.namelist()[0]
                            print(unzipped_filename)
                            zipObj.extractall(unzipped_path + "/")  
                            '''extractall() Note: if a member filename is an absolute path, a drive/UNC sharepoint and leading (back)slashes will be stripped, 
                            e.g. C:\foo\bar becomes foo\bar on Windows. '''
                            
# Rename the file
                            existfile = f"{unzipped_path}/{unzipped_filename}"
                            newfile= f"{unzipped_path}/{fixed_API}_{tool_filename}_{unzipped_filename}"
                            print('- Changing name/path into ' + newfile + '\n')
                            os.rename(existfile, newfile)
