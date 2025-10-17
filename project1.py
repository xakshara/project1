import csv

filename1 = "air_quality.csv"
filename2 = "uhf.csv"

'''
PART A: This function takes in a csv file, iterates through it row by row, 
then assigns the columns to 4 fields: geo_id, geo_description, date, pm_mcg
There are if statements that prevent duplicates from being entered by geo_id and date
'''
def read_air_quality(filename1):
    uhf_dict = {}
    date_dict = {}

    f = open(filename1, 'r')
    reader = csv.reader(f)

    for row in reader:
        geo_id = row[0].strip()
        geo_description = row[1].strip()
        date = row[2].strip()
        pm_mcg = row[3].strip()

    measurement = (geo_id, geo_description, date, pm_mcg)

    if geo_id not in uhf_dict:
        uhf_dict[geo_id] = [] 
    uhf_dict[geo_id].append(measurement)
    
    if date not in date_dict:
        date_dict[date] = []
    date_dict[date].append(measurement)

    f.close()
    
    return uhf_dict, date_dict

# print(read_air_quality("air_quality.csv"))

'''
PART B: This function reads the uhf.csv file. The function reads through 
this file, sorts the columns into borough, uhf_code, geo_id, zip_list,
then returns dictionaries that show zip codes to uhf ids and also
borough to uhf ids
'''
def read_uhf(filename2):
    zip_to_uhf = {}
    borough_to_uhf = {}

    f = open(filename2, 'r')
    reader = csv.reader(f)

    for row in reader:
        borough = row[0].strip()
        uhf_code = row[1].strip()
        geo_id = row[2].strip()
        zip_list = [z.strip()for z in row[3:] if z.strip() != ""]

        if borough not in borough_to_uhf:
            borough_to_uhf[borough] = []
        if geo_id not in borough_to_uhf[borough]:
            borough_to_uhf[borough].append(geo_id)

        for z in zip_list:
                if z not in zip_to_uhf:
                    zip_to_uhf[z] = []
                if geo_id not in zip_to_uhf[z]:
                    zip_to_uhf[z].append(geo_id)
    
    return zip_to_uhf, borough_to_uhf

# print(read_uhf("uhf.csv"))

'''
PART C: This function asks the user to if they want to search data
by zip code, UHF id, borough, or date. Then, the function uses 
the past dictionaries to find the info for the user.

MAKE SURE: return statement w/ this format: 
6/1/09 UHF 205 Sunset Park 11.45 mcg/m^3
'''




