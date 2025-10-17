import csv

filename = "air_quality.csv"

'''
This function takes in a csv file, iterates through it row by row, 
then assigns the columns to 4 fields: geo_id, geo_description, date, pm_mcg
There are if statements that prevent duplicates from being entered by geo_id and date
'''
def data_to_fields(filename):
    uhf_dict = {}
    date_dict = {}

    f = open(filename, 'r')
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
