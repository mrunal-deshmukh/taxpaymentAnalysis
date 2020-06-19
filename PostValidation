import csv
import time
start_time = time.time()
path = "/Users/kloganathan/Desktop/Desktop_2/ONSITE/Documents/Mounika/Study/scripts/merged-1.csv"


#To validate the file size to be equal or greater than 10GB
def file_size(fname):
        import os
        statinfo = os.stat(fname)
        return statinfo.st_size
if(round(file_size(path)/(1024*1024*1024))>=10):
    print("Validation 1 - File size is validated and is equal to or greater than 10GB")
else:
    print("Validation 1 - File size is not 10GB")

#To validate if the file is sorted
HouseHoldRows = []
StateRows = []
with open(path, 'r') as csvfile:
    csvreader = csv.reader(csvfile)
    for row in csvreader:
        StateRows.append(row[0])
        HouseHoldRows.append(row[5])
    flag = 0
    if(all(StateRows[i] <= StateRows[i + 1] for i in range(len(StateRows)-1))):
        flag = 1
    if (flag) :
        print ("Validation 2 - Yes, file is sorted.")
    else :
        print ("Validation 2 - No, file is not sorted.")

#To validate the range of values (to verify the number of households in the city
# are not over populated)
    notOverPopulated = True
    for i in HouseHoldRows:
        if (not (0 < int(i) < 25000)):
            notOverPopulated = False
            print ("Validation 3 - Number of Households in the pincode are over populated")
            break;
    if (len(HouseHoldRows) > 0 and notOverPopulated == True):
       print ("Validation 3 - Number of Households in the pincode are not over populated")
print("--- %s seconds ---" % (time.time() - start_time))
csvfile.close()
