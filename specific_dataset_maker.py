
import urllib.request
import random

month = '04'
year = '2014'
i = 0


#while i <= 5:

date = year + '-' + month

try:
    href = 'https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_' + date + '.csv'

except ValueError:
    print('No data found')

file = open('sampleData/sample' + str(i) + '.csv','w')
data = urllib.request.urlopen(href)
counter = 0

for line in data:
    line = line.decode()
    line = line.strip()
    file.write(line)
    if counter == 25:
        break
    counter = counter + 1

#3i += 1