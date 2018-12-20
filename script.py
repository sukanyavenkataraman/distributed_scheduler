import csv

with open('dump.txt', 'r') as in_file:
    stripped = [line.strip() for line in in_file]
    res = [['Tasks', 'PGs', 'Skewed', 'TaskLength', 'Local Reserver Algo', 'Remote Reserver Algo', 'Avg Response Time']]
    for i in range(0,len(stripped),28):
        
        for j in range(i+1,i+28,3):
            r = stripped[i].split()
            r.extend(stripped[j].split())
            r.append(stripped[j+2].split(':')[-1])
            res.append(r)
            r = ['','','','','','','']
        res.append([])

for row in res:
    if row:
        if row[4] == 'loaddist':
            row[4] = 'Load Balanced'
        elif row[4] == 'uniform':
            row[4] = 'Uniform'
        else:
            row[4] = 'Current'

        if row[5] == 'distributed':
            row[5] = 'Load Balanced'
        elif row[5] == 'same_as_prev':
            row[5] = 'Network Efficient'
        else:
            row[5] = 'Current'
    
with open("refined_dataset.csv","w+") as my_csv:
    csvWriter = csv.writer(my_csv,delimiter=',',lineterminator='\n')
    csvWriter.writerows(res)
