import json
import os
from mpi4py import MPI

FILEPATH = 'bigInstagram.json'

comm = MPI.COMM_WORLD
comm_size = comm.Get_size()
rank = comm.Get_rank()

if rank == 0 :
    # Read melbGrid
    areas = {}
    with open('melbGrid.json', 'r') as f:
        data = json.load(f)
        areas = data["features"]
    melbGrid = {}
    for area in areas:
        melbGrid[area["properties"]["id"]] = [area["properties"]["xmin"], area["properties"]["xmax"],
                                              area["properties"]["ymin"], area["properties"]["ymax"]]

    # Calculate rough start position by dividing the file size by comm size
    FILE_SIZE = os.path.getsize(FILEPATH) # Read file size
    rough_start = []
    for r in range (comm_size):
        rough_start.append(r*FILE_SIZE/comm_size)
    start = [0]*(comm_size+1)
    file = open(FILEPATH)
    start[0] = len(file.readline())  # Skip the first line

    # Find the actual start position by moving the file reading pointer to the next line
    for r in range(comm_size - 1):
        file.seek(rough_start[r + 1])
        line_break_position = rough_start[r + 1] + len(file.readline()) # Find the next line break position
        start[r + 1] = line_break_position
    start[-1] = FILE_SIZE

    file.close()

else:
    melbGrid = None
    start = None

# Boradcast grid and all starting position
melbGrid = comm.bcast(melbGrid, root=0)
start = comm.bcast(start, root=0)

file = open(FILEPATH)
file.seek(start[rank]) # Move the file reading pointer to starting position
ins_data = []
melbGrid_ins_count={}

# Read file by lines and load JSON data
while True:
    line  = file.readline().strip("\n").strip()
    # Skip the last line
    if line ==']}':
        break

    if len(line) == 0:
        continue

    # Remove the comma
    if line[-1] == ",":
        line = line[:-1]
    try:
        ins_dict = json.loads(line)

        # Map the Instagram JSON to blocks in the grid
        if ins_dict["doc"].has_key("coordinates"):
            coordinate_y, coordinate_x = ins_dict["doc"]["coordinates"]["coordinates"]
            for area in melbGrid:
                if coordinate_x >= melbGrid[area][0] and coordinate_x < melbGrid[area][1] and coordinate_y >=  \
                        melbGrid[area][2] and coordinate_y < melbGrid[area][3]:
                    melbGrid_ins_count[area] = melbGrid_ins_count.get(area, 0) + 1
                    break
    except:
        pass

    if file.tell() >= start[rank+1]:
        break
file.close()

# Gather all results
send_data = melbGrid_ins_count
recv_data = comm.gather(send_data, root=0)
if rank == 0:

    # Sort in blocks
    melbGrid_ins_total_count = {}
    for key in melbGrid.keys():
        melbGrid_ins_total_count[key] = 0
    for count_dict in recv_data:
        for key,value in count_dict.items():
            melbGrid_ins_total_count[key] = melbGrid_ins_total_count.get(key,0) +value
    print ("Order of the Grid boxes based on the total number of Instagram posts")
    for key,value in sorted(melbGrid_ins_total_count.items(), lambda x, y: cmp(x[1], y[1]),reverse=True):
        print str(key) +": "+ str(value) +" posts"
    print

    # Sort in rows
    row_ins_total_count = {}
    for key,value in melbGrid_ins_total_count.items():
        row_ins_total_count[str(key[0]).upper()] = row_ins_total_count.get(str(key[0]).upper(),0) + value
    print ("Order of the rows based on the total number of Instagram posts")
    for key,value in sorted(row_ins_total_count.items(), lambda x, y: cmp(x[1], y[1]), reverse=True):
        print key +"-Row: "+ str(value) +" posts"
    print

    # Sort in columns
    column_ins_total_count = {}
    for key,value in melbGrid_ins_total_count.items():
        column_ins_total_count[str(key[1])] = column_ins_total_count.get(str(key[1]),0) + value
    print ("Order of the columns based on the total number of Instagram posts")
    for key, value in sorted(column_ins_total_count.items(), lambda x, y: cmp(x[1], y[1]), reverse=True):
        print "Column "+key + ": " + str(value) + " posts"
