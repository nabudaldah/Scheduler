
print('cleaning log files...')

import os
import pandas as pd
from glob import glob 

data = pd.DataFrame()
data = data.assign(file  = glob('./cache/log/*/*.txt', recursive = True))
data = data.assign(mtime = data.file.apply(os.path.getmtime))
data = data.assign(mtime = data.mtime.apply(pd.Timestamp, unit = 's', tz = 'CET'))

time0 = pd.Timestamp('now', tz = 'CET').floor('1min') - pd.Timedelta('24H')

data = data.assign(old = data.mtime < time0)

remove = data.query('old == True')

print(f'found {len(data)} log files and deleting {len(remove)} old log files')

def saferemove(file):
    if os.path.isfile(file): os.remove(file)
    else: print(f'warning: could not remove file {file}')

remove.file.apply(saferemove)

print('all done')
