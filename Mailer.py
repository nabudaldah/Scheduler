
print('emailing people about errors and timeouts ...')

# General libs
import os
import sqlite3
import pandas as pd
import json

# Custom libs
import sys
sys.path.append('../PyLib/')
from sendmail import sendmail
import handy

# Libs for project
import setup

# Setup

# Max number of bytes to send of logfile
maxbytes = int(2 * 1024 * 1024) # 2 megabyte

time0 = pd.Timestamp('now', tz = 'CET') - pd.Timedelta('24H')
emailed_file = './cache/emailed.P'
if not os.path.exists(emailed_file): # os.remove(emailed_file)
    emailed = pd.DataFrame({'name': ['dummy'], 'cycle': [time0], 'emailed': [True]})
    emailed.to_pickle(emailed_file)
emailed = pd.read_pickle(emailed_file)
emailed = emailed.assign(cycle = pd.to_datetime(emailed.cycle))
emailed = emailed.query(f'cycle >= "{time0}"')
emailed = emailed.assign(emailed = True)

notified = emailed.filter(['name'])
notified = notified.drop_duplicates()
notified = notified.assign(notified = True)

# Get latest status
print(f'loading latest status')
con = sqlite3.connect(setup.dbfile)
cur = con.cursor()
status = cur.execute('SELECT timestamp, cycle, name, status FROM latest').fetchall()
status = pd.DataFrame(status, columns = ['timestamp', 'cycle', 'name', 'status'])
status = status.assign(timestamp = pd.to_datetime(status.timestamp).dt.tz_localize('UTC').dt.tz_convert('CET'))
status = status.assign(cycle     = pd.to_datetime(status.cycle)    .dt.tz_localize('UTC').dt.tz_convert('CET'))
status = status.query('cycle >= @time0')
cur.close()
con.close()

print(f'loading tasks')
def safeload(text):
    try: return json.loads(text)
    except: return {}
tasks = [handy.saferead(f'./cache/tasks/{name}.txt') for name in status['name']]
tasks = [safeload(task) for task in tasks]
tasks = pd.DataFrame(tasks)
tasks = tasks.dropna(how = 'all')

tasks = tasks.merge(status,   how = 'left', on = ['name'])
tasks = tasks.merge(emailed,  how = 'left', on = ['name', 'cycle'])
tasks = tasks.assign(emailed = pd.np.where(tasks.emailed.isna(), False, tasks.emailed))
tasks = tasks.merge(notified, how = 'left', on = ['name'])
tasks = tasks.assign(notified = pd.np.where(tasks.notified.isna(), False, tasks.notified))


tasks = tasks.query('not (notify  == "once" & notified == True)')
tasks = tasks.query('notify  != "none"')
tasks = tasks.query('status  != "disabled"')
tasks = tasks.query('status  == "error"')
tasks = tasks.query('emailed == False')

def make_link(path, script):
    if not setup.jupyter_folder in path: return setup.jupyter_url
    relpath = path[(len(setup.jupyter_folder) + 1):]
    relpath = relpath.replace('\\', '/')
    link = f'{setup.jupyter_url}/{relpath}/{script}'
    return link

print(f'we have {tasks.shape[0]} errors to report')

for i, task in tasks.iterrows():
    print('emailing:', task["name"])
    recipients   = task.email
    subject      = f'Scheduler Failure: {task["name"]} ({task.cycle})'
    task['link'] = make_link(task.path, task.script)
    body         = '\n'.join([f'{k[:6]}:\t{v}' for k, v in task.iteritems()])
    cycle        = task.cycle.strftime('%Y%m%d.%H%M%S')
    logfile      = f'./cache/log/{task["name"]}/{cycle}.txt'
    attachments  = [str(handy.read_log(logfile, nchars = maxbytes))]
    try:    sendmail(recipients, subject, body, attachments)
    except: print(f'Could not send email to {recipients} ...')

# Keep track of emails of last 24h
toemail = tasks.filter(['name', 'cycle', 'emailed'])
toemail = toemail.assign(emailed = True)
emailed = emailed.append(toemail)
emailed.to_pickle(emailed_file)

print('all done')
