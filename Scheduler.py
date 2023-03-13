
# %% Libraries

print('loading libraries')
import signal, os, subprocess, glob, sys, time, json, pandas as pd
import threading
import psutil
import sqlite3
import queue

print('loading custom libraries')
import sys
sys.path.append('../PyLib/')
import handy

print('loading setup parameters')
import setup

# %% Setup

print('setting clock')
clock = '5s'

print('creating folder')
handy.mkdir('./cache')
handy.mkdir('./cache/tasks')
handy.mkdir('./cache/log')
handy.mkdir('./cache/pid')
handy.mkdir('./cache/sts')

print('connect to database')
con = sqlite3.connect(setup.dbfile)
cur = con.cursor()
cur.execute('CREATE TABLE IF NOT EXISTS history'
            ' (timestamp INT, cycle INT, name TEXT, status TEXT)') 
cur.execute('CREATE TABLE IF NOT EXISTS latest'
            ' (timestamp INT, cycle INT, name TEXT, status TEXT, PRIMARY KEY (name))')
con.commit()

print('setup sql queue')
sqlqueue = queue.Queue()

# %% Functions

print('defining functions')

# Append to file
def append(file, txt):
    with open(file, 'a') as f:
        f.write(str(txt))
    return None

# Overwrite file
def overwrite(file, txt):
    with open(file, 'w') as f:
        f.write(str(txt))
    return None

# Write status to sql queue
def appendstatus(sqlqueue, cycle, name, status):
    
    if setup.testing: print('appendstatus:', sqlqueue, cycle, name, status)

    # Append to status table (history)
    timestamp = pd.Timestamp('now', tz = 'CET')
    query = 'INSERT INTO history VALUES (?, ?, ?, ?)'
    data = (int(timestamp.value), int(cycle.value), name, status)
    sqlqueue.put((query, data))
    
    # Insert into latest status table (current)
    query  = 'INSERT OR IGNORE INTO latest VALUES (?, ?, ?, ?)'
    data = (int(timestamp.value), int(cycle.value), name, status)
    sqlqueue.put((query, data))

    # Update latest status table (current)
    query  = 'UPDATE latest SET timestamp = ?, cycle = ?, status = ? WHERE name = ?'
    data = (int(timestamp.value), int(cycle.value), status, name)
    sqlqueue.put((query, data))


# Asynchronous execution thread
# Credits: https://eli.thegreenplace.net/2017/interacting-with-a-long-running-child-process-in-python/
def runasync(cycle, name, path, script, timeout):
    
    if setup.testing: print('runasync()')
    
    logtime = pd.Timestamp(cycle)
    logtime = pd.Timestamp.tz_convert(logtime, tz = 'CET')
    logtime = logtime.strftime('%Y%m%d.%H%M%S')
    handy.mkdir(f'./cache/log/{name}')
    logfile   = f'./cache/log/{name}/{logtime}.txt'
    stsfile   = f'./cache/sts/{name}.txt'
    pidfile   = f'./cache/pid/{name}.txt'
    
    def stsfun(proc, sqlqueue):
        overwrite(pidfile, str(proc.pid))
        
        # Write status to sql database
        appendstatus(sqlqueue, cycle, name, status = 'started')
    
        t0 = time.perf_counter()
        while proc.poll() is None:
            if (time.perf_counter() - t0) > timeout:
                break
            time.sleep(1)        
        poll = proc.poll()
        status = ''
        if poll is None:
            parent   = psutil.Process(proc.pid)
            children = parent.children(recursive = True)
            for child in children: child.kill()
            proc.kill()
            status = 'timeout'
        elif poll > 0:
            status = 'error'
        else:
            status = 'done'
        overwrite(pidfile, '-1')
        
        appendstatus(sqlqueue, cycle, name, status)
    
    def logfun(proc):
        overwrite(logfile, '')
        for line in iter(proc.stdout.readline, b''):
            append(logfile, line.decode(errors = 'ignore'))
            
    ext = script.lower().split('.')[-1]
    interpreter = handy.descent(setup.binaries, [ext], '')
    
    if interpreter == '':
        print('no interpreter found for "' + ext + '" for file "' + name + '"')

    exitstatus, logtext = 'error', ''
    try:
        proc = subprocess.Popen(args = interpreter + [script], cwd = path, stdout = subprocess.PIPE, stderr = subprocess.STDOUT)
        run_thread = threading.Thread(target = logfun, name = name, args = (proc,))
        run_thread.start()
        ctl_thread = threading.Thread(target = stsfun, name = name, args = (proc,sqlqueue))
        ctl_thread.start()
    except Exception as e:
        append(logfile, 'Scheduler: ' + str(e))
        appendstatus(sqlqueue, cycle, name, status = 'exception')


def get_tasks():
    files = glob.glob('./cache/tasks/*.txt')
    names = [os.path.basename(file).replace('.txt', '') for file in files]
    data = [json.loads(handy.saveread(file, '{}')) for file in files]
    data = pd.DataFrame(data)
    return data

def get_pid(name):
    pid = handy.saveread('./cache/pid/' + name + '.txt', fail = '-1')
    try:    pid = int(pid)
    except: pid = -1
    if not psutil.pid_exists(pid): pid = -1
    return pid


def safedelta(t):
    try:    return pd.Timedelta(t)
    except: return None


def safetime(t):
    try:    return pd.Timestamp(t)
    except: return None
    
# %% Cleanups

print('removing files')
print('cleaned pid:', len([os.remove(file) for file in handy.rls('./cache/pid/')]))

print('binding SIGINT and SIGTERM signals')
def cleanup(self, signum):
    print('cleanup')
    files = handy.rls('./cache/pid')
    pids = [handy.saveread(file, '-1') for file in files]
    rems = [os.remove(file) for file in files]
    pids = [pid for pid in pids if pid != '-1']
    for pid in pids:
        try: 
            os.kill(int(pid), signal.SIGINT)
            print('cleaned  up:', pid)
        except Exception as e:
            print(e)
    print('closing db connection')
    con.close()
    print('exiting...')
    sys.exit()

signal.signal(signal.SIGINT,  cleanup)
signal.signal(signal.SIGTERM, cleanup)


# %% Main Loop

print('entering main loop')
while True:

    # Get all current tasks
    cycle = pd.Timestamp('now', tz = 'CET').floor(clock)

    # Retrieve all current tasks
    tasks = get_tasks()

    # Safely assume columns that we need
    cols  = ['name', 'execute', 'path', 'script', 'interval', 'delay', 'enabled', 'timeout', 'status', 'lastrun']
    types = [str, str, str, str, safedelta, safedelta, bool, safedelta, str, safetime]
    tasks = handy.havecols(tasks, cols, fill = '', types = types)
    
    # Cycle
    tasks = tasks.assign(cycle   = cycle)
    tasks = tasks.assign(timeout = tasks.timeout.apply(lambda t: t.total_seconds()))
    tasks = tasks.assign(timeout = tasks.timeout.astype(int))

    # Execute interval tasks
    interval = tasks.query('execute == "interval"')
    interval = interval.assign(this_cycle = interval.cycle)
    interval = interval.assign(this_cycle = interval.this_cycle.apply(pd.Timestamp))
    interval = interval.assign(closest_cycle = interval.interval.apply(lambda interval: cycle.floor(interval)))
    interval = interval.assign(next_cycle = (interval.closest_cycle + interval.delay).apply(lambda t: t.floor(clock)))
    interval = interval.query('next_cycle == this_cycle')

    # Start continuous tasks
    continuous = tasks.query('execute == "continuous"')
    continuous = continuous.assign(pid = continuous.name.apply(get_pid))
    continuous = continuous.query('pid == -1')
    
    # Combine and start threads
    cols = ['cycle', 'name', 'path', 'script', 'timeout']
    runnow = pd.concat([interval.filter(cols), continuous.filter(cols)])
    for i, task in runnow.iterrows():
        cycle, name, path, script, timeout = task[cols].to_dict().values()
        print(f'    Running: {name}')
        runasync(cycle, name, path, script, timeout)

    # Write states to sql
    cur = con.cursor()
    while sqlqueue.qsize() > 0:
        query, data = sqlqueue.get()
        cur.execute(query, data)
    cutoff = pd.Timestamp('now', tz = 'CET').floor('1H') - pd.Timedelta('24H')
    cur.execute('DELETE FROM history WHERE cycle < ?', (int(cutoff.value),))
    con.commit()
    cur.close()

    # Check threads
    n = len(threading.enumerate())
        
    # Calculate how much we need to wait until the next cycle  
    cycle1 = pd.Timestamp(cycle) + pd.Timedelta(clock)
    sleep = max(0, (cycle1 - pd.Timestamp('now', tz = 'CET')).total_seconds())
    
    print(f'{cycle}: Sleeping for {sleep} ({n} threads)')
    sys.stdout.flush()
    time.sleep(sleep)

