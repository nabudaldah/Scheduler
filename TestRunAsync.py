
import psutil

cycle = pd.Timestamp('now', tz = 'CET').floor(clock)
name = 'TestApp'
path = r'C:\Projects\Project001'
script = 'TestApp.R'
timeout = 10
debug = True

def runasync(cycle, name, path, script, timeout, debug = False):

    logtime = pd.Timestamp(cycle, tz = 'CET').strftime('%Y%m%d.%H%M%S')
    handy.mkdir(f'./cache/log/{name}')
    logfile   = f'./cache/log/{name}/{logtime}.txt'
    stsfile   = f'./cache/sts/{name}.txt'
    pidfile   = f'./cache/pid/{name}.txt'
    
    if debug:
        print('logfile', logfile)
        print('stsfile', stsfile)
        print('pidfile', pidfile)
    
    def stsfun(proc):
        if debug: print('stsfun(): pid = ', proc.pid)
        if debug: print('stsfun(): overwrite pidfile')
        overwrite(pidfile, str(proc.pid))
        timestamp = pd.Timestamp('now', tz = 'CET')
        if debug: print('stsfun(): append stsfile')
        append(stsfile,  f'{timestamp},{cycle},started,{name}\n')
        t0 = time.clock()
        while proc.poll() is None:
            if debug: print('stsfun(): poll() ...')
            if (time.clock() - t0) > timeout:
                if debug: print('stsfun(): timeout break!')
                break
            time.sleep(5)
        poll = proc.poll()
        status = ''
        if poll is None:
            if debug: print('stsfun(): kill')
            # Kill children
            parent   = psutil.Process(proc.pid)
            children = parent.children(recursive = True)
            for child in children: child.kill()
            proc.kill()
            status = 'timeout'
        elif poll > 0:
            if debug: print('stsfun(): error')
            status = 'error'
        else:
            if debug: print('stsfun(): done')
            status = 'done'
        if debug: print('stsfun(): overwrite pidfile')
        overwrite(pidfile, '-1')
        timestamp = pd.Timestamp('now', tz = 'CET')
        if debug: print('stsfun(): append stsfile')
        append(stsfile,  f'{timestamp},{cycle},{status},{name}\n')
    
    def logfun(proc):
        if debug: print('logfun()')
        if debug: print('logfun() overwrite logfile')
        overwrite(logfile, '')
        for line in iter(proc.stdout.readline, b''):
            linestr = line.decode(errors = 'ignore')
            #if debug: print('logfun() append logfile:', linestr)
            append(logfile, linestr)
            
    ext = script.lower().split('.')[-1]
    interpreter = handy.descent(setup.binaries, [ext], '')
    
    interpreter = [r'C:\Apps\R-3.4.1\bin\Rscript.exe'] + ['--vanilla']
    exitstatus, logtext = 'error', ''
    try:
        if debug: print('runasync(): Popen()')
        proc = subprocess.Popen(args = interpreter + [script], cwd = path, stdout = subprocess.PIPE, stderr = subprocess.STDOUT)
        if debug: print('runasync(): log thread')
        run_thread = threading.Thread(target = logfun, name = name, args = (proc,))
        run_thread.start()
        if debug: print('runasync(): sts thread')
        ctl_thread = threading.Thread(target = stsfun, name = name, args = (proc,))
        ctl_thread.start()
    except Exception as e:
        if debug: print('runasync(): Exception', e)
        if debug: print('runasync(): append logfile')
        append(logfile, str(e))
        timestamp = pd.Timestamp('now', tz = 'CET')
        if debug: print('runasync(): append stsfile')
        append(stsfile,  f'{timestamp},{cycle},exception,{name}\n')

