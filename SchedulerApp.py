
# %% Libraries

# Libs
#import waitress

# Local
import setup

# Custom
import sys
sys.path.append('../PyLib/')
import handy
import dashboard

# Dashboard
from dashboard import body, box, btn, btn2, clock, datatable, dcc, dropdown, form, formitem, go, hidden, html, Input
from dashboard import make_datatable, make_options, menu, menuitem
from dashboard import on, onclick, onplotclick, onrows, ontick
from dashboard import page, plotly, row, rowsof
from dashboard import setcontent, setdatatable, setplot, setvalue, setoptions, setlink, textarea, textinput, valueof

# Standard
import pandas as pd
import os
import re
from glob import glob
import json
import signal
import waitress
import psutil
import sqlite3


# %% Setup

dashboard.debug = setup.testing

plchldr = {
    'name':     'name: MyProject_MyScript',
    'path':    r'path: C:\Python\MyProject',
    'script':   'script: MyScript.py',
    'execute':  'execute: interval (scripts), continuous (apps)',
    'interval': 'interval: 30 sec, 5 min, 2 hour',
    'delay':    'delay: 30 sec, 5 min, 2 hour',
    'timeout':  'timeout: 30 sec, 5 min, 2 hour',
    'email':    'email: mailbox@email.com',
    'notify':   'notify: all, once or none'
}

# %% App Layout

app = dashboard.app(users = setup.users, title = setup.title)

def make_task(task = {}):

    default = {'name': '', 'path': '', 'script': '', 'execute': '', 'interval': '', 'delay': '', 'timeout': '', 'email': '', 'notify': ''}
    
    for key in default.keys():
        if not key in task:
            task[key] = default[key]

    notify_opts = make_options(['all', 'once', 'none'])
            
    content = [
        
        formitem(None, textinput(id = 'name',   value = task['name'],   placeholder = plchldr['name']),     width = 12),
        formitem(None, textinput(id = 'email',  value = task['email'],  placeholder = plchldr['email']),    width = 8),
        formitem(None, dropdown(id = 'notify', value = task['notify'],  placeholder = plchldr['notify'], options = notify_opts),    width = 4),

        formitem(None, textinput(id = 'path',   value = task['path'],   placeholder = plchldr['path']),     width = 12),
        formitem(None, textinput(id = 'script', value = task['script'], placeholder = plchldr['script']),   width = 8),
        
        formitem(None, html.A(id = 'link', href = '/', target = '_blank', children = btn2(id = None, name = 'JupyterLab', color = 'success')), width = 4),
        
        formitem(None, dropdown (id = 'execute',  value = task['execute'],  placeholder = plchldr['execute'],   options = make_options(['interval', 'continuous', 'disabled'])), width = 12),
        formitem(None, textinput(id = 'interval', value = task['interval'], placeholder = plchldr['interval']), width = 4),
        formitem(None, textinput(id = 'delay',    value = task['delay'],    placeholder = plchldr['delay']),    width = 4),
        formitem(None, textinput(id = 'timeout',  value = task['timeout'],  placeholder = plchldr['timeout']),  width = 4),
                
    ]
    return content


app.layout = page(

    # Title (top of dashboard)
    title = setup.title,

    # Menu (left)
    menu = None,

    # Body (right)
    body = body([

        # Plot states
        row([
                         
            box(None, 3, content = [
                
                #datatable(id = 'tasks', max_rows_in_viewport = 3),
                form(id = '', content = [
                    formitem(None, dropdown(id = 'tasks')),
                    formitem(None, btn('refresh_tasks', 'Refresh')),
                ]),
                
                html.Hr(),
                
                form(id = 'task', content = make_task()),
                form(id = '', content = [
                    formitem(None, btn('save', 'save'), width = 4),
                    formitem(None, btn('kill', 'kill', color = 'warning'), width = 4),
                    formitem(None, btn('del',  'del', color = 'danger'),  width = 4),
                    formitem(None, [ html.Div(id = 'do_kill'), html.Div(id = 'do_save'), html.Div(id = 'do_del') ], width = 12),
                ]),
                
                html.Hr(),
                form(id = '', content = [
                    formitem(None, textarea(id = 'log', placeholder = '')),
                    formitem(None, btn('refresh_log', 'Refresh')),                    
                ])
                
            ]),
            
            box(None, 9, [
                dcc.Graph(id = 'plot'),
                html.Hr(),
                form(id = '', content = [
                    formitem(None, btn('refresh_plot', 'Refresh'), width = 2),
                    formitem(None, dropdown(id = 'window', options = make_options(['1H', '8H', '24H']), value = '1H'), width = 2),
                ])
            ]),
            
        ]),
                
        
        clock(id = 'timer', interval =  30 * 1000)

    ])
)


# %% App Functions

def safeconcat(lst):
    try:    return pd.concat(lst)
    except: return pd.DataFrame()

# Set plot
def set_plot(inputs):
        
    window = handy.descent(inputs, ['window'], '1H')
    time0 = pd.Timestamp('now', tz = 'CET') - pd.Timedelta(window)

    con = sqlite3.connect(setup.dbfile)
    cur = con.cursor()
    data = cur.execute('SELECT timestamp, cycle, name, status FROM history WHERE cycle >= ?', (int(time0.value),)).fetchall()
    data = pd.DataFrame(data, columns = ['timestamp', 'cycle', 'name', 'status'])
    data = data.assign(timestamp = pd.to_datetime(data.timestamp).dt.tz_localize('UTC').dt.tz_convert('CET'))
    data = data.assign(cycle     = pd.to_datetime(data.cycle)    .dt.tz_localize('UTC').dt.tz_convert('CET'))
    cur.close()
    con.close()
        
    colors = {'started': 'orange', 'done': 'lawngreen', 'error': 'red', 'timeout': 'purple' }
    data = data.assign(color = data.status.map(colors))
    data = data.filter(['cycle', 'name', 'color'])

    traces = []
    for name in sorted(data.name.unique()):
        subset = data.query('name == "{}"'.format(name))
        subset = subset.assign(cycle = subset.cycle.dt.tz_localize(None))
        trace = go.Scatter(x = subset['cycle'], y = subset['name'], name = name, mode = 'markers', marker = {'color': subset['color']})
        traces.append(trace)
    figure = {'data': traces, 'layout': go.Layout(height = 680, hovermode= 'closest', margin = go.layout.Margin(l = 200, r = 0, t = 0, b = 20, autoexpand = False), showlegend = False)}
    return figure # plotly(figure)


# Save
def do_save(inputs):

    if not 'save' in inputs['_changes']: return ''

    if inputs['name'] is None: return ''
    
    task = {
        'name':      inputs['name'],
        'execute':   inputs['execute'],
        'email':     inputs['email'],
        'notify':    inputs['notify'],
        'interval':  inputs['interval'],
        'delay':     inputs['delay'],
        'path':      inputs['path'],
        'script':    inputs['script'],
        'timeout':   inputs['timeout']        
    }
        
    freq = '^([0-9]+)([ ]?)(sec|seconds|min|minutes|hour|hours)$'
    
    if re.match('^([a-z0-9_]+)$', task['name'], re.IGNORECASE) is None:
        return 'invalid name: try something like "Project001_MyScript" or "MyProject_Script01"'

    if re.match('interval|continuous|disabled', task['execute'], re.IGNORECASE) is None:
        return 'invalid execute: try "interval" or "continuous" or "disabled"'

    regex = '^([a-z\.]+)@gmail.com$'
    if re.match(regex, task['email'], re.IGNORECASE) is None:
        return 'invalid email: for now only 1 email address'

    if re.match('all|once|none', task['notify'], re.IGNORECASE) is None:
        return 'invalid notify: try "all" or "once" or "none"'
    
    if re.match(freq, task['interval'], re.IGNORECASE) is None:
        return 'invalid interval frequency: try "5 mins" or "1 hour"'

    if re.match(freq, task['delay'], re.IGNORECASE) is None:
        return 'invalid delay frequency: try "5 mins" or "1 hour"'
    
    if re.match(freq, task['timeout'], re.IGNORECASE) is None:
        return 'invalid timeout: try "5 mins" or "1 hour"'
    
    if not os.path.isdir(task['path']):
        return 'could not find path folder: select other folder'
    
    if not os.path.isfile(task['path'] + '/' + task['script']):
        return 'could not find script file: select other script'
    
    with open('./cache/tasks/' + task['name'] + '.txt', 'w') as fp:
        json.dump(task, fp, indent = 2)
    
    return 'saved {} at {}'.format(task['name'], pd.Timestamp('now'))



# Delete
def do_del(inputs):
    if not 'del' in inputs['_changes']: return ''
    name = inputs['name']
    if name is None: return ''
    result = kill_task(name)
    file = './cache/tasks/{}.txt'.format(inputs['name'])
    if os.path.isfile(file):
        os.remove(file)
        return f'{result} ... deleted {inputs["name"]} at {pd.Timestamp("now")}'
    else:
        return f'{result} ... task {inputs["name"]} doesn\'t exist at {pd.Timestamp("now")}'


# Get currently selected task    
def get_name(inputs):
    
    changes = handy.descent(inputs, ['_changes'], [])
    
    if 'tasks' in changes:
        return inputs['tasks']
    
    if 'tasks.selected_row_indices' in changes:
        rows = dashboard.getrows(inputs, 'tasks', selected = True)
        rows = handy.havecols(rows, ['name'])
        name = handy.descent(rows.name.tolist(), [0], '')
        if name != '':
            return name
    
    if 'plot' in changes:
        time = handy.descent(inputs, ['plot', 'points', 0, 'x'], '')
        name = handy.descent(inputs, ['plot', 'points', 0, 'y'], '')
        if name != '':
            return name
    
    return ''


# Set task based on selection of row and or click in plot
def set_task(inputs):
    name = get_name(inputs)
    task = get_task(name)
    return make_task(task)


# Get one task from file
def get_task(name):
    data = json.loads(handy.saveread('./cache/tasks/{}.txt'.format(name), '{}'))
    return data

# Shared kill function to be used by do_kill() and do_del() ...
def kill_task(name):
    pid = handy.saveread('./cache/pid/{}.txt'.format(name), '-1')
    if pid == '-1': return f'{name} seems not to be running (pid={pid})'
    try:
        parent   = psutil.Process(int(pid))
        children = parent.children(recursive = True)
        for child in children: child.kill()
        parent.kill()
        with open('./cache/pid/{}.txt'.format(name), 'w') as f: f.write('-1')
        return f'succesfuly killed {name} ({pid})'
    except Exception as e:
        return f'failed to kill {name} ({pid})'


# Kill service
def do_kill(inputs):
    if not 'kill' in inputs['_changes']: return ''
    name = inputs['name']
    if inputs['kill'] is None: return ''
    return kill_task(name)



def set_log(inputs):
    
    changes = handy.descent(inputs, ['_changes'], [])
    name    = handy.descent(inputs, ['name'], '')
    if 'plot' in changes:
        cycle = handy.descent(inputs, ['plot', 'points', 0, 'x'], 'now')
        name  = handy.descent(inputs, ['plot', 'points', 0, 'y'], '')
        logtime = pd.Timestamp(cycle, tz = 'CET').strftime('%Y%m%d.%H%M%S')
        file = f'./cache/log/{name}/{logtime}.txt'
        return handy.read_log(file, 10000)
    if name != '':
        files = sorted(glob(f'./cache/log/{name}/*'), reverse = True)
        file  = handy.descent(files, [0], '')
        return handy.read_log(file, 10000)            
    return ''


# Set link to edit script in JupyterLab
def set_link(inputs):
    path     = inputs['path']
    script   = inputs['script']
    if not setup.jupyter_folder in path: return setup.jupyter_url
    relpath = path[(len(setup.jupyter_folder) + 1):]
    relpath = relpath.replace('\\', '/')
    link = f'{setup.jupyter_url}/{relpath}/{script}'
    return link


# time = pd.Timestamp('now', tz = 'CET')
def humanize(time):
    try:
        dt = pd.Timestamp('now', tz = 'CET') - pd.Timestamp(time, tz = 'CET')
        c = dt.components
    except:
        return ''
    ds, hr, mn, sc = c.days, c.hours, c.minutes, c.seconds
    if ds > 1: return f'{ds} days ago' if ds > 1 else f'{ds} day ago'
    if hr > 1: return f'{hr} hrs ago'  if hr > 1 else f'{hr} hr ago'
    if mn > 1: return f'{mn} mins ago' if mn > 1 else f'{mn} min ago'
    if sc > 1: return f'{sc} secs ago' if sc > 1 else f'{sc} sec ago'
    return 'right now!'
# humanize(time)

def set_tasks(inputs):

    # Get all tasks
    tasks = [get_task(file.replace('.txt', '')) for file in os.listdir('./cache/tasks/')]
    tasks = pd.DataFrame(tasks)
    tasks = tasks.set_index('name')

    # Get latest status
    con = sqlite3.connect(setup.dbfile)
    cur = con.cursor()
    status = cur.execute('SELECT timestamp, cycle, name, status FROM latest').fetchall()
    status = pd.DataFrame(status, columns = ['timestamp', 'cycle', 'name', 'status'])
    status = status.assign(timestamp = pd.to_datetime(status.timestamp).dt.tz_localize('UTC').dt.tz_convert('CET'))
    status = status.assign(cycle     = pd.to_datetime(status.cycle)    .dt.tz_localize('UTC').dt.tz_convert('CET'))
    status = status.set_index('name')
    cur.close()
    con.close()

    # Make tasks list
    data = tasks.join(status)
    data = data.assign(status  = data.status.where(~data.status.isna(), ''))
    data = data.assign(lastrun = data.status + ' ' + data.cycle.apply(humanize))
    data = data.filter(['lastrun']).reset_index()
    #return make_datatable(data)
    return make_options((data.name + ' ' + data.lastrun).tolist(), data.name.tolist())

# %% App Bindings

# Set plot
app.do(
    on  = onclick('refresh_plot') + on('window') + ontick('timer'),
    set = setplot('plot'),
    to  = set_plot
)

# Set task list
app.do(
    on  = onclick('refresh_tasks'),
    set = setoptions('tasks'), #setdatatable('tasks'),
    to  = set_tasks
)

# Form items
app.do(
    on  = on('tasks') + onplotclick('plot'),
    set = setcontent('task'),
    to  = set_task
)

# Logging info
app.do(
    on  = onplotclick('plot') + onclick('refresh_log'),
    set = setvalue('log'), 
    to  = set_log,
    using = valueof('name')
)

# Create link
app.do(
    on  = on('path') + on('script'),
    set = setlink('link'), 
    to  = set_link
)

# Save task
app.do(
    on  = onclick('save'), 
    set = setcontent('do_save'),
    to  = do_save,
    using = valueof('name') + valueof('execute') + valueof('email') + valueof('notify') + valueof('interval') + valueof('delay') + valueof('path') + valueof('script') + valueof('timeout')
)

# Delete task
app.do(
    on  = onclick('del'), 
    set = setcontent('do_del'),
    to  = do_del,
    using = valueof('name')
)

# Kill running task
app.do(
    on  = onclick('kill'), 
    set = setcontent('do_kill'),
    to  = do_kill,
    using = valueof('name')
)


# %% Run

#if setup.testing: app.run_server(host = setup.host, port = setup.port + 1000, debug = False)
#else: waitress.serve(app.server, host = setup.host, port = setup.port, threads = setup.threads)

app.run_server(host = setup.host, port = setup.port, debug = False)
