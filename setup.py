
import os
import sys
sys.path.append('../PyLib/')
import handy

testing = False

users = [
    ['admin', 'admin']
]

title = 'Scheduler'

dbfile = './cache/scheduler.db'

jupyter_url    = 'http://localhost:8888/lab/tree'
jupyter_folder = r'C:\Python\Projects'

handy.mkdir('./cache')
handy.mkdir('./cache/tasks')
handy.mkdir('./cache/log')
handy.mkdir('./cache/pid')
handy.mkdir('./cache/sts')
handy.mkdir('./cache/hist')

host = handy.gethost()
port = 2000
threads = 4

binaries = {
    'py':    [r'C:\Users\nabi\appdata\Local\Programs\Python\Python310\python.exe', '-u'],
    'ipynb': [r'C:\Users\nabi\AppData\Local\Programs\Python\Python310\Scripts\jupyter.exe', 'nbconvert', '--execute', '--to', 'notebook', '--inplace'],
    #'r':     [r'C:\Apps\R-Open-3.4.0\bin\x64\Rscript.exe'],
    #'m':     [r'C:\Projects\Python\Scheduler\matlab.bat'],
    'bat':   [r'C:\Windows\System32\cmd.exe', '/C'],
    #'sh':    [r'C:\Program Files\Git\git-bash.exe'],
}

for binary in binaries.values():
    folder  = os.path.dirname(binary[0])
    image   = os.path.basename(binary[0])
    binary0 = binary[0]
    # Check if binary does exist
    if not os.path.isdir(folder) or not os.path.isfile(binary0):
        raise Exception(f'binary {image} does not reside in folder {folder}!')
    # Add folder to path
    #sys.path.append(folder)
    #os.putenv('PATH', folder + ';' + os.getenv('PATH'))
    os.environ['PATH'] = folder + ';' + os.environ['PATH']
