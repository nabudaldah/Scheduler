
import time
print(f'app001 starting at {time.strftime("%Y-%m-%d %H:%M:%S")}')

while True:
    time.sleep(1)
    print(f'app001 looping at {time.strftime("%Y-%m-%d %H:%M:%S")}')

print('done')
