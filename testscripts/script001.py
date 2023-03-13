
import random
import time
print(f'script001 starting at {time.strftime("%Y-%m-%d %H:%M:%S")}')

for i in range(0,10):
    time.sleep(1)
    print(f'script001 looping at {time.strftime("%Y-%m-%d %H:%M:%S")}')
    if random.randint(0, 100) < 5: raise Exception('error!')

print('done')
