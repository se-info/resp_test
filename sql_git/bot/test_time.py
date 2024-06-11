import time

wakeup = time.time()

print(wakeup)

while True:
	print('start')
	wakeup += 6 * 60
	for i in range(500):
		if time.time() > wakeup:
			break
		while time.time() < wakeup:
			print('start sleep')
			time.sleep(1)