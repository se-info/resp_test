import pyautogui
import time
import keyboard

# Coordinates where you want to click (x, y)
x, y = 959, 435

# Interval between taps in seconds
interval = 0.001

# Duration for auto-clicking in seconds (20 minutes)
duration = 20 * 60

# Get the start time
start_time = time.time()

try:
    while True:
        # Check if the 'esc' key is pressed
        if keyboard.is_pressed('esc'):
            print("Auto tap script stopped.")
            break
        
        # Check if the duration has passed
        elapsed_time = time.time() - start_time
        if elapsed_time > duration:
            print("20 minutes have passed. Auto tap script stopped.")
            break

        # Move the mouse to the specified coordinates
        pyautogui.moveTo(980, 451)
        
        # Perform the click
        pyautogui.click()
        
        # Wait for the specified interval
        time.sleep(interval)

except KeyboardInterrupt:
    print("Auto tap script stopped.")
