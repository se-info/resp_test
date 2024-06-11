import pyautogui
import tkinter as tk
import threading
import time

# Constants (adjust as needed)
CURSOR_SIZE = 16
CURSOR_COLOR = "red"
CLICK_INTERVAL = 1000  # Milliseconds between clicks (1 second)

# Get screen dimensions
SCREEN_WIDTH, SCREEN_HEIGHT = pyautogui.size()

# Create the tkinter window for the cloned cursor
root = tk.Tk()
root.overrideredirect(True)
# root.attributes("-topmost", True)
root.attributes("-disabled", True)

canvas = tk.Canvas(root, width=CURSOR_SIZE, height=CURSOR_SIZE, bg=CURSOR_COLOR, highlightthickness=0)
canvas.pack()

# Auto-click state variables (shared between threads)
auto_click_enabled = True
next_click_time = 0

# Function for the auto-clicking thread
def auto_click_thread():
    global next_click_time
    while auto_click_enabled:
        current_time = time.time() * 1000  # Convert to milliseconds
        if current_time >= next_click_time:
            x, y = 2612,1686  # Get the main cursor's position
            pyautogui.click(x, y)  # Perform the click
            next_click_time = current_time + CLICK_INTERVAL
        time.sleep(0.01)  # Sleep briefly to avoid excessive CPU usage

# Function to update the cloned cursor's position
def update_cursor():
    x, y = 2612,1686
    root.geometry(f"+{x}+{y}")
    root.after(10, update_cursor)

# Create and start the auto-clicking thread
auto_clicker_thread = threading.Thread(target=auto_click_thread)
auto_clicker_thread.daemon = True
auto_clicker_thread.start()

# Start the cloned cursor update loop
update_cursor()
root.mainloop()
