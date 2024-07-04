import pyautogui
import tkinter as tk

def get_mouse_position():
    # Get the current mouse position
    x, y = pyautogui.position()
    # Display the coordinates in the label
    coords_label.config(text=f"Mouse coordinates: ({x}, {y})")
    # Schedule the function to run again after 100 milliseconds
    root.after(100, get_mouse_position)

# Create the main window
root = tk.Tk()
root.title("Mouse Coordinates")

# Create a label to display the coordinates
coords_label = tk.Label(root, text="Mouse coordinates: (0, 0)", font=("Helvetica", 16))
coords_label.pack(pady=20)

# Start getting the mouse position
get_mouse_position()

# Start the Tkinter event loop
root.mainloop()