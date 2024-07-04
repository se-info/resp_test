from tkinter import *
import time
root = Tk()
root.geometry('600x600')
score = 2000000
clicker_counter = 1

def counter(args):
    global score
    if args==1:
        score += 1
        points_label.config(text=score)
    elif args==2:
        time_interval=1000 #change it as you like
        points_button.invoke()
        root.after(time_interval,lambda:counter(2))

points_button = Button(root, text='click me', command=lambda:counter(1))

def autoclicker(args):
    global clicker_counter
    if args == 1:
        time_interval=1000 #change it as you like
        points_button.invoke()
        root.after(time_interval,lambda:autoclicker(1))

def clickerpurchase():
    global clicker_counter
    global score
    if score >= 1000:
        score -= 1000
        clicker_counter += 1
        points_label.config(text=score)
        clicker_label['text'] += str(clicker_counter)
        clicker_label.config(text='purchase clicker(1k): ' + str(clicker_counter))

clicker_button = Button(root, text='purchase', command=lambda:[clickerpurchase, autoclicker(1)])
clicker_button.grid(row=0, column=3)

clicker_label = Label(root, text='purchase clicker(1k): ')
clicker_label.grid(row=0, column=2)

points_label = Label(root, text='0')
points_label.grid(row=0, column=1)

points_button = Button(root, text='click me', command=counter)
points_button.grid(row=0, column=0)

points_label.config(text=score)
root.mainloop()