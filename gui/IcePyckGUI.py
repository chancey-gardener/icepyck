# usr/bin/python3
# IcePyck GUI
# 2017 A.I. Bicket

from tkinter import *
from tkinter import ttk
import interface as inter


mmenu = Tk()
mmenu.title('IcePyck Interface for Amazon Glacier')
mainframe = ttk.Frame(mmenu, padding="3 3 12 12")
mainframe.grid(column=0, row=0, sticky=('N', 'W', 'E', 'S'))
mainframe.columnconfigure(0, weight=1)
mainframe.rowconfigure(0, weight=1)
ttk.Label(mainframe, text=inter.pyck).grid(column=2, row=1, sticky=W)
mmenu.mainloop()
