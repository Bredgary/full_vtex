from tkinter import *
import geommetry_app

def main():
    # ----------------- ROOT DECLARATION ------------------
    root = Tk()
    root.title('Contact Book. V 2.0')
    root.configure(bg = "#53CDB8")
    root.geometry("+350+80")
    root.resizable(0,0)
    geommetry_app.App(root) # Call to the App
    root.mainloop()

if __name__ == "__main__":
    main()