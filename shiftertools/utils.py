"""
Utility functions
"""

import sys

def stprint(msg):
    print(">>>",msg)

def error(msg, func=False):
    print("\n <<--ERROR-->>:",msg,"\n")
    if func: func()
    sys.exit()


class Messenger:
    def __init__(self,owner):
        self.owner = owner

    def log(self,*args):
        message = '>>> '+self.owner+': '
        for arg in args: message += str(arg) + ' '
        print(message)

  
