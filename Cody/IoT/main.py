# main.py -- put your code here!
import pyb

from machine import Pin

p = Pin('X1', Pin.OUT)
blue_uart pyb.UART(2,9600)

while True:
    if blue_uart.any():
        line = blue_uart.readline()
        line = line.decode('ascii')
        print(line)
        if line == "o":
            print("high")
            p.high()
        else:
            p.low()
