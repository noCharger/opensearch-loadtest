import pyautogui
import random
import time

while True:
    x = random.randint(0, pyautogui.size().width - 1)
    y = random.randint(0, pyautogui.size().height - 1)
    pyautogui.moveTo(x, y)
    print(f"Mouse moved to: ({x}, {y})")
    time.sleep(60)