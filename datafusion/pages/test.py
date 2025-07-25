import threading
import time

def print_cube(num):
    while num > 0:
        num -= 0.1
        time.sleep(0.1)
    print("Cube: {}" .format(num * num * num))
    


def print_square(num):
    print("Square: {}" .format(num * num))


if __name__ =="__main__":
    t1 = threading.Thread(target=print_square, args=(10,))
    t2 = threading.Thread(target=print_cube, args=(10,))

    t1.start()
    t2.start()
    print('low')
    t1.join()
    t2.join()

    print("Done!")