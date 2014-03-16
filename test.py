import time

def timer():
    t =[]
    while True:
        t.append(time.time())
        if len(t) > 1:
            yield t[-1] - t.pop(0)
        else:
            yield 0

a = timer()
print a.next()
print a.next()
print a.next()
