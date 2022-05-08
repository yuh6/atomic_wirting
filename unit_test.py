import unittest
import decorator_atomic_write
import time
import threading


class MyTestCase(unittest.TestCase):
    def thread1(self):
        decorator_atomic_write.write_file("Thread 1", "./sample1.txt", mode='string_w')
        time.sleep(4)
        print("Thread 1, time is %f" % time.perf_counter())

    def thread2(self):
        decorator_atomic_write.write_file("Thread 2", "./sample1.txt", mode='string_w')
        time.sleep(2)
        print("Thread 1, time is %f" % time.perf_counter())

    def test_something(self):
        t1 = threading.Thread(target=MyTestCase.thread1(self))
        t2 = threading.Thread(target=MyTestCase.thread2(self))
        t1.start()
        t2.start()
        time.sleep(6)
        with open("./sample1.txt", 'r') as f:
            content = f.read()
        self.assertEqual(content, str('Thread 2'))  # add assertion here


if __name__ == '__main__':
    unittest.main()
