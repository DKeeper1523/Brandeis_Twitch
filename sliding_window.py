
#the sliding window class is used to store last n objects, used for manual feature recognition
class slidingWindow():
    data = []
    ptr = 0 #pointer here will increase indefinitely
    window_size = 3

    def __init__(self, in_size):
        self.data = [None] * in_size
        self.window_size = in_size
        self.ptr = 0
    
    def getData(self):
        return self.data

    def getFirst(self):
        return self.data[self.ptr%self.window_size]
    
    def getMid(self):
        return self.data[(self.ptr+1)%self.window_size]

    def getLast(self):
        return self.data[(self.ptr-1)%self.window_size]

    def add(self, obj):
        self.data[self.ptr%self.window_size] = obj
        self.ptr += 1
    
    def isFull(self):
        if any([x is None for x in self.data]):
            return False
        else:
            return True
    
    def isEmpty(self):
        if all([x is None for x in self.data]):
            return True
        else:
            return False

    def earseAll(self):
        self.data = [None] * self.window_size
        self.ptr = 0