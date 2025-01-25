#!/usr/bin/python3
# Data structures and methods for analysis of data.

from __future__ import print_function
inf = float('inf')

class Sample(object):

  def __init__(self, data=None):
    self.num = 0
    self.sum = 0
    self.sum2 = 0
    self.min = inf
    self.max = -inf
    if data:
      self.update(data)

  def add(self, v):
    self.num += 1
    self.sum += v
    self.sum2 += v*v
    self.min = min(self.min, v)
    self.max = max(self.max, v)

  def update(self, data):
    for v in data:
      self.add(v)

  @property
  def avg(self):
    return float(self.sum) / self.num

  @property
  def var(self):
    return (self.sum2 - float(self.sum * self.sum) / self.num) / self.num

  @property
  def dev(self):
    return self.var ** 0.5

  def __repr__(self):
    return "Sample()"

  def __str__(self):
    if self.num:
      return "num=%s sum=%s min/avg/max/dev=%s/%s/%s/%s" % (
          self.num, self.sum, self.min, self.avg, self.max, self.dev)
    else:
      return "num=0 sum=0"

  def getstate(self):
    return (self.num, self.sum, self.sum2, self.min, self.max)

  def setstate(self, state):
    self.num, self.sum, self.sum2, self.min, self.max = state


#class Histogram:
#       def Add(i):
#       def Inc(i,v):
#       def Count():
#       def High():
#       def Low():
#       def Best():
#       def Worst():
#       def Median():

if __name__ == '__main__':
  s = Sample()
  s.add(1.0)
  s.add(2.0)
  print(s)
