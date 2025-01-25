#!/usr/bin/pypy3 -O
"""
RollsumChunking modelling.

This will run tests for the specified chunker with different avg/min/max
length settings, and dump the summary data into a file in a directory.

Usage: %(cmd)s <chunker|weibull[0-2]|weibullt[0-2]|nc[1-3]|rc4|fastweibull2> [dir]

This file is from from https://github.com/dbaarda/rollsum-chunking/blob/master/chunker.py  and its license is:

This is free and unencumbered software released into the public domain.

Anyone is free to copy, modify, publish, use, compile, sell, or
distribute this software, either in source code form or as a compiled
binary, for any purpose, commercial or non-commercial, and by any
means.

In jurisdictions that recognize copyright laws, the author or authors
of this software dedicate any and all copyright interest in the
software to the public domain. We make this dedication for the benefit
of the public at large and to the detriment of our heirs and
successors. We intend this dedication to be an overt act of
relinquishment in perpetuity of all present and future rights to this
software under copyright law.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
OTHER DEALINGS IN THE SOFTWARE.

For more information, please refer to <https://unlicense.org>

"""
from __future__ import print_function
import os
import pickle
import random
import sys
from math import e, gamma, log
from stats1 import Sample

def solve(f, x0=-1.0e9, x1=1.0e9, e=1.0e-9):
  """ Solve f(x)=0 for x where x0<=x<=x1 within +-e. """
  y0, y1 = f(x0), f(x1)
  # y0 and y1 must have different sign.
  assert y0*y1 <= 0
  while (x1 - x0) > e:
    xm = (x0 + x1) / 2.0
    ym = f(xm)
    if y0*ym > 0:
      x0,y0 = xm,ym
    else:
      x1,y1 = xm,ym
  return x0

def gammalower(s, z):
  # Make sure s is a float.
  s=float(s)
  # For large z it converges to gamma(s).
  if z >= 32:
    return gamma(s)
  tot = term = z**s * e**-z / s
  # For the range of z and s values we care about, this is enough iterations.
  for i in range(1,int(2*z)+12):
    term *= z / (s+i)
    tot += term
  return tot


class RandIter(object):
  """ A fast LCG random uint32 iterator.

  This also supports specifying a cycle period where it will repeat the
  values, and a fast skip(n) method for skipping over n values.
  """

  # Pythons random stuff is too slow, so we use a simple good-enough LCG
  # generator with modulus m = 2^32 for 32 bits. These values come from
  # Numerical Recipes.
  m = 2**32  # LCG modulus value.
  a = 1664525  # LCG multiplier value.
  c = 1013904223  # LCG increment value.
  b = m - 1  # fast bitmask version of m.

  def __init__(self, seed, cycle=2**32):
    self.seed=seed
    self.cycle=cycle
    self.value = seed
    self.count = 0  # count of total values produced.
    self.dat_n = 0  # count of values left this cycle.

  def __iter__(self):
    return self

  def __next__(self):
    if not self.dat_n:
      self.value = self.seed
      self.dat_n = self.cycle
    self.value = (self.a * self.value + self.c) & self.b
    self.count += 1
    self.dat_n -= 1
    return self.value

  # For python2 compatibility.
  next = __next__

  def skip(self, n):
    """ Skip over the next n random values. """
    self.count += n
    self.dat_n -= n
    # if past the cycle length, skip to the start of the last cycle.
    if self.dat_n < 0:
      self.value = self.seed
      self.dat_n %= self.cycle
      n = self.cycle - self.dat_n
    # https://www.nayuki.io/page/fast-skipping-in-a-linear-congruential-generator
    m, a, c = self.m, self.a, self.c
    a1 = self.a - 1
    ma = a1 * m
    self.value = (pow(a, n, m)*self.value + (pow(a, n, ma) - 1) // a1 * c) & self.b
    return self.value

  def getstate(self):
    return (self.value, self.count, self.dat_n)

  def setstate(self, state):
    self.value, self.count, self.dat_n = state


class Data(object):
  """ Data source with rollsums and block hashes.

  It simulates a stream of data that starts with olen bytes of initial random
  data that is then repeated with modifications. The modifications are cycles
  of copied, inserted, and deleted data. The copy, insert, and delete have
  exponentially distributed random lengths with averages of clen, ilen, and
  dlen respectively.

  It simulates returning a 32bit rolling hash for each input byte with
  getroll(). A simulated strong hash of the previous block can be fetched with
  gethash(), which also starts a new block.

  It keeps counts of the number of bytes and duplicate bytes.
  """

  def __init__(self, olen, clen, ilen, dlen, seed=1):
    self.olen = olen
    self.clen = clen
    self.ilen = ilen
    self.dlen = dlen
    self.seed = seed
    # exponential distribution lambda parameters for clen/ilen/dlen.
    self.clambd = 1.0/clen
    self.ilambd = 1.0/ilen
    self.dlambd = 1.0/dlen
    self.reset()

  def reset(self):
    self.tot_c = 0         # total bytes scanned.
    self.dup_c = 0         # duplicate bytes scanned.
    self.blkh = 0          # the accumulated whole block hash.
    # Initialize the random generators for the original and inserted data.
    self.dat = RandIter(self.seed, self.olen)
    self.ins = RandIter(self.seed + 6)
    self.mod = random.Random(self.seed)
    self.cpystats = Sample()
    self.insstats = Sample()
    self.delstats = Sample()
    self.initcycle()

  def initcycle(self):
    self.cpy_n = int(self.mod.expovariate(self.clambd))
    self.ins_n = self.ilen and int(self.mod.expovariate(self.ilambd))
    self.del_n = self.dlen and int(self.mod.expovariate(self.dlambd))
    self.cpystats.add(self.cpy_n)
    self.insstats.add(self.ins_n)
    self.delstats.add(self.del_n)

  def getroll(self):
    if self.tot_c < self.olen:
      # Output initial data.
      h = self.dat.next()
    elif self.cpy_n:
      # Output copied data.
      h = self.dat.next()
      self.cpy_n -= 1
      self.dup_c += 1
    elif self.ins_n:
      # Output inserted data.
      h = self.ins.next()
      self.ins_n -= 1
    else:
      # do delete, setup next cycle, and recurse.
      self.dat.skip(self.del_n)
      self.initcycle()
      return self.getroll()
    # increment tot_c and update blkh.
    self.tot_c += 1
    self.blkh = hash((self.blkh, h))
    return h

  def gethash(self):
    """ Get a strong hash of the past l bytes and reset for a new block. """
    blkh, self.blkh = self.blkh, 0
    return blkh

  def getstate(self):
    return (self.tot_c, self.dup_c, self.blkh,
            self.cpy_n, self.ins_n, self.del_n,
            self.dat.getstate(), self.ins.getstate(), self.mod.getstate(),
            self.cpystats.getstate(), self.insstats.getstate(), self.delstats.getstate())

  def setstate(self, state):
    (self.tot_c, self.dup_c, self.blkh, self.cpy_n, self.ins_n, self.del_n,
     dat, ins, mod, cpystats, insstats, delstats) = state
    self.dat.setstate(dat)
    self.ins.setstate(ins)
    self.mod.setstate(mod)
    self.cpystats.setstate(cpystats)
    self.insstats.setstate(insstats)
    self.delstats.setstate(delstats)

  def __repr__(self):
    return "Data(olen=%s, clen=%s, ilen=%s, dlen=%s, seed=%s)" % (
        self.olen, self.clen, self.ilen, self.dlen, self.seed)

  def __str__(self):
    return "%r: tot=%d dup=%d(%4.1f%%)\n  cpy: %s\n  ins: %s\n  del: %s" % (
        self, self.tot_c, self.dup_c, 100.0 * self.dup_c / self.tot_c,
        self.cpystats, self.insstats, self.delstats)


class Chunker(object):
  """ A standard exponential chunker

  This is the standard simple chunker that gives an exponential distribution
  of block sizes between min and max. The only difference is it uses 'h<p`
  instead of 'h&mask==r' for the hash judgement, which supports arbitrary
  target block sizes, not just power-of-2 sizes. For tgt_len as the mean, the
  distribution's curves where x is measured from min_len and L is the
  normal exponential distribution lambda parameter are;

    f(x) = L
    CDF(x) = 1 - e^-(L*x)
    PDF(x) = L*e^-(L*x)
    mean = C + A*(1-e^-(L*T))

  Where;

    A = tgt_len
    L = 1/A
    C = min_len
    T = max_len - min_len

  The tgt_len for this chunker represents the exponential distribution mean
  size, not including the affects of min_len and max_len.
  """

  MIN_LEN, MAX_LEN = 0, 2**32

  def __init__(self, tgt_len, min_len=MIN_LEN, max_len=MAX_LEN):
    assert min_len < max_len
    self.tgt_len = tgt_len
    self.min_len = min_len
    self.max_len = max_len
    self.avg_len = self.get_avg_len(tgt_len, min_len, max_len)
    self.reset()

  @classmethod
  def from_avg(cls, avg_len, min_len=MIN_LEN, max_len=MAX_LEN):
    """Initialize using the avg_len."""
    tgt_len = int(cls.get_tgt_len(avg_len, min_len, max_len) + 0.5)
    return cls(tgt_len, min_len, max_len)

  @classmethod
  def get_avg_len(cls, tgt_len, min_len, max_len):
    """Get the avg_len given a tgt_len."""
    if tgt_len <= 0:
      return min_len
    z = (max_len - min_len)/tgt_len
    return min_len + tgt_len * (1.0 - e**-z)

  @classmethod
  def get_tgt_len(cls, avg_len, min_len, max_len):
    """Get the tgt_len given an avg_len."""
    return solve(lambda x: cls.get_avg_len(x, min_len, max_len) - avg_len,
                 x0=0.0, x1=2.0**32, e=0.5)

  def reset(self):
    self.blocks = {}
    self.blkstats = Sample()
    self.dupstats = Sample()
    self.prob = 2**32 // self.tgt_len
    self.initblock()

  def initblock(self):
    self.blk_len = 0

  def incblock(self):
    self.blk_len += 1

  def isblock(self, r):
    """ Checks if rollsum r is a break point and increments the block. """
    self.incblock()
    return self.blk_len >= self.min_len and (r < self.prob or self.blk_len >= self.max_len)

  def addblock(self, h):
    """ Adds a block with hash h and initializes for the next block. """
    l = self.blk_len
    b = (h, l)
    n = self.blocks[b] = self.blocks.setdefault(b, 0) + 1
    self.blkstats.add(l)
    if n > 1:
      self.dupstats.add(l)
    self.initblock()

  def scan(self, data, len):
    """Scan for whole chunks upto at least len offset in data."""
    # keep a reference to the data for chunkers that might need it.
    self.data = data
    # Stop after we've read enough data and finished a whole block.
    while data.tot_c < len or self.blk_len:
      if self.isblock(data.getroll()):
        self.addblock(data.gethash())
    return data.tot_c

  def getstate(self):
    """Get a mid-block-point state snapshot."""
    return (self.blk_len, self.data.getstate())

  def setstate(self, state):
    """Restore a saved mid-block-point state snapshot."""
    self.blk_len, data = state
    self.data.setstate(data)

  def __repr__(self):
    return "%s(tgt_len=%s, min_len=%s, max_len=%s)" % (
        self.__class__.__name__, self.tgt_len, self.min_len, self.max_len)

  def __str__(self):
    return "%r: avg_len=%s\n  blks: %s\n  dups: %s" % (
        self, self.avg_len, self.blkstats, self.dupstats)

