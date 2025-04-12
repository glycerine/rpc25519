#!/usr/bin/python3
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


def runtest(chunker, data, data_len):
  # Stop after we've read enough data and finished a whole block.
  chunker.scan(data, data_len)
  print(data)
  print(chunker)
  assert data.tot_c == chunker.blkstats.sum
  tot_n, dup_n = chunker.blkstats.num, chunker.dupstats.num
  tot_c, dup_c = chunker.blkstats.sum, chunker.dupstats.sum
  perf = float(dup_c) / data.dup_c
  print("bytes: tot=%s dup=%s(%4.2f%%)" % ( tot_c, dup_c, 100.0 * dup_c / tot_c))
  print("blocks: tot=%s dup=%s(%4.2f%%)" % ( tot_n, dup_n, 100.0 * dup_n / tot_n))
  print("found: %4.2f%%" % (100.0 * perf))
  print()
  return perf, chunker.blkstats, chunker.dupstats


def tableadd(table, value, *args):
  # Adds an entry to a nested dict of dicts keyed by the *args.
  for k in args[0:-1]:
    table = table.setdefault(k, {})
  table[args[-1]] = value


def addtest(table, data, dsize, bsize, cls, bavg, bmin, bmax):
  try:
    table[bavg][bmin][bmax]
  except KeyError:
    bavg_len = bsize * bavg
    bmin_len = int(bavg_len * bmin)
    bmax_len = int(bavg_len * bmax)
    data.reset()
    chunker = cls.from_avg(bavg_len, bmin_len, bmax_len)
    result = runtest(chunker, data, 2*dsize)
    tableadd(table, result, bavg, bmin, bmax)

bavgs = (1, 2, 4, 8, 16, 32, 64)
bmins = (0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7)
bmaxs = (1.25, 1.5, 2.0, 4.0, 8.0)

def alltests(cls, tsize, bsize):
  """Get results for different avg,min,max chunker args."""
  results = {}
  # Data size is tsize times the average 8*bsize blocks.
  dsize = tsize*bsize*8
  data = Data(dsize, bsize*16, bsize*8, bsize*4)
  for bavg in bavgs:
    for bmin in bmins:
      addtest(results, data, dsize, bsize, cls, bavg, bmin, 8.0)
      addtest(results, data, dsize, bsize, cls, bavg, bmin, 1.25)
    for bmax in bmaxs:
      addtest(results, data, dsize, bsize, cls, bavg, 0.0, bmax)
    addtest(results, data, dsize, bsize, cls, bavg, 0.5, 2.0)
  bavg = 8.0
  for bmin in bmins:
    addtest(results, data, dsize, bsize, cls, bavg, bmin, 2.0)
  for bmax in bmaxs:
    addtest(results, data, dsize, bsize, cls, bavg, 0.5, bmax)
  return (tsize, bsize, results)


chunkers = dict(
    chunker=Chunker)

# This code is to quicly test RC4 avg_len calculations.
# tsize=1000
# min = 0.0
# avg = 1.0
# for min in (0.0, 0.2, 0.4, 0.6):
#   for avg in (2/3, 4/5, 1.0, 2.0, 3.0):
#     c = RC4Chunker(int((1.0-min)*avg*1024), int(min*1024), 1024)
#     d = Data(tsize*8*1024, 16*1024, 8*1024, 4*1024)
#     runtest(c, d, 2*tsize*8*1024)
#     num_b = c.blkstats.num
#     avg_b = c.blkstats.avg
#     print(avg_b / c.avg_len)
#     print()
# exit(1)


def usage(code, error=None, *args):
  if error:
    print(error % args)
  print(__doc__ % dict(cmd=os.path.basename(sys.argv[0])))
  sys.exit(code)


if __name__ == '__main__':
  cmd = sys.argv[1] if len(sys.argv) > 1 else None
  dir = sys.argv[2] if len(sys.argv) > 2 else '.'
  if cmd in ("-?", "-h", "--help", None):
    usage(0)
  if cmd not in chunkers:
    usage(1, "Error: invalid chunker argument %r.", cmd)
  cls = chunkers[cmd]
  results = alltests(cls, tsize=10000, bsize=1024)
  pickle.dump(results, open('%s/%s.dat' % (dir,cmd), 'wb'))
