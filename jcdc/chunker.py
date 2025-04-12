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


class Weibull0Chunker(Chunker):
  """ Weibull0Chunker class.

  This uses a chunking probability criteria where the hash is treated as a
  fixed point number in the range 0.0 -> 1.0 and compared to a slowly
  increasing probability. The position x past min_len is a chunk boundary if h
  < f(x) where the f(x) "hazard function" is a function of x^P. This gives a
  Weibull block length distribution. For tgt_len as the mean and P curve
  power, the distribution's curves where x is measured from min_len and k and
  L are the normal Weibull parameters are;

    f(x) = M*x^P
    CDF(x) = 1 - e^-(M/k*x^k)
    PDF(x) = M*x^(k-1) * e^-(M/k*x^k)
    mean = C + L*gammalower((k+1)/k,(T/L)^k) + T*e^-((T/L)^k)

  Where;

    k = P + 1
    L = tgt_len/gamma(1+1/k)
    M = k/L^k = b*k
    C = min_len
    T = max_len - min_len

  The tgt_len for this chunker represents the distribution mean, not including
  the effects of min_len and max_len. This class uses P=0 (k=1) which makes it
  the same as a classic chunker, but subclasses can overide P for different
  variants.
  """
  P = 0

  @classmethod
  def get_avg_len(cls, tgt_len, min_len, max_len):
    # Getting the average length for the distribution chopped off at 't' is
    # calculated as follows;
    #
    # avg = integ(x*PDF(x), 0, t) + t*(1-CDF(t))
    # x*PDF(x) = k*(x/L)^k*e^(-(x/L)^k)
    # t*(1-CDF(t)) = t*e^(-(t/L)^k)
    # avg = integ(k*(x/L)^k*e^(-(x/L)^k),0,t) + t*e^(-(t/L)^k)
    #     = L*gammalower(1 + 1/k, (t/L)^k) + t*e^(-(t/L)^k)
    if tgt_len <= 0:
      return min_len
    k = cls.P + 1
    s = 1.0 + 1.0/k
    L = tgt_len / gamma(s)
    t = max_len - min_len
    z = (t/L)**k
    return min_len + L * gammalower(s, z) + t*e**(-z)

  def reset(self):
    super(Weibull0Chunker, self).reset()
    # Set the M probability multiplier.
    k = self.P + 1
    L = self.tgt_len / gamma(1.0 + 1.0/k)
    self.M = 2**32 * k / L**k

  def initblock(self):
    self.blk_len = 0
    self.prob = 0.0

  def incblock(self):
    self.blk_len += 1
    x = self.blk_len - self.min_len
    if x > 0:
      self.prob = int(self.M * x**self.P)

class Weibull1Chunker(Weibull0Chunker):
  P = 1

class Weibull2Chunker(Weibull0Chunker):
  P = 2


class WeibullT0Chunker(Weibull0Chunker):
  """WeibullT0 Chunker class.

  This is the similar to the Weibull Chunker except that min_len doesn't just
  shift the distribution to the right, instead it zero's the hazard function.
  This changes the distribution so it's not actually a Weibull distribution
  any more, unless min_len=0. The distribution's curves, where x is measured
  from min_len and k and L are the normal Weibull parameters, are;

    f(x) = M*(x+C)^P
    CDF(x) = 1 - e^-(M/k*((x+C)^k - C^k))
    PDF(x) = M*(x+C)^(k-1) * e^-(M/k*((x+C)^k - C^k))
    mean = L*e^((C/L)^k) * (gammalower((k+1)/k, ((T+C)/L)^k) -
        gammalower((k+1)/k, (C/L)^k)) + (C+T)*e^-(((T+C)/L)^k - (C/L)^k)

  Where;

    k = P + 1
    L = tgt_len/gamma(1+1/k)
    M = k/L^k
    C = min_len
    T = max_len - min_len

  The tgt_len for this chunker represents the weibull distribution mean, not
  including the effects of min_len and max_len. This class uses P=0 (k=1)
  which makes it the same as a classic chunker, but subclasses can overide P
  for different variants.
  """

  @classmethod
  def get_avg_len(cls, tgt_len, min_len, max_len):
    if tgt_len <= 0:
      return min_len
    k = cls.P + 1
    s = 1.0 + 1.0/k
    L = tgt_len / gamma(s)
    zc = (min_len/L)**k
    zt = (max_len/L)**k
    return L * e**zc * (gammalower(s, zt) - gammalower(s, zc)) + max_len*e**(zc-zt)

  def incblock(self):
    self.blk_len += 1
    x = self.blk_len
    if x >= self.min_len:
      self.prob = int(self.M * x**self.P)

class WeibullT1Chunker(WeibullT0Chunker):
  P = 1

class WeibullT2Chunker(WeibullT0Chunker):
  P = 2


class NC1Chunker(Chunker):
  """ NC1Chunker class.

  This implements FastCDC's NC chunking algorithm modified to use uses a 'h<p'
  hash judgment to support arbitrary tgt_len values.

  The tgt_len for this chunker is the "target length" to set the hazzard
  function probabilities of 1/(tgt_len<<NC) and 1/(tgt_len>>NC). The
  "transition point" where the probability steps up is set to tgt_len/2. Note
  that this is offset by min_len, and copies what was evaluated in the FastCDC
  paper.

  The FastCDC paper is not entirely clear how it set things up for different
  min_len values. It seems to have used a fixed 8K "normalized chunk size" for
  the purpose of setting the hash judgement masks, and then set the transition
  point to 4K past min_len. This is like setting the transition point to half
  of the target length, which we copy here. However, this is a little strange
  and unexplained given they evaluated normalized chunking's distribution for
  min_len=0 with the transition point == target length.

  Other common implementations based on
  https://github.com/ronomon/deduplication set the hash judgment masks based
  on the target length, and set the transition point to max(0, tgt_len -
  1.5*min_len), which is also strange since it means you only use the first
  mask if tgt_len > 2.5*min_len, and FastCDC recommends and gets it's speed
  benefits when tgt_len <= 2*min_len.

  The distribution's curves where x is measured from min_len and L is the
  normal exponential distribution lambda parameter are;

    f(x) = L1, x<=T1
           L2, x>T1
    CDF(x) = 1 - e^-(L1*x), x<=T1
             1 - e^-(L1*T1 + L2*(x-T1)), x>T1
    PDF(x) = L1*e^-(L1*x), x<=T1
             L2*e^-(L1*T1 + L2*(x-T1)), x>T1
    mean = C + A1 - e^-(L1*T1) * (A1 - A2*(1-e^-(L2*T2)))

  Where;

    A1 = tgt_len << NC
    A2 = tgt_len >> NC
    L1 = 1/A1
    L2 = 1/A2
    C = min_len
    mid_len = min_len + tgt_len/2
    T1 = mid_len - min_len
    T2 = max_len - mid_len

  This sets the "normalized chunking level" NC=1, but subclasses can override
  it for different levels.
  """
  NC=1

  @classmethod
  def get_avg_len(cls, tgt_len, min_len, max_len):
    if tgt_len <= 0:
      return min_len
    A1 = tgt_len * 2.0**cls.NC
    A2 = tgt_len / 2.0**cls.NC
    mid_len = min(min_len + tgt_len / 2.0, max_len)  # transition point
    z1 = (mid_len - min_len)/A1
    z2 = (max_len - mid_len)/A2
    return min_len + A1 - e**-z1 * (A1 - A2*(1-e**-z2))

  def reset(self):
    super(NC1Chunker, self).reset()
    # Set the transition point where we change probabilities.
    self.mid_len = self.min_len + self.tgt_len // 2

  def initblock(self):
    self.blk_len = 0
    self.prob = 2**32 // (self.tgt_len << self.NC)

  def incblock(self):
    self.blk_len += 1
    if self.blk_len == self.mid_len:
      self.prob = 2**32 // (self.tgt_len >> self.NC)

class NC2Chunker(NC1Chunker):
  NC=2

class NC3Chunker(NC1Chunker):
  NC=3


class RC4Chunker(Chunker):
  """ RC4Chunker Class.

  This implements MicroSofts "Regression Chunker" algorithm modified to use
  uses a 'h<p' hash judgment to support arbitrary tgt_len values.

  This implementation use k=4, but subclasses can override this.
  """
  K=4

  @classmethod
  def get_avg_len(cls, tgt_len, min_len, max_len):
    """Get the avg_len given a tgt_len."""
    if tgt_len <= 0:
      return min_len
    # This is a mystery scaling factor for c that somehow works.
    M = 0.85
    A = tgt_len
    C = min_len
    T = max_len - C  # regression distance between min_len and max_len.
    # Iteratively solve for "c" offset to blocks after regressions.
    ck = [0.0] * cls.K  # additional regression length per regression.
    a, da = 0, 1
    while abs(da) >= 0.001:
      da = a                   # store old a in da for calculating da later.
      Ak = A                   # tgt_len for each recursion.
      cr = sum(ck)             # cumulative regression for each recursion.
      dr = e**-((T-cr)/A)      # cumulative decay for each recursion.
      dC = e**-(C/A)           # reverse decay fraction after C.
      a = cr + A - dr*(T + A)  # avg len past C with max_len chopped off.
      # Add reverse-decaying regressions minus the bits before C+ck.
      for k in range(cls.K):
        dk = e**-((T-cr)/Ak)   # regression decay past cr.
        a += dr*(T - Ak + dk*(Ak - cr))
        #print("Ak=%s dr=%s cr=%s dk=%s ck=%s a=%s" % (Ak, dr, cr, dk, ck[k], a))
        cr, ck[k] = cr - ck[k], M*dr*(dC*Ak - dk*(T - cr - C + Ak)) if T>C else 0.0
        dr *= dk
        Ak /= 2
        dC *= dC
      # Add the final bit truncated to max_len.
      a += dr*T
      # Update the average length change and iterate.
      da -= a
      #print("tgt=%s min=%s max=%s a=%s da=%s" % (tgt_len, min_len, max_len, a, da))
    return C + a

  def initblock(self):
    self.blk_len = 0
    self.rprob = 2**(32 + self.K) // self.tgt_len
    self.rstate = None

  def isblock(self, r):
    """ Checks if rollsum r is a break point and increments the block. """
    self.incblock()
    if self.blk_len < self.min_len:
      # Too small, not a block.
      return False
    elif self.blk_len >= self.max_len:
      # Too big, is a block, and restore to regression point if it's better.
      if r >= self.rprob and self.rstate:
        # Restore the regression state.
        self.setstate(self.rstate)
      return True
    elif r < self.rprob:
      # A better regression state or possible block.
      if r < self.prob:
        # It is a block!
        return True
      # Update the regression state and adjust rprob.
      self.rstate = self.getstate()
      while r < (self.rprob >> 1):
        self.rprob >>= 1
    return False


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
    chunker=Chunker,
    weibull0=Weibull0Chunker,
    weibull1=Weibull1Chunker,
    weibull2=Weibull2Chunker,
    weibullt0=WeibullT0Chunker,
    weibullt1=WeibullT1Chunker,
    weibullt2=WeibullT2Chunker,
    nc1=NC1Chunker,
    nc2=NC2Chunker,
    nc3=NC3Chunker,
    rc4=RC4Chunker)

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
