#cython: language_level=3

# first try
def sumn(int n):
    cdef int s, i
    i = 1
    s = 0
    while i <= n:
        s = s + i
        i = i + 1
    res = s
    return res

# count number of a in b
def cntainb(a, b):
  # still the fastest: return len(a.intersection(b)) / len(a)
  return sum([ 1 for x in a if x in b ])

# VERY slow
def oldcnt(a, b):
    cdef int n
    n = 0
    for x in a:
        if x in b: n += 1
    return n

