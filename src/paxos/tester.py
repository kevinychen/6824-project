#!/usr/bin/env python
import subprocess

f = open('results.txt', 'w')

for i in range(100):
#  p = subprocess.Popen('go test -test.run TestRepeatedCrashUnreliable', stdout = subprocess.PIPE, stderr = subprocess.PIPE, shell = True)
  p = subprocess.Popen("go test | egrep -v 'EOF|connection|broken'", stdout = subprocess.PIPE, stderr = subprocess.PIPE, shell = True)
  out, err = p.communicate()

  if 'PASS' in out:
    print i, 'SUCCESS!'
  else:
    print i, 'FAILURE!'
    print out
    print "======================"
    print err

    f.write("FAILED ON RUN: %d\n" % i)
    f.write("OUTPUT:\n" + out)
    f.write("ERROR: \n" + err)
    f.write("=======================\n")

f.close()
