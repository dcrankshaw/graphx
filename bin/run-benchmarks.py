#!/usr/bin/env python
import urllib2
import re
import subprocess
import time


MAX_RETRIES = 3
NUM_SLAVES = 16

def countAliveSlaves(master):
  url = 'http://' + master + ':8080'
  response = urllib2.urlopen(url)
  html = response.read()
  aliveCount = len(re.findall('ALIVE', html))
  deadCount = len(re.findall('DEAD', html))
  return (aliveCount, deadCount)

def run_algo(master,
             algo='pagerank',
             epart=128,
             data='soc-LiveJournal1.txt',
             iters=5,
             strategy='RandomVertexCut'):
  cls = 'org.apache.spark.graph.Analytics'
  command = ['/root/graphx/run-example',
             cls,
             'spark://' + master + ':7077',
             algo,
             'hdfs://' + master + ':9000/' + data,
             '--numIter=' + str(iters),
             '--numEPart=' + str(epart),
             '--partStrategy=' + strategy]
  command_string = ' '.join(command)

  start = time.time()
  print start
  proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  out, err = proc.communicate()
  end = time.time()
  print end
  full_runtime = (end - start)
  rc = proc.returncode
  gx_runtime = -1
  for line in err.splitlines():
    if 'GREPTHIS' in line:
      words = line.split()
      for word in words:
        try:
          gx_runtime = float(word)
          break
        except ValueError, e:
          pass
      break

  if gx_runtime == -1:
    # TODO do something more intelligent
    print err
    raise Exception('Run Failure', err)
  return (gx_runtime, full_runtime)
  # print out
  print err

def restart_cluster(master, recompile=''):
  success = False
  retries = 0
  while (not success and retries < MAX_RETRIES):
    rc = subprocess.call(['/root/graphx/bin/rebuild-graphx', recompile])
    (aliveCount, deadCount) = countAliveSlaves(master)
    success = (aliveCount == NUM_SLAVES and deadCount == 0 and rc == 0)
    retries += 1
  if not success:
    raise Exception('Cluster could not be resurrected')


def main():
  master = ''
  # find URL
  with open('/root/spark-ec2/ec2-variables.sh', 'r') as vars:
    for line in vars:
      if 'MASTERS' in line:
        master = line.split('=')[1].strip()[1:-1]
        break
  try:
    results = run_algo(master, iters=2)
    print results
  except Exception as e:
    # TODO something more intelligent, probably retry test
    print e
  (alive, dead) = countAliveSlaves(master)

if __name__ == '__main__':
  main()







