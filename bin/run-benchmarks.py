#!/usr/bin/env python
import urllib2
import re
import subprocess
import time
import traceback


MAX_RETRIES = 3
NUM_SLAVES = 16

def countAliveSlaves(master):
  url = 'http://' + master + ':8080'
  response = urllib2.urlopen(url)
  html = response.read()
  aliveCount = len(re.findall('ALIVE', html))
  deadCount = len(re.findall('DEAD', html))
  return (aliveCount, deadCount)

def restart_cluster(master, recompile='', allowed_attempts=MAX_RETRIES):
  print 'Restarting Cluster'
  success = False
  retries = 0
  while (not success and retries < allowed_attempts):
    # rc = subprocess.call(['/root/graphx/bin/rebuild-graphx', recompile])
    rc = subprocess.call(['rebuild-graphx', recompile])
    (aliveCount, deadCount) = countAliveSlaves(master)
    success = (aliveCount == NUM_SLAVES and deadCount == 0 and rc == 0)
    retries += 1
  if not success:
    raise Exception('Cluster could not be resurrected')

def get_master_url():
  master = ''
  # find URL
  with open('/root/spark-ec2/ec2-variables.sh', 'r') as vars:
    for line in vars:
      if 'MASTERS' in line:
        master = line.split('=')[1].strip()[1:-1]
        break
  return master

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

  num_restarts = 0
  command_string = ' '.join(command)
  # Restart cluster if slaves have died. If it fails restart_cluster throws an exception
  # which will propagate through
  (alive, dead) = countAliveSlaves(master)
  if alive != NUM_SLAVES:
    print alive, NUM_SLAVES
    restart_cluster(master, recompile='no', allowed_attempts=1)
    num_restarts += 1

  start = time.time()
  proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  # TODO poll to see if dead slaves
  out, err = proc.communicate()
  end = time.time()
  full_runtime = (end - start)
  rc = proc.returncode
  gx_runtime = -1
  for line in err.splitlines():
    if 'Runtime' in line:
      words = line.split()
      for word in words:
        try:
          gx_runtime = float(word)
          break
        except ValueError, e:
          pass
      break

  (alive, dead) = countAliveSlaves(master)
  if alive != NUM_SLAVES:
    print str(dead) + 'slaves died during run. Rerun algorithm...'
    raise Exception(str(dead) + ' slaves died.')
  if gx_runtime == -1:
    # TODO do something more intelligent
    print err
    raise Exception('Run Failure', err)
  return (command_string, gx_runtime, full_runtime, num_restarts)


def run_part_benchmark(master, strat, timing, errors):
  for i in range(5):
    restart_cluster(master, 'no', 1)
    retries = 0
    success = False
    while (not success and retries < MAX_RETRIES):
      try:
        results = run_algo(master, algo='pagerank', iters=20, strategy=strat)
        print strat, i, results
        success = True
      except Exception as e:
        errors += '\n' + str(e) +'\n'
        retries += 1
        print strat, i
        traceback.print_exc()
    if success:
      timing += str(results)
      
    else:
      errors += 'BENCHMARK FAILED ON TRIAL: ' + str(i)
      break
  return (timing, errors)
      

def main():
  master = get_master_url()
  print master
  timing = ''
  error_output = ''
  strategies = ['EdgePartition1D', 'EdgePartition2D', 'RandomVertexCut']
  for strat in strategies:
    (timing, error_output) = run_part_benchmark(master, strat, timing, error_output)

  now = str(int(time.clock()))
  with open('/root/results/timing-' + now, 'w') as t:
    t.write(timing)
  with open('/root/results/errors-' + now, 'w') as e:
    e.write(error_output)



  # try:
  #   results = run_algo(master, iters=2)
  #   print results
  # except Exception as e:
  #   # TODO something more intelligent, probably retry test
  #   print e
  # (alive, dead) = countAliveSlaves(master)

if __name__ == '__main__':
  main()




