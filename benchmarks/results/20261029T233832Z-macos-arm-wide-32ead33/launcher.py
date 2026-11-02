import fcntl,subprocess,sys
from pathlib import Path
root=Path('/Users/henrik/clojure/quiescent/benchmarks/results/20261029T233832Z-macos-arm-wide-32ead33')
with open('/private/tmp/quiescent-local-benchmark.lock','a') as lock:
 try: fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
 except BlockingIOError: sys.exit('A local Quiescent capture already holds the lock.')
 with open(root/'console.log','w') as log:
  result=subprocess.call(['/usr/bin/caffeinate','-is','/bin/bash',str(root/'capture.sh')],stdout=log,stderr=subprocess.STDOUT)
 print('Capture exited',result,flush=True)
 sys.exit(result)
