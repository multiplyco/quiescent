from pathlib import Path
import json,re,hashlib,datetime,statistics
from zoneinfo import ZoneInfo
root=Path('/Users/henrik/clojure/quiescent/benchmarks/results/20261029T234600Z-macos-arm-wide-32ead33')
reference=Path('/Users/henrik/clojure/quiescent/benchmarks/results/20261029T200137Z-wide-fan-in-32ead33')
r=json.loads((root/'review.json').read_text()); x=json.loads((reference/'review.json').read_text())
assert r['workers']==8 and r['timing-trials']==96 and r['allocation-trials']==40
assert r['status']=='verified'
for k in ['source-sha256','harness-sha256','class-sha256','libraries']:
 assert r['context'][k]==x['context'][k],k
for k in ['java','clojure-version','max-heap-bytes','gc','scheduler-properties']:
 assert r['context']['environment'][k]==x['context']['environment'][k],k
assert r['context']['environment']['os']['os.arch']=='aarch64'
case=r['comparisons'][0]; baseline=x['comparisons'][0]
assert case['case']==baseline['case']=='wide-fan-in'
verified=0
for line in (root/'artifact-sha256.txt').read_text().splitlines():
 digest,path=line.split('  ',1)
 assert hashlib.sha256(Path(path).read_bytes()).hexdigest()==digest,path
 verified+=1
assert verified==44

# Top's global CPU figures are sampled over worker lifetimes, including loading,
# warmup and allocation. They are not restricted to timed-trial windows.
frames=[]; frame={}
for line in (root/'top.log').read_text(errors='replace').splitlines():
 if re.fullmatch(r'\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}',line):
  if 'cpu' in frame: frames.append(frame)
  frame={'time':datetime.datetime.strptime(line,'%Y/%m/%d %H:%M:%S').replace(tzinfo=ZoneInfo('Europe/Stockholm')),'processes':[]}
 m=re.match(r'CPU usage: ([\d.]+)% user, ([\d.]+)% sys, ([\d.]+)% idle',line)
 if m: frame['cpu']=list(map(float,m.groups()))
 m=re.match(r'\s*(\d+)\s+([\d.]+)\s+(\S+)',line)
 if m and 'time' in frame: frame['processes'].append({'pid':int(m[1]),'cpu':float(m[2]),'command':m[3]})
if 'cpu' in frame: frames.append(frame)
host_by_arm={}
for arm in ['quiescent','core-async']:
 windows=[]
 for path in (root/'measurement').glob('fork-*/wide-fan-in/'+arm+'/run.edn'):
  text=path.read_text()
  start=datetime.datetime.fromisoformat(re.search(r':started-at\s+"([^"]+)"',text)[1].replace('Z','+00:00'))
  finish=datetime.datetime.fromisoformat(re.search(r':finished-at\s+"([^"]+)"',text)[1].replace('Z','+00:00'))
  windows.append((start,finish))
 selected=[f for f in frames if any(a<=f['time']<=b for a,b in windows)]
 host_by_arm[arm]={'sample-count':len(selected),'median-user-pct':statistics.median(f['cpu'][0] for f in selected),'median-system-pct':statistics.median(f['cpu'][1] for f in selected),'median-idle-pct':statistics.median(f['cpu'][2] for f in selected)}
background={}
for name in ['iTerm2','WindowServer','com.apple.Virtua']:
 vals=[p['cpu'] for f in frames for p in f['processes'] if p['command']==name]
 if vals: background[name]={'samples-present':len(vals),'median-percent-of-one-core':statistics.median(vals),'max-percent-of-one-core':max(vals)}
# vm_stat repeats a cumulative row after every printed header. Reconcile
# those totals with the intervening deltas rather than counting totals as IO.
vm_deltas=[]; last_absolute=None; cumulative_next=False
for line in (root/'vm-stat.log').read_text().splitlines():
 parts=line.split()
 if parts and parts[0]=='free': cumulative_next=True
 elif len(parts)==22 and parts[0][0].isdigit():
  values=list(map(int,parts[-3:]))  # pageout, swap-in, swap-out (pages)
  if cumulative_next:
   delta=None if last_absolute is None else [v-old for v,old in zip(values,last_absolute)]
   last_absolute=values; cumulative_next=False
  else:
   delta=values
   last_absolute=[old+v for old,v in zip(last_absolute,values)]
  if delta is not None:
   assert min(delta)>=0,delta
   vm_deltas.append(delta)
vm={'intervals':len(vm_deltas),'nonzero-swap-in-intervals':sum(row[1]>0 for row in vm_deltas),'nonzero-swap-out-intervals':sum(row[2]>0 for row in vm_deltas),'nonzero-pageout-intervals':sum(row[0]>0 for row in vm_deltas),'swap-in-bytes':sum(row[1] for row in vm_deltas)*16384,'swap-out-bytes':sum(row[2] for row in vm_deltas)*16384,'pageout-bytes':sum(row[0] for row in vm_deltas)*16384}
trends=[]
for o in r['observations']:
 values=o['sample-ms']; first=statistics.mean(values[:3]); last=statistics.mean(values[-3:])
 trends.append({'framework':o['framework'],'fork':o['fork'],'first-three-mean-ms':first,'last-three-mean-ms':last,'last-over-first':last/first})
host={'by-framework-worker-lifetime':host_by_arm,'background-processes':background,'vm-stat-intervals':vm,'timing-trends':trends}
(root/'host-review.json').write_text(json.dumps(host,indent=2)+'\n')
started=datetime.datetime.fromisoformat((root/'started-at').read_text().strip().replace('Z','+00:00'))
finished=datetime.datetime.fromisoformat((root/'finished-at').read_text().strip().replace('Z','+00:00'))
lines=['# ARM/macOS versus x86/Linux: wide fan-in','',
'Case: 24 channels, 128 producers and one consumer per channel, buffer 1024; 96,000 values/channel (2,304,000 delivered values/trial).',
'',f'Local capture completed {finished.astimezone(ZoneInfo("Europe/Stockholm")).isoformat(timespec="seconds")}; elapsed {finished-started}, including preflight.',
'',f'All eight full workers, 96 timing trials, 40 allocation trials and two smoke workers passed. All {verified} measurement artifacts were independently checksum-verified. All 125 source files and 20 compiled Java classes match the x86 reference; dependency versions, JDK build, heap, collectors, scheduler override properties and normalized JVM flags match.',
'', '## Results','',
'Each time is the median of four independent JVM means. Allocation is whole-JVM transfer bytes per delivered value, measured separately after timing. Ratios are core.async / Quiescent.',
'', '| Machine | Quiescent ms | core.async ms | Time ratio | Q bytes/value | async bytes/value | Allocation ratio |',
'|---|---:|---:|---:|---:|---:|---:|']
for label,c in [('M1 Max / macOS',case),('i5-4590S / Linux',baseline)]:
 q=c['quiescent'];a=c['core-async']
 lines.append(f"| {label} | {q['median-ms']:.3f} | {a['median-ms']:.3f} | {c['time-ratio']:.2f}x | {q['median-allocation-bytes-per-value']:.2f} | {a['median-allocation-bytes-per-value']:.2f} | {c['allocation-ratio']:.2f}x |")
lines+=['', 'The Mac paired time ratios are '+', '.join(f'{v:.2f}x' for v in case['paired-time-ratios'])+'. All observations are retained.',
'', '## Scope of the comparison','',
'The Mac is an Apple M1 Max with eight performance and two efficiency cores, 64 GiB RAM, and macOS 27.0. Totem is an Intel i5-4590S with four cores, about 16 GiB RAM, and Linux 7.0.0-38. Both run native Corretto 26.0.2+10-FR, Clojure 1.12.4, core.async 1.9.865, a 2 GiB heap and G1 with default schedulers. Mac Java reports ten processors; Totem reports four. The source snapshot matches commit 32ead33.',
'', 'This controls the implementation and Java-level harness while changing the complete machine/runtime environment. Architecture, OS synchronization/scheduling, core count and mix, caches/memory, and background activity remain coupled. It does not identify ISA alone as the cause. Absolute timings and ratios are reported separately; samples from the two machines are not pooled.',
'', 'Timing covers prepared worker release through channel transfer/drain, including runtime scheduling and count/sum bookkeeping. Setup is excluded from elapsed timing and recorded separately for allocation. Transfer allocation includes worker teardown and all JVM/background allocation. The default Quiescent task and core.async go execution models are part of the comparison.',
'', '## Host activity and variability','']
for arm,stats in host_by_arm.items():
 lines.append(f"- During {arm} worker lifetimes ({stats['sample-count']} host samples), median system-wide CPU shares were {stats['median-user-pct']:.2f}% user, {stats['median-system-pct']:.2f}% system and {stats['median-idle-pct']:.2f}% idle.")
for name,stats in background.items():
 lines.append(f"- {name} appeared in {stats['samples-present']} top-process samples, with median CPU use {stats['median-percent-of-one-core']:.2f}% of one core when present. This is recorded background activity, not benchmark JVM work.")
lines+=['', f"The vm_stat log contains {vm['intervals']} intervals after its initial cumulative row: {vm['nonzero-swap-in-intervals']} with nonzero swap-in, {vm['nonzero-swap-out-intervals']} with nonzero swap-out, and {vm['nonzero-pageout-intervals']} with nonzero pageout. Totals: {vm['swap-in-bytes']} bytes swapped in, {vm['swap-out-bytes']} bytes swapped out; periodic cumulative rows are reconciled with interval deltas.", '', 'Within-worker timing trends (last three trial mean / first three trial mean):']
for t in trends:
 lines.append(f"- {t['framework']}, fork {t['fork']}: {t['first-three-mean-ms']:.3f} ms → {t['last-three-mean-ms']:.3f} ms ({t['last-over-first']:.3f}x).")
lines+=['', 'These system-wide samples cover complete worker lifetimes, including JVM loading, warmup, setup and allocation, not just timed windows. Background processes contribute to the figures. High system CPU is a useful diagnostic clue for a later synchronization/scheduling profile, not proof of which function or OS mechanism caused the difference.',
'', '## Evidence','', '- [Every JVM and trial](measurement/summary.md)', '- [Portable measurements](review.json)', '- [Host activity summary](host-review.json)', '- [SHA-256 manifest](artifact-sha256.txt)', '- [Matched x86 reference](../20261029T200137Z-wide-fan-in-32ead33/ASSESSMENT.md)', '']
(root/'GENERATED-ASSESSMENT.md').write_text('\n'.join(lines))
print(json.dumps({'case':case,'host':host},indent=2))
