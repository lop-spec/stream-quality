'use strict';
// Explicit diagnostic preload only; never loaded by the runner or normal tests.
const fs=require('node:fs'),path=require('node:path'),os=require('node:os');
const {performance,PerformanceObserver}=require('node:perf_hooks');
const dir=process.env.SQ_TRACE_DIR;if(!dir||!path.isAbsolute(dir))throw Error('Explicit absolute SQ_TRACE_DIR required');
const start=performance.now(),clock=()=>performance.now()-start;
const report={pid:process.pid,entry:path.basename(process.argv[1]||''),node:process.version,epochStart:Date.now(),hrOriginUs:Number(process.hrtime.bigint()/1000n)-clock()*1000,pauses:[],gc:[],cpu:[],system:[]};
let prev=clock(),cpu=process.cpuUsage(),elu=performance.eventLoopUtilization(),lastStats=prev,statsCpu=cpu,lastSystem;
setInterval(()=>{const at=clock(),c=process.cpuUsage(),u=performance.eventLoopUtilization(),extra=at-prev-20;
 if(extra>20)report.pauses.push({fromMs:prev,toMs:at,extraMs:extra,cpuMs:(c.user+c.system-cpu.user-cpu.system)/1000,activeMs:u.active-elu.active,idleMs:u.idle-elu.idle});prev=at;cpu=c;elu=u;
},20).unref();
setInterval(()=>{const at=clock(),c=process.cpuUsage();report.cpu.push({atMs:at,wallMs:at-lastStats,cpuMs:(c.user+c.system-statsCpu.user-statsCpu.system)/1000});lastStats=at;statsCpu=c;
 if(process.env.SQ_TRACE_SYSTEM==='1'){
  const before=clock(),cpus=os.cpus(),totals=cpus.reduce((sum,cpu)=>{for(const [key,v]of Object.entries(cpu.times))sum[key]=(sum[key]||0)+v;return sum},{}),total=Object.values(totals).reduce((a,b)=>a+b,0);
  report.system.push({atMs:clock(),readMs:clock()-before,logicalCpus:cpus.length,busyPct:lastSystem?100*(1-(totals.idle-lastSystem.idle)/(total-lastSystem.total)):null,freeMB:os.freemem()/1048576,totalMB:os.totalmem()/1048576});lastSystem={idle:totals.idle,total};
 }
},1000).unref();
new PerformanceObserver(list=>{for(const e of list.getEntries())report.gc.push({fromMs:e.startTime-start,durationMs:e.duration,kind:e.detail?.kind})}).observe({entryTypes:['gc']});
process.on('exit',()=>{report.wallMs=clock();fs.writeFileSync(path.join(dir,`trace-${process.pid}.json`),JSON.stringify(report),{flag:'wx'})});
