// Isolated test child: only this process trusts the PUBLIC loopback-test certificate.
// Keep IPC alive after the result so parent assertions cannot mistake process exit for transport cleanup.
'use strict';
const {probe,request,transportSession}=require('../../stream-quality-runner.cjs');
const [base,mode,...ports]=process.argv.slice(2),controller=new AbortController();
process.on('message',message=>{if(message==='cancel')controller.abort();if(message==='stop')process.disconnect()});
const timeout=setTimeout(()=>controller.abort(),35000);
(async()=>{
 const values=mode==='identity'?await Promise.all(ports.map(async(port,index)=>{
  port=Number(port);const session=index===0?transportSession(base,port):undefined;
  try{return await request(base,'/api/manifest',{port,session,signal:controller.signal,maxBytes:8192})}
  catch(error){return {error:{message:error.message,code:error.code}}}
  finally{session?.destroy()}
 })):await Promise.all(ports.map((port,index)=>probe(base,{port:Number(port),includeDownload:false,reuseConnections:mode!=='mixed'||index<2,signal:controller.signal})));
 process.send({type:'result',values});
})().catch(error=>process.send({type:'result',error:{message:error.message,code:error.code,scope:error.failureScope}})).finally(()=>clearTimeout(timeout));
process.on('disconnect',()=>controller.abort());
