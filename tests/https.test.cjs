'use strict';
const {test}=require('node:test'),assert=require('node:assert/strict');
const http=require('node:http'),https=require('node:https'),net=require('node:net'),fs=require('node:fs'),path=require('node:path'),{fork}=require('node:child_process');
const cert=path.join(__dirname,'fixtures/loopback-test.cert.pem'),key=path.join(__dirname,'fixtures/loopback-test.key.pem');
const listen=server=>new Promise((resolve,reject)=>{server.once('error',reject);server.listen(0,'127.0.0.1',resolve)});
async function until(predicate,message){const deadline=Date.now()+1500;while(!predicate()){if(Date.now()>deadline)throw Error(message);await new Promise(r=>setTimeout(r,10))}}
async function fixture(t){
 const {createServer}=await import('../server.mjs'),source=createServer(),requests=[],secureSockets=new Set(),tunnels=new Set();
 const origin=https.createServer({cert:fs.readFileSync(cert),key:fs.readFileSync(key)},(req,res)=>{requests.push({route:req.url,peer:req.socket.remotePort});source.emit('request',req,res)});
 origin.on('secureConnection',socket=>{secureSockets.add(socket);socket.once('close',()=>secureSockets.delete(socket))});
 origin.on('tlsClientError',()=>{});await listen(origin);
 const proxies=await Promise.all([0,1].map(async()=>{
  const server=http.createServer();let connects=0;
  server.on('connect',(req,client,head)=>{
   if(!['127.0.0.1','127.0.0.2'].some(host=>req.url===`${host}:${origin.address().port}`)){client.end('HTTP/1.1 403 Forbidden\r\n\r\n');return}
   connects++;tunnels.add(client);client.once('close',()=>tunnels.delete(client));
   const upstream=net.connect(origin.address().port,'127.0.0.1',()=>{client.write('HTTP/1.1 200 Connection Established\r\n\r\n');if(head.length)upstream.write(head);client.pipe(upstream);upstream.pipe(client)});
   client.on('error',()=>upstream.destroy());upstream.on('error',()=>client.destroy());client.once('close',()=>upstream.destroy());upstream.once('close',()=>client.destroy());
  });await listen(server);return {server,port:server.address().port,get connects(){return connects}};
 }));
 t.after(()=>{for(const socket of [...secureSockets,...tunnels])socket.destroy();origin.closeAllConnections();origin.close();for(const {server}of proxies)server.close()});
 return {base:`https://127.0.0.1:${origin.address().port}`,proxies,requests,secureSockets,tunnels};
}
function client(t,source,mode,ports,trusted=true){
 const env={...process.env,NODE_TLS_REJECT_UNAUTHORIZED:'1'};
 if(trusted)env.NODE_EXTRA_CA_CERTS=cert;else delete env.NODE_EXTRA_CA_CERTS;
 const child=fork(path.join(__dirname,'fixtures/https-client.cjs'),[source.base,mode,...ports.map(String)],{env,execArgv:[],windowsHide:true,stdio:['ignore','ignore','pipe','ipc']});
 let stderr='',exited=false;child.stderr.on('data',data=>stderr+=data);child.once('exit',()=>exited=true);
 const result=new Promise((resolve,reject)=>{child.once('error',reject);child.on('message',message=>{if(message.type==='result')resolve(message)});child.once('exit',(code)=>reject(Error(`HTTPS fixture child exited ${code}: ${stderr}`)))});
 t.after(()=>{if(!exited)child.kill()});
 return {child,result,get exited(){return exited},async stop(){const exit=new Promise(resolve=>child.once('exit',resolve));child.send('stop');assert.equal(await exit,0,stderr)}};
}
async function assertClosedWhileChildAlive(source,child){
 await until(()=>source.secureSockets.size===0&&source.tunnels.size===0,'HTTPS socket or CONNECT tunnel retained after probe');
 assert.equal(child.exited,false,'Process exit must not hide a transport leak');
}
test('trusted HTTPS CONNECT completes all 401 frames, isolates lanes, preserves fresh mode and closes before child exit',{timeout:40000},async t=>{
 const source=await fixture(t),ports=[source.proxies[0].port,source.proxies[1].port,source.proxies[0].port];
 const child=client(t,source,'mixed',ports),started=Date.now(),result=await child.result;
 assert.equal(result.error,undefined,JSON.stringify(result.error));assert.equal(result.values.length,3);
 for(const value of result.values){assert.equal(value.ok,true,JSON.stringify(value));assert.equal(value.stream.receivedSamples,401);assert.equal(value.stream.flowPass,true);assert.ok(value.stream.sourceSlipMs<=250);assert.equal(value.measurement,'sse-only');assert.equal(value.download,undefined)}
 assert.ok(Date.now()-started>=19900,'Full source window must not be shortened');
 assert.deepEqual(result.values.map(v=>v.connectionMode),['reused','reused','fresh']);assert.notEqual(result.values[0].profileKey,result.values[2].profileKey);
 assert.deepEqual(source.proxies.map(p=>p.connects),[3,1]);
 assert.equal(source.requests.filter(r=>r.route==='/api/manifest').length,3);assert.equal(source.requests.filter(r=>r.route==='/api/stream').length,3);assert.equal(source.requests.length,6);
 const peers=new Map();for(const {peer}of source.requests)peers.set(peer,(peers.get(peer)||0)+1);assert.deepEqual([...peers.values()].sort(),[1,1,2,2]);
 await assertClosedWhileChildAlive(source,child);await child.stop();
});
test('HTTPS certificate validation is not disabled by CONNECT keep-alive',{timeout:10000},async t=>{
 const source=await fixture(t),child=client(t,source,'untrusted',[source.proxies[0].port],false),result=await child.result;
 assert.ok(['DEPTH_ZERO_SELF_SIGNED_CERT','SELF_SIGNED_CERT_IN_CHAIN','UNABLE_TO_VERIFY_LEAF_SIGNATURE'].includes(result.error?.code),JSON.stringify(result));
 assert.equal(source.requests.length,0);assert.equal(source.proxies[0].connects,1);
 await assertClosedWhileChildAlive(source,child);await child.stop();
});
test('HTTPS hostname verification uses the requested IP, not localhost, in fresh and reused tunnels',{timeout:10000},async t=>{
 const source=await fixture(t),target={...source,base:source.base.replace('127.0.0.1','127.0.0.2')};
 const child=client(t,target,'identity',source.proxies.map(p=>p.port)),result=await child.result;
 assert.equal(result.error,undefined,JSON.stringify(result.error));
 for(const value of result.values)assert.equal(value.error?.code,'ERR_TLS_CERT_ALTNAME_INVALID',JSON.stringify(value));
 assert.equal(source.requests.length,0,'Wrong-host certificates must be rejected before sending HTTP');
 await assertClosedWhileChildAlive(source,child);await child.stop();
});
test('cancelling established HTTPS SSE closes the live TLS response and CONNECT tunnel',{timeout:10000},async t=>{
 const source=await fixture(t),child=client(t,source,'cancel',[source.proxies[0].port]);
 await until(()=>source.requests.some(r=>r.route==='/api/stream'),'HTTPS SSE did not start');
 await new Promise(r=>setTimeout(r,100));child.child.send('cancel');const result=await child.result;
 assert.equal(result.error?.scope,'cancelled',JSON.stringify(result));assert.equal(source.proxies[0].connects,1);assert.equal(source.requests.length,2);
 await assertClosedWhileChildAlive(source,child);await child.stop();
});
