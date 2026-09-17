const {test}=require('node:test'),assert=require('node:assert/strict'),http=require('node:http');
const {request,transportSession}=require('../stream-quality-runner.cjs');

test('per-lane keep-alive reuses one connection and rejects cross-origin or cross-proxy reuse',async t=>{
 const peers=new Set();let requests=0;
 const server=http.createServer((req,res)=>{peers.add(req.socket.remotePort);requests++;res.end('ok')});
 await new Promise(r=>server.listen(0,'127.0.0.1',r));t.after(()=>{server.closeAllConnections();server.close()});
 const base=`http://127.0.0.1:${server.address().port}`,s=transportSession(base);
 try{for(let i=0;i<3;i++)assert.equal((await request(base,'/',{session:s,maxBytes:8})).bytes,2);
  assert.equal(peers.size,1);assert.equal(requests,3);
  await assert.rejects(request(base,'/',{session:s,port:1,maxBytes:8}),/route mismatch/);
  await assert.rejects(request('http://localhost:9','/',{session:s,maxBytes:8}),/route mismatch/);
 }finally{s.destroy()}
 const s2=transportSession(base);try{await request(base,'/',{session:s2,maxBytes:8});assert.equal(peers.size,2)}finally{s2.destroy()}
});
test('keep-alive cancellation closes response socket and all owned resources',async t=>{
 let closed=false;const server=http.createServer((_req,res)=>{res.write('partial');res.once('close',()=>closed=true)});
 await new Promise(r=>server.listen(0,'127.0.0.1',r));t.after(()=>{server.closeAllConnections();server.close()});
 const base=`http://127.0.0.1:${server.address().port}`,s=transportSession(base),c=new AbortController();
 const timer=setTimeout(()=>c.abort(),50);try{await assert.rejects(request(base,'/',{session:s,signal:c.signal,maxBytes:100}),e=>e.failureScope==='cancelled')}
 finally{clearTimeout(timer);s.destroy()}
 await new Promise(r=>setTimeout(r,30));assert.equal(closed,true);
});
test('keep-alive session cancellation also closes pending HTTPS CONNECT and TLS handshake',async t=>{
 let peer,connected;const ready=new Promise(r=>connected=r);const server=http.createServer();
 server.on('connect',(_req,socket)=>{peer=socket;socket.write('HTTP/1.1 200 Connection Established\r\n\r\n');socket.on('data',()=>{});socket.on('end',()=>socket.end());socket.on('error',()=>{});connected()});
 await new Promise(r=>server.listen(0,'127.0.0.1',r));t.after(()=>{peer?.destroy();server.close()});
 const port=server.address().port,base='https://example.test',s=transportSession(base,port),c=new AbortController();
 const pending=request(base,'/',{port,session:s,signal:c.signal,maxBytes:100});
 await ready;const closed=new Promise(r=>peer.once('close',r));const check=assert.rejects(pending,e=>e.failureScope==='cancelled');c.abort();await check;s.destroy();
 await Promise.race([closed,new Promise((_,reject)=>{const t=setTimeout(()=>reject(Error('CONNECT socket retained')),700);t.unref()})]);
});
