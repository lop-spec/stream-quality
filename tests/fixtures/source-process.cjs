// Separate process: the timing source must not share the client's event loop.
(async()=>{
 const { createServer }=await import('../../server.mjs');const server=createServer();const routes={};let active=0,peak=0;
 server.on('request',(req,res)=>{routes[req.url]=(routes[req.url]||0)+1;if(req.url==='/api/stream'){peak=Math.max(peak,++active);res.once('close',()=>active--)}});
 server.listen(0,'127.0.0.1',()=>process.send({type:'ready',port:server.address().port}));
 process.on('message',m=>{if(m==='stats')process.send({type:'stats',routes,active,peak});if(m==='stop'){server.closeAllConnections();server.close(()=>process.disconnect())}});
 process.on('disconnect',()=>{server.closeAllConnections();server.close()});
})().catch(e=>{console.error(e.message);process.exitCode=1});
