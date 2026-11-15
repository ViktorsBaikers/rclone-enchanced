import http.server, threading, tempfile, subprocess, pathlib, time, os, hashlib
import sys
binary=sys.argv[1] if len(sys.argv)>1 else './rclone'
content=bytes(range(256))*32768
class Handler(http.server.BaseHTTPRequestHandler):
 def log_message(self,*args): pass
 def do_HEAD(self):
  self.send_response(200);self.send_header('Content-Length',str(len(content)));self.end_headers()
 def do_GET(self):
  if self.path=='/':
   body=b'<a href="good.bin">good.bin</a><a href="bad.bin">bad.bin</a>'
   self.send_response(200);self.send_header('Content-Type','text/html');self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body);return
  a,b=0,len(content)-1
  if self.headers.get('Range'):
   x=self.headers['Range'].removeprefix('bytes=').split('-');a=int(x[0]);b=min(int(x[1]) if x[1] else b,b)
  self.send_response(206);self.send_header('Content-Range',f'bytes {a}-{b}/{len(content)}')
  if self.path=='/bad.bin':
   self.send_header('Transfer-Encoding','chunked');self.end_headers();self.wfile.write(b'0\r\n\r\n');return
  self.send_header('Content-Length',str(b-a+1));self.end_headers()
  try:self.wfile.write(content[a:b+1])
  except (BrokenPipeError,ConnectionResetError):pass
server=http.server.ThreadingHTTPServer(('127.0.0.1',0),Handler)
threading.Thread(target=server.serve_forever,daemon=True).start()
with tempfile.TemporaryDirectory(prefix='rclone-mount-check-') as root:
 mount=pathlib.Path(root)/'mnt';mount.mkdir()
 log=open(root+'/mount-check.log','w')
 p=subprocess.Popen([binary,'mount',':http:', '--http-url',f'http://127.0.0.1:{server.server_port}/',str(mount),'--read-only','--vfs-cache-mode','full','--cache-dir',root+'/cache','--vfs-read-chunk-streams','4','--vfs-read-chunk-size','4M','--timeout','2s','--low-level-retries','1','--log-level','DEBUG'],stdout=log,stderr=log)
 try:
  for _ in range(100):
   if os.path.ismount(mount):break
   if p.poll() is not None:raise RuntimeError('mount failed; inspect log')
   time.sleep(.1)
  assert os.path.ismount(mount)
  # All file operations execute in killable, bounded child processes.
  r=subprocess.run(['head','-c','65536',str(mount/'bad.bin')],capture_output=True,timeout=45)
  assert r.returncode!=0,(r.returncode,len(r.stdout))
  print('BROKEN READ returned error:',r.stderr.decode().strip())
  r=subprocess.run(['cat',str(mount/'good.bin')],capture_output=True,timeout=15)
  assert r.returncode==0 and r.stdout==content
  print('HEALTHY READ exact bytes:',len(r.stdout),'sha256:',hashlib.sha256(r.stdout).hexdigest())
  print('PASS: broken range did not hang read or subsequent healthy file')
 finally:
  p.terminate()
  try:p.wait(timeout=10)
  except subprocess.TimeoutExpired:p.kill();p.wait()
  subprocess.run(['fusermount3','-uz',str(mount)],capture_output=True)
  server.shutdown();log.close()
