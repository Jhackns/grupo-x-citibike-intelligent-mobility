#!/usr/bin/env python3
"""Exportador stdlib en Docker: no tiene socket Docker ni credenciales PostgreSQL."""
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from pathlib import Path
import signal
import threading
import time

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        p=self.server.metricas_path
        if self.path!='/metrics':self.send_error(404);return
        if not p.is_file() or time.time()-p.stat().st_mtime>30:self.send_error(503,'Colector sin datos recientes');return
        raw=p.read_bytes();self.send_response(200)
        self.send_header('Content-Type','text/plain; version=0.0.4; charset=utf-8')
        self.send_header('Content-Length',str(len(raw)));self.end_headers();self.wfile.write(raw)
    def log_message(self,*args):pass

def main(host='0.0.0.0',port=9108,archivo=Path('/estado/metricas.prom')):
    with ThreadingHTTPServer((host,port),Handler) as server:
        server.metricas_path=Path(archivo)
        def cerrar(*_):
            # shutdown debe ejecutarse desde otro hilo; en el hilo de serve_forever
            # produciría un interbloqueo. SIGTERM debe manejarse también siendo PID 1.
            threading.Thread(target=server.shutdown,daemon=True).start()
        for sig in (signal.SIGTERM,signal.SIGINT):signal.signal(sig,cerrar)
        server.serve_forever(poll_interval=0.2)

if __name__=='__main__':main()
