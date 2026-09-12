"""DSS150P local paginated REST API.
Run: python src/local_api_server.py
Examples:
  http://127.0.0.1:8000/api/events?page=1&per_page=20
  http://127.0.0.1:8000/api/events?updated_after=2026-08-05T00:00:00&page=1&per_page=20
"""
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs
from pathlib import Path
import json
from datetime import datetime

DATA = json.loads((Path(__file__).resolve().parents[1]/'data'/'api_events.json').read_text(encoding='utf-8'))

def parse_dt(s):
    return datetime.fromisoformat(s) if s else None

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        u=urlparse(self.path)
        if u.path != '/api/events':
            self.send_response(404); self.end_headers(); return
        q=parse_qs(u.query)
        page=max(1,int(q.get('page',['1'])[0])); per_page=min(50,max(1,int(q.get('per_page',['20'])[0])))
        updated_after=q.get('updated_after',[None])[0]
        rows=DATA
        if updated_after:
            cutoff=parse_dt(updated_after)
            rows=[r for r in rows if parse_dt(r['updated_at']) > cutoff]
        rows=sorted(rows,key=lambda r:(r['updated_at'],r['event_id']))
        start=(page-1)*per_page; items=rows[start:start+per_page]
        total=len(rows); has_more=start+per_page < total
        payload={'page':page,'per_page':per_page,'total':total,'has_more':has_more,'next_page':page+1 if has_more else None,'items':items}
        b=json.dumps(payload).encode('utf-8')
        self.send_response(200); self.send_header('Content-Type','application/json'); self.send_header('Content-Length',str(len(b))); self.end_headers(); self.wfile.write(b)
    def log_message(self, format, *args): pass

if __name__=='__main__':
    print('API: http://127.0.0.1:8000/api/events?page=1&per_page=20')
    HTTPServer(('127.0.0.1',8000),Handler).serve_forever()
