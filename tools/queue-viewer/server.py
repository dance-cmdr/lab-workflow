"""Local dashboard and read-only proxy; the assessment application is untouched."""
import argparse
import json
import re
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import urlopen


class Handler(SimpleHTTPRequestHandler):
    def do_GET(self):
        path = urlsplit(self.path).path
        if path.startswith('/api/'):
            target = path[4:]
            if not re.fullmatch(r'/(health|runs|drivers|devices|workflows|runs/[A-Za-z0-9_-]+(?:/timeline)?)', target):
                self.send_error(404)
                return
            try:
                with urlopen('http://127.0.0.1:5001' + target, timeout=12) as response:
                    data, status = response.read(), response.status
            except HTTPError as exc:
                data, status = exc.read(), exc.code
            except (URLError, TimeoutError, OSError):
                data, status = json.dumps({'error': 'Assessment API unavailable on port 5001'}).encode(), 502
            self.send_response(status)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Cache-Control', 'no-store')
            self.end_headers()
            self.wfile.write(data)
            return
        if path not in ('/', '/index.html', '/app.js', '/style.css', '/model.mjs', '/pipeline.mjs', '/timeline.mjs', '/assets/automata-logo.svg', '/assets/roboto-regular.woff2', '/assets/jetbrains-mono.woff2'):
            self.send_error(404)
            return
        super().do_GET()

    def end_headers(self):
        self.send_header('Cache-Control', 'no-store')
        self.send_header('X-Content-Type-Options', 'nosniff')
        super().end_headers()

    def log_message(self, format, *args):
        if args and str(args[1]) not in ('200', '304'):
            super().log_message(format, *args)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', type=int, default=5010)
    args = parser.parse_args()
    handler = partial(Handler, directory=str(Path(__file__).parent))
    server = ThreadingHTTPServer(('127.0.0.1', args.port), handler)
    print(f'Queue viewer: http://localhost:{args.port} (read-only; API on port 5001)', flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.server_close()
