import sys
import json
import cgi
from http.server import HTTPServer, BaseHTTPRequestHandler
sys.path.append("..")
from libs import tools
from libs.constants import *


class RequestHeandler(BaseHTTPRequestHandler):
    def _set_headers_ok(self):
        self.send_response(200)
        self.send_header("Content-type", "application/json; charset=utf-8")
        self.end_headers()

    def _set_headers_err(self, err_code):
        self.send_response(err_code)
        self.send_header("Content-type", "text/html")
        self.end_headers()

    def _html(self, message):
        """This just generates an HTML document that includes `message`
        in the body. Override, or re-write this do do more interesting stuff.
        """
        content = f"{message}"
        return content.encode("utf8")  # NOTE: must return a bytes object!

    def do_POST(self):
        path = self.path.split("?")[0]
        length = self.headers['content-length']
        if not length:
            self._set_headers_err(400)
            self.wfile.write(self._html("no data"))
            return

        form = cgi.FieldStorage(
            fp=self.rfile,
            headers=self.headers,
            environ={
                "REQUEST_METHOD": "POST",
                "CONTENT_TYPE":   self.headers['Content-Type']
            })

        try:
            twits = json.loads(form.list[0].file.read().decode(ENCODING), encoding=ENCODING)
        except Exception:
            self._set_headers_err(400)
            self.wfile.write(self._html("bad data. expect utf-8 encoded json"))
            return

        path = path.lower()
        if path == "/stat":
            stat = tools.TwitsStatCalculator(twits).get_full_report()
        elif path == "/enti":
            stat = tools.EntityExtractor(twits).extract_entities()
        else:
            self._set_headers_err(404)
            self.wfile.write(self._html("not found"))
            return

        self._set_headers_ok()
        self.wfile.write(self._html(json.dumps(stat, ensure_ascii=False)))


def run(server_class=HTTPServer, handler_class=RequestHeandler, addr="localhost", port=SERVER_PORT):
    server_address = (addr, port)
    httpd = server_class(server_address, handler_class)

    print(f"Starting httpd server on {addr}:{port}")
    httpd.serve_forever()


if __name__ == '__main__':
    run()
