import cherrypy
import httplib
import traceback

from components.webserver.test_db import TestDB

class Webserver(object):
  db = TestDB()
  @cherrypy.expose
  def index(self):
    return "Hello world! It's working!!!"

  @cherrypy.expose
  @cherrypy.tools.json_in()
  @cherrypy.tools.json_out()
  def execute(self):
    data = cherrypy.request.json
    try:
      ret = self.db.perform_operation(**data)
      cherrypy.response.status = httplib.OK
      res = {"ret_val": ret}
    except Exception:
      cherrypy.response.status = httplib.BAD_REQUEST
      res = {"error": traceback.format_exc()}
    return res

  @cherrypy.expose
  @cherrypy.tools.json_out()
  def data(self):
    try:
      ret = self.db.get_whole_db()
      cherrypy.response.status = httplib.OK
      res = {"ret_val": ret}
    except Exception:
      cherrypy.response.status = httplib.BAD_REQUEST
      res = {"error": traceback.format_exc()}
    return res

if __name__ == '__main__':
  cherrypy.quickstart(Webserver())