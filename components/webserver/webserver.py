import cherrypy

from components.webserver.test_db import TestDB

class Webserver(object):
  @cherrypy.expose
  def index(self):
    return "Hello world! It's working!!!"

  @cherrypy.expose
  def exit_test(self):
    TestDB.add_key_value(key="test_args.exit_test", value=True)

if __name__ == '__main__':
  cherrypy.quickstart(Webserver())