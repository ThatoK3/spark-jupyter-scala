c.ServerApp.ip = '0.0.0.0'
c.ServerApp.port = 8888
c.ServerApp.open_browser = False
c.ServerApp.allow_root = True
c.ServerApp.allow_origin = '*'
c.ServerApp.allow_remote_access = True
c.ServerApp.disable_check_xsrf = True
c.ServerApp.token = ''
c.ServerApp.password = ''
c.ServerApp.notebook_dir = '/home/jovyan/work'

c.ContentsManager.allow_hidden = True
c.FileContentsManager.delete_to_trash = False

# Enable widgets
c.ServerApp.jpserver_extensions = {
    'jupyterlab': True,
    'jupyter_spark': True
}
