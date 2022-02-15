Server for Vitex-stats website
==============================
This is the server side of vitex-stats website.


Launch debug server
-------------------

PowerShell
```
> $env:FLASK_APP = "vitex_stats_server"
> $env:FLASK_ENV = "development"
> flask run
```

CMD
```
> set FLASK_APP=vitex_stats_server
> set FLASK_ENV=development
> flask run
```

Bash
```
$ export FLASK_APP=vitex_stats_server
$ export FLASK_ENV=development
$ flask run
```

Launch download task
------------------------
```
flask manage download-chunk-auto 11979617 11979920
```