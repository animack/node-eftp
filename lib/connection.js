var fs = require('fs'),
    tls = require('tls'),
    zlib = require('zlib'),
    Socket = require('net').Socket,
    EventEmitter = require('events').EventEmitter,
    inherits = require('util').inherits,
    inspect = require('util').inspect,
    StringDecoder = require('string_decoder').StringDecoder;

var Parser = require('./parser');
var XRegExp = require('xregexp').XRegExp;

var REX_TIMEVAL = XRegExp.cache('^(?<year>\\d{4})(?<month>\\d{2})(?<date>\\d{2})(?<hour>\\d{2})(?<minute>\\d{2})(?<second>\\d+)(?:.\\d+)?$'),
    RE_PASV = /([\d]+),([\d]+),([\d]+),([\d]+),([-\d]+),([-\d]+)/,
    RE_EOL = /\r?\n/g,
    RE_WD = /"(.+)"(?: |$)/,
    RE_SYST = /^([^ ]+)(?: |$)/;

var /*TYPE = {
      SYNTAX: 0,
      INFO: 1,
      SOCKETS: 2,
      AUTH: 3,
      UNSPEC: 4,
      FILESYS: 5
    },*/
    RETVAL = {
      PRELIM: 1,
      OK: 2,
      WAITING: 3,
      ERR_TEMP: 4,
      ERR_PERM: 5
    },
    /*ERRORS = {
      421: 'Service not available, closing control connection',
      425: 'Can\'t open data connection',
      426: 'Connection closed; transfer aborted',
      450: 'Requested file action not taken / File unavailable (e.g., file busy)',
      451: 'Requested action aborted: local error in processing',
      452: 'Requested action not taken / Insufficient storage space in system',
      500: 'Syntax error / Command unrecognized',
      501: 'Syntax error in parameters or arguments',
      502: 'Command not implemented',
      503: 'Bad sequence of commands',
      504: 'Command not implemented for that parameter',
      530: 'Not logged in',
      532: 'Need account for storing files',
      550: 'Requested action not taken / File unavailable (e.g., file not found, no access)',
      551: 'Requested action aborted: page type unknown',
      552: 'Requested file action aborted / Exceeded storage allocation (for current directory or dataset)',
      553: 'Requested action not taken / File name not allowed'
    },*/
    bytesNOOP = new Buffer('NOOP\r\n');

var FTP = module.exports = function() {
  if (!(this instanceof FTP))
    return new FTP();
  
  this._worker = {};
  this._queue = [];
  this._sockets = 0;
  this._ending = false;
  
  this.options = {
    host: undefined,
    port: undefined,
    user: undefined,
    password: undefined,
    secure: false,
    secureOptions: undefined,
    connTimeout: undefined,
    pasvTimeout: undefined,
    aliveTimeout: undefined,
    maxSockets: 5
  };
  this.connected = false;
  this.ready = false;
};
inherits(FTP, EventEmitter);

FTP.prototype.connect = function(options) {
  var self = this;
  if (typeof options !== 'object')
    options = {};
  this.connected = false;
  this.options.host = options.host || 'localhost';
  this.options.port = options.port || 21;
  this.options.user = options.user || 'anonymous';
  this.options.password = options.password ||
      options.password === '' ? options.password
      : 'anonymous@';
  this.options.secure = options.secure || false;
  this.options.secureOptions = options.secureOptions;
  this.options.connTimeout = options.connTimeout || 10000;
  this.options.pasvTimeout = options.pasvTimeout || 10000;
  this.options.aliveTimeout = options.keepalive || 10000;

  if (typeof options.debug === 'function') {
    this._debug = options.debug;
  }
  
  if (this.options.secure) {
    this.options.secureOptions = this.options.secureOptions || {};
    this.options.secureOptions.host = this.options.host;
  }
  
  var handleSockets = function() {
		var time = new Date().getTime();
		for (var i in self._worker) {
			if (time - self._worker[i].lastActive > 10000 && self._sockets > 1 && !self._worker[i].getQueue().length && !self._worker[i].currentRequest) {
				self._reset(i);
			}
		}
		
		self._multiSocketHandler = setTimeout(handleSockets, 1000);
	};
	
	handleSockets();
  
  this._createWorker();
};

FTP.prototype._createWorker = function(onSuccess) {
  var self = this;
  
  var debug = this._debug,
      socket = new Socket();

  socket.setTimeout(0);
  socket.setKeepAlive(true);
  
  var id = 1;
	
	while (this._worker[id]) {
		id++;
	}
  
  var worker = this._worker[id] = {
		socket: null,
		parser: null,
		pasvSock: null,
		pasvGetting: false,
		pasvProcessing: false,
		feat: null,
		currentRequest: null,
		secondState: null,
		keepAlive: null,
		lastActive: new Date().getTime(),
		ready: false,
		getQueue: function() {
			return self._queue.filter(function(data) {
				return data.id === id;
			});
		}
	};

  worker.parser = new Parser({ debug: debug });
  worker.parser.on('response', function(code, text) {
    var retval = code / 100 >> 0;
    if (retval === RETVAL.ERR_TEMP || retval === RETVAL.ERR_PERM) {
      if (worker.currentRequest)
        worker.currentRequest.cb(makeError(code, text), undefined, code);
      else
        self.emit('error', makeError(code, text));
    } else if (worker.currentRequest) {
      worker.currentRequest.cb(undefined, text, code);
    }

    // a hack to signal we're waiting for a PASV data connection to complete
    // first before executing any more queued requests ...
    //
    // also: don't forget our current request if we're expecting another
    // terminating response ....
    if (worker.currentRequest && retval !== RETVAL.PRELIM) {
      worker.currentRequest = undefined;
      self._sendc(id);
    }

    noopreq.cb();
  });
  
  var secureOptions;

  if (this.options.secure) {
    secureOptions = $.extend({}, this.options.secureOptions);
    secureOptions.socket = socket;
    this.options.secureOptions = secureOptions;
  }

  if (this.options.secure === 'implicit') {
    worker.socket = tls.connect(secureOptions, onconnect);
  } else {
    socket.once('connect', onconnect);
    worker.socket = socket;
  }

  var noopreq = {
    cmd: 'NOOP',
    cb: function() {
      clearTimeout(worker.keepAlive);
      worker.keepAlive = setTimeout(donoop, self.options.aliveTimeout);
    }
  };

  function donoop() {
    if (!worker.socket || !worker.socket.writable) {
      clearTimeout(worker.keepAlive);
    } else if (!worker.currentRequest && self._queue.length === 0) {
      worker.currentRequest = noopreq;
      debug&&debug('[connection] > NOOP');
      worker.socket.write(bytesNOOP);
    } else {
      noopreq.cb();
    }
  }

  function onconnect() {
    clearTimeout(timer);
    clearTimeout(worker.keepAlive);
    self.connected = true;
    worker.socket = socket; // re-assign for implicit secure connections

    var cmd;

    if (worker.secondState) {
      if (worker.secondState === 'upgraded-tls' && self.options.secure === true) {
        cmd = 'PBSZ';
        self._sendc(id, 'PBSZ 0', reentry, true);
      } else {
        cmd = 'USER';
        self._sendc(id, 'USER ' + self.options.user, reentry, true);
      }
    } else {
      worker.currentRequest = {
        cmd: '',
        cb: reentry
      };
    }

    function reentry(err, text, code) {
      if (err && (!cmd || cmd === 'USER' || cmd === 'PASS' || cmd === 'TYPE')) {
        self.emit('error', err);
        return worker.socket && worker.socket.end();
      }
      if ((cmd === 'AUTH TLS' && code !== 234 && self.options.secure !== true)
          || (cmd === 'AUTH SSL' && code !== 334)
          || (cmd === 'PBSZ' && code !== 200)
          || (cmd === 'PROT' && code !== 200)) {
        self.emit('error', makeError(code, 'Unable to secure connection(s)'));
        return worker.socket && worker.socket.end();
      }

      if (!cmd) {
        // sometimes the initial greeting can contain useful information
        // about authorized use, other limits, etc.
        self.emit('greeting', text);

        if (self.options.secure && self.options.secure !== 'implicit') {
          cmd = 'AUTH TLS';
          self._sendc(id, cmd, reentry, true);
        } else {
          cmd = 'USER';
          self._sendc(id, 'USER ' + self.options.user, reentry, true);
        }
      } else if (cmd === 'USER') {
        if (code !== 230) {
          // password required
          if (!self.options.password &&
               self.options.password !== '') {
            self.emit('error', makeError(code, 'Password required'));
            return worker.socket && worker.socket.end();
          }
          cmd = 'PASS';
          self._sendc(id, 'PASS ' + self.options.password, reentry, true);
        } else {
          // no password required
          cmd = 'PASS';
          reentry(undefined, text, code);
        }
      } else if (cmd === 'PASS') {
        cmd = 'FEAT';
        self._sendc(id, cmd, reentry, true);
      } else if (cmd === 'FEAT') {
        if (!err)
          worker.feat = Parser.parseFeat(text);
        cmd = 'TYPE';
        self._sendc(id, 'TYPE I', reentry, true);
      } else if (cmd === 'TYPE') {
        worker.ready = true;
				if (!self.ready) {
					self.ready = true;
					self.emit('ready');
				}
				
				if (typeof onSuccess == 'function') {
					onSuccess(id);
				}
      } else if (cmd === 'PBSZ') {
        cmd = 'PROT';
        self._sendc(id, 'PROT P', reentry, true);
      } else if (cmd === 'PROT') {
        cmd = 'USER';
        self._sendc(id, 'USER ' + self.options.user, reentry, true);
      } else if (cmd.substr(0, 4) === 'AUTH') {
        if (cmd === 'AUTH TLS' && code !== 234) {
          cmd = 'AUTH SSL';
          return self._sendc(id, cmd, reentry, true);
        } else if (cmd === 'AUTH TLS')
          worker.secondState = 'upgraded-tls';
        else if (cmd === 'AUTH SSL')
          worker.secondState = 'upgraded-ssl';
        socket.removeAllListeners('data');
        socket.removeAllListeners('error');
        socket._decoder = null;
        worker.currentRequest = null; // prevent queue from being processed during
                             // TLS/SSL negotiation
        secureOptions.socket = worker.socket;
        secureOptions.session = undefined;
        socket = tls.connect(secureOptions, onconnect);
        socket.setEncoding('binary');
        socket.on('data', ondata);
        socket.once('end', onend);
        socket.on('error', onerror);
      }
    }
  }

  socket.on('data', ondata);
  function ondata(chunk) {
    debug&&debug('[connection] < ' + inspect(chunk.toString('binary')));
    if (worker.parser)
      worker.parser.write(chunk);
  }

  socket.on('error', onerror);
  function onerror(err) {
    clearTimeout(timer);
    clearTimeout(worker.keepAlive);
    self.emit('error', err);
  }

  socket.once('end', onend);
  function onend() {
    ondone();
    self.emit('end');
  }

  socket.once('close', function(had_err) {
    ondone();
    self.emit('close', had_err);
  });

  var hasReset = false;
  function ondone() {
    if (!hasReset) {
      hasReset = true;
      clearTimeout(timer);
      self._reset(id);
    }
  }

  var timer = setTimeout(function() {
    self.emit('error', new Error('Timeout while connecting to server'));
    worker.socket && worker.socket.destroy();
    self._reset(id);
  }, this.options.connTimeout);

  worker.socket.connect(this.options.port, this.options.host);
  
  this._sockets++;
};

FTP.prototype.end = function() {
  if (this._queue.length) {
    this._ending = true;
  } else {
    this._reset();
  }
};

FTP.prototype.destroy = function() {
  this._reset();
};

// "Standard" (RFC 959) commands
FTP.prototype.ascii = function(cb) {
  return this._sendc(true, 'TYPE A', cb);
};

FTP.prototype.binary = function(cb) {
  return this._sendc(true, 'TYPE I', cb);
};

FTP.prototype.abort = function(immediate, cb) {
  if (typeof immediate === 'function') {
    cb = immediate;
    immediate = true;
  }
  if (immediate) {
    this._sendc(true, 'ABOR', cb, true);
  } else {
    this._sendc(true, 'ABOR', cb);
  }
};

FTP.prototype.cwd = function(path, cb, promote) {
  this._sendc(true, 'CWD ' + path, function(err, text, code) {
    if (err)
      return cb(err);
    var m = RE_WD.exec(text);
    cb(undefined, m ? m[1] : undefined);
  }, promote);
};

FTP.prototype.delete = function(path, cb) {
  this._send('DELE ' + path, cb);
};

FTP.prototype.site = function(cmd, cb) {
  this._send('SITE ' + cmd, cb);
};

FTP.prototype.status = function(cb) {
  this._send('STAT', cb);
};

FTP.prototype.rename = function(from, to, cb) {
  var self = this;
  this._send('RNFR ' + from, function(err) {
    if (err)
      return cb(err);

    self._sendc(this.id, 'RNTO ' + to, cb, true);
  });
};

FTP.prototype.logout = function(cb) {
  this._sendc(true, 'QUIT', cb);
};

FTP.prototype.listSafe = function(path, zcomp, cb) {
  if (typeof path === 'string') {
    var self = this;
    // store current path
    this.pwd(function(err, origpath) {
      if (err) return cb(err);
      // change to destination path
      self.cwd(path, function(err) {
        if (err) return cb(err);
        // get dir listing
        self.list(zcomp || false, function(err, list) {
          // change back to original path
          if (err) return self.cwd(origpath, cb);
          self.cwd(origpath, function(err) {
            if (err) return cb(err);
            cb(err, list);
          });
        });
      });
    });
  } else
    this.list(path, zcomp, cb);
};

FTP.prototype.list = function(path, zcomp, cb) {
  var self = this, cmd;

  if (typeof path === 'function') {
    // list(function() {})
    cb = path;
    path = undefined;
    cmd = 'LIST';
    zcomp = false;
  } else if (typeof path === 'boolean') {
    // list(true, function() {})
    cb = zcomp;
    zcomp = path;
    path = undefined;
    cmd = 'LIST';
  } else if (typeof zcomp === 'function') {
    // list('/foo', function() {})
    cb = zcomp;
    cmd = 'LIST ' + path;
    zcomp = false;
  } else
    cmd = 'LIST ' + path;

  this._pasv(function(err, sock, id) {
    if (err)
      return cb(err);

    if (self._worker[id].getQueue()[0] && self._worker[id].getQueue()[0].cmd === 'ABOR') {
      sock.destroy();
      return cb();
    }

    var sockerr, done = false, replies = 0, entries, buffer = '', source = sock;
    var decoder = new StringDecoder('utf8');

    if (zcomp) {
      source = zlib.createInflate();
      sock.pipe(source);
    }

    source.on('data', function(chunk) {
      buffer += decoder.write(chunk);
    });
    source.once('error', function(err) {
      if (!sock.aborting)
        sockerr = err;
    });
    source.once('end', ondone);
    source.once('close', ondone);

    function ondone() {
      if (decoder) {
        buffer += decoder.end();
        decoder = null;
      }
      done = true;
      final();
    }
    function final() {
      if (done && replies === 2) {
        replies = 3;
        if (sockerr)
          return cb(new Error('Unexpected data connection error: ' + sockerr));
        if (sock.aborting)
          return cb();

        // process received data
        entries = buffer.split(RE_EOL);
        entries.pop(); // ending EOL
        var parsed = [];
        for (var i = 0, len = entries.length; i < len; ++i) {
          var parsedVal = Parser.parseListEntry(entries[i]);
          if (parsedVal !== null)
            parsed.push(parsedVal);
        }

        if (zcomp) {
          self._sendc(id, 'MODE S', function() {
            cb(undefined, parsed);
          }, true);
        } else
          cb(undefined, parsed);
      }
    }

    if (zcomp) {
      self._sendc(id, 'MODE Z', function(err, text, code) {
        if (err) {
          sock.destroy();
          return cb(makeError(code, 'Compression not supported'));
        }
        sendList();
      }, true);
    } else
      sendList();

    function sendList() {
      // this callback will be executed multiple times, the first is when server
      // replies with 150 and then a final reply to indicate whether the
      // transfer was actually a success or not
      self._sendc(id, cmd, function(err, text, code) {
        if (err) {
          sock.destroy();
          if (zcomp) {
            self._sendc(id, 'MODE S', function() {
              cb(err);
            }, true);
          } else
            cb(err);
          return;
        }

        // some servers may not open a data connection for empty directories
        if (++replies === 1 && code === 226) {
          replies = 2;
          sock.destroy();
          final();
        } else if (replies === 2)
          final();
      }, true);
    }
  });
};

FTP.prototype.get = function(path, zcomp, cb) {
  var self = this;
  if (typeof zcomp === 'function') {
    cb = zcomp;
    zcomp = false;
  }

  this._pasv(function(err, sock, id) {
    if (err)
      return cb(err);

    if (self._worker[id].getQueue()[0] && self._worker[id].getQueue()[0].cmd === 'ABOR') {
      sock.destroy();
      return cb();
    }

    // modify behavior of socket events so that we can emit 'error' once for
    // either a TCP-level error OR an FTP-level error response that we get when
    // the socket is closed (e.g. the server ran out of space).
    var sockerr, started = false, lastreply = false, done = false,
        source = sock;

    if (zcomp) {
      source = zlib.createInflate();
      sock.pipe(source);
      sock._emit = sock.emit;
      sock.emit = function(ev, arg1) {
        if (ev === 'error') {
          if (!sockerr)
            sockerr = arg1;
          return;
        }
        sock._emit.apply(sock, Array.prototype.slice.call(arguments));
      };
    }

    source._emit = source.emit;
    source.emit = function(ev, arg1) {
      if (ev === 'error') {
        if (!sockerr)
          sockerr = arg1;
        return;
      } else if (ev === 'end' || ev === 'close') {
        if (!done) {
          done = true;
          ondone();
        }
        return;
      }
      source._emit.apply(source, Array.prototype.slice.call(arguments));
    };

    function ondone() {
      if (done && lastreply) {
        self._worker[id].pasvProcessing = false;
        self._sendc(id, 'MODE S', function() {
          source._emit('end');
          source._emit('close');
        }, true);
      }
    }

    sock.pause();

    if (zcomp) {
      self._sendc(id, 'MODE Z', function(err, text, code) {
        if (err) {
          sock.destroy();
          return cb(makeError(code, 'Compression not supported'));
        }
        sendRetr();
      }, true);
    } else
      sendRetr();

    function sendRetr() {
      // this callback will be executed multiple times, the first is when server
      // replies with 150, then a final reply after the data connection closes
      // to indicate whether the transfer was actually a success or not
      self._sendc(id, 'RETR ' + path, function(err, text, code) {
        if (sockerr || err) {
          sock.destroy();
          if (!started) {
            if (zcomp) {
              self._sendc(id, 'MODE S', function() {
                cb(sockerr || err);
              }, true);
            } else
              cb(sockerr || err);
          } else {
            source._emit('error', sockerr || err);
            source._emit('close', true);
          }
          return;
        }
        self._worker[id].pasvProcessing = true;
        // server returns 125 when data connection is already open; we treat it
        // just like a 150
        if (code === 150 || code === 125) {
          started = true;
          cb(undefined, source);
        } else {
          lastreply = true;
          ondone();
        }
      }, true);
    }
  });
};

FTP.prototype.put = function(input, path, zcomp, cb) {
  this._store('STOR ' + path, input, zcomp, cb);
};

FTP.prototype.append = function(input, path, zcomp, cb) {
  this._store('APPE ' + path, input, zcomp, cb);
};

FTP.prototype.pwd = function(cb) { // PWD is optional
  var self = this;
  this._send('PWD', function(err, text, code) {
    if (code === 502) {
      return self.cwd('.', function(cwderr, cwd) {
        if (cwderr)
          return cb(cwderr);
        if (cwd === undefined)
          cb(err);
        else
          cb(undefined, cwd);
      }, true);
    } else if (err)
      return cb(err);
    cb(undefined, RE_WD.exec(text)[1]);
  });
};

FTP.prototype.cdup = function(cb) { // CDUP is optional
  var self = this;
  this._send('CDUP', function(err, text, code) {
    if (code === 502)
      self.cwd('..', cb, true);
    else
      cb(err);
  });
};

FTP.prototype.mkdir = function(path, recursive, cb) { // MKD is optional
  if (typeof recursive === 'function') {
    cb = recursive;
    recursive = false;
  }
  if (!recursive)
    this._send('MKD ' + path, cb);
  else {
    var self = this, owd, abs, dirs, dirslen, i = -1, searching = true;

    abs = (path[0] === '/');

    var nextDir = function() {
      if (++i === dirslen) {
        // return to original working directory
        return self._sendc(this.id, 'CWD ' + owd, cb, true);
      }
      if (searching) {
        self._sendc(this.id, 'CWD ' + dirs[i], function(err, text, code) {
          if (code === 550) {
            searching = false;
            --i;
          } else if (err) {
            // return to original working directory
            return self._sendc(this.id, 'CWD ' + owd, function() {
              cb(err);
            }, true);
          }
          nextDir.call({id: this.id});
        }, true);
      } else {
        self._sendc(this.id, 'MKD ' + dirs[i], function(err, text, code) {
          if (err) {
            // return to original working directory
            return self._sendc(this.id, 'CWD ' + owd, function() {
              cb(err);
            }, true);
          }
          self._sendc(this.id, 'CWD ' + dirs[i], nextDir, true);
        }, true);
      }
    };
    this.pwd(function(err, cwd) {
      if (err)
        return cb(err);
      owd = cwd;
      if (abs)
        path = path.substr(1);
      if (path[path.length - 1] === '/')
        path = path.substring(0, path.length - 1);
      dirs = path.split('/');
      dirslen = dirs.length;
      if (abs)
        self._send('CWD /', function(err) {
          if (err)
            return cb(err);
          nextDir.call({id: this.id});
        }, true);
      else
        nextDir.call({id: null});
    });
  }
};

FTP.prototype.rmdir = function(path, recursive, cb) { // RMD is optional
  if (typeof recursive === 'function') {
    cb = recursive;
    recursive = false;
  }
  if (!recursive) {
    return this._send('RMD ' + path, cb);
  }
  
  var self = this;
  this.list(path, function(err, list) {
    if (err) return cb(err);
    var idx = 0;
    
    // this function will be called once per listing entry
    var deleteNextEntry;
    deleteNextEntry = function(err) {
      if (err) return cb(err);
      if (idx >= list.length) {
        if (list[0] && list[0].name === path) {
          return cb(null);
        } else {
          return self.rmdir(path, cb);
        }
      }
      
      var entry = list[idx++];
      
      // get the path to the file
      var subpath = null;
      if (entry.name[0] === '/') {
        // this will be the case when you call deleteRecursively() and pass
        // the path to a plain file
        subpath = entry.name;
      } else {
        if (path[path.length - 1] == '/') {
          subpath = path + entry.name;
        } else {
          subpath = path + '/' + entry.name
        }
      }
      
      // delete the entry (recursively) according to its type
      if (entry.type === 'd') {
        if (entry.name === "." || entry.name === "..") {
          return deleteNextEntry();
        }
        self.rmdir(subpath, true, deleteNextEntry);
      } else {
        self.delete(subpath, deleteNextEntry);
      }
    }
    deleteNextEntry();
  });
};

FTP.prototype.system = function(cb) { // SYST is optional
  this._send('SYST', function(err, text) {
    if (err)
      return cb(err);
    cb(undefined, RE_SYST.exec(text)[1]);
  });
};

// "Extended" (RFC 3659) commands
FTP.prototype.size = function(path, cb) {
  var self = this;
  this._send('SIZE ' + path, function(err, text, code) {
    if (code === 502) {
      // Note: this may cause a problem as list() is _appended_ to the queue
      return self.list(path, function(err, list) {
        if (err)
          return cb(err);
        if (list.length === 1)
          cb(undefined, list[0].size);
        else {
          // path could have been a directory and we got a listing of its
          // contents, but here we echo the behavior of the real SIZE and
          // return 'File not found' for directories
          cb(new Error('File not found'));
        }
      }, true);
    } else if (err)
      return cb(err);
    cb(undefined, parseInt(text, 10));
  });
};

FTP.prototype.lastMod = function(path, cb) {
  var self = this;
  this._send('MDTM ' + path, function(err, text, code) {
    if (code === 502) {
      return self.list(path, function(err, list) {
        if (err)
          return cb(err);
        if (list.length === 1)
          cb(undefined, list[0].date);
        else
          cb(new Error('File not found'));
      }, true);
    } else if (err)
      return cb(err);
    var val = XRegExp.exec(text, REX_TIMEVAL), ret;
    if (!val)
      return cb(new Error('Invalid date/time format from server'));
    ret = new Date(val.year + '-' + val.month + '-' + val.date + 'T' + val.hour
                   + ':' + val.minute + ':' + val.second);
    cb(undefined, ret);
  });
};

FTP.prototype.restart = function(offset, cb) {
  this._sendc(true, 'REST ' + offset, cb);
};



// Private/Internal methods
FTP.prototype._pasv = function(cb) {
  var self = this, first = true, ip, port;
  this._send('PASV', function reentry(err, text) {
    if (err)
      return cb(err);

    var id = this.id;
		
		self._worker[id].currentRequest = null;
		self._worker[id].pasvProcessing = true;

    if (first) {
      var m = RE_PASV.exec(text);
      if (!m)
        return cb(new Error('Unable to parse PASV server response'));
      ip = m[1];
      ip += '.';
      ip += m[2];
      ip += '.';
      ip += m[3];
      ip += '.';
      ip += m[4];
      port = (parseInt(m[5], 10) * 256) + parseInt(m[6], 10);

      first = false;
    }
    self._pasvConnect(id, ip, port, function(err, sock) {
      if (err) {
        // try the IP of the control connection if the server was somehow
        // misconfigured and gave for example a LAN IP instead of WAN IP over
        // the Internet
        if (self._worker[id].socket && ip !== self._worker[id].socket.remoteAddress) {
          ip = self._worker[id].socket.remoteAddress;
          return reentry.call({id: id});
        }

        self._worker[id].pasvProcessing = false;
        
        // automatically abort PASV mode
        self._sendc(id, 'ABOR', function() {
          cb(err);
          self._sendc(id);
        }, true);

        return;
      }
      self._worker[id].pasvProcessing = false;
      cb(undefined, sock, id);
      self._sendc(id);
    });
  });
};

FTP.prototype._pasvConnect = function(id, ip, port, cb) {
  var self = this,
      socket = new Socket(),
      sockerr,
      timedOut = false,
      timer = setTimeout(function() {
        timedOut = true;
        socket.destroy();
        cb(new Error('Timed out while making data connection'));
      }, this.options.pasvTimeout);

  socket.setTimeout(0);

  socket.once('connect', function() {
    self._debug&&self._debug('[connection] PASV socket connected');
    if (self.options.secure === true) {
      self.options.secureOptions.socket = socket;
      self.options.secureOptions.session = self._worker[id].socket.getSession();
      //socket.removeAllListeners('error');
      socket = tls.connect(self.options.secureOptions);
      //socket.once('error', onerror);
      socket.setTimeout(0);
    }
    clearTimeout(timer);
    self._worker[id].pasvSock = socket;
    cb(undefined, socket);
  });
  socket.once('error', onerror);
  function onerror(err) {
    sockerr = err;
  }
  socket.once('end', function() {
    clearTimeout(timer);
  });
  socket.once('close', function(had_err) {
    clearTimeout(timer);
    if (self._worker[id] && !self._worker[id].pasvSock && !timedOut) {
      var errmsg = 'Unable to make data connection';
      if (sockerr) {
        errmsg += '( ' + sockerr + ')';
        sockerr = undefined;
      }
      cb(new Error(errmsg));
    }
    if (self._worker[id]) {
			self._worker[id].pasvSock = null;
		}
  });

  socket.connect(port, ip);
};

FTP.prototype._store = function(cmd, input, zcomp, cb) {
  var isBuffer = Buffer.isBuffer(input);

  if (!isBuffer && input.pause !== undefined)
    input.pause();

  if (typeof zcomp === 'function') {
    cb = zcomp;
    zcomp = false;
  }

  var self = this;
  this._pasv(function(err, sock, id) {
    if (err)
      return cb(err);

    if (self._worker[id].getQueue()[0] && self._worker[id].getQueue()[0].cmd === 'ABOR') {
      sock.destroy();
      return cb();
    }

    var sockerr, dest = sock;
    sock.once('error', function(err) {
      sockerr = err;
    });

    if (zcomp) {
      self._sendc(id, 'MODE Z', function(err, text, code) {
        if (err) {
          sock.destroy();
          return cb(makeError(code, 'Compression not supported'));
        }
        // draft-preston-ftpext-deflate-04 says min of 8 should be supported
        dest = zlib.createDeflate({ level: 8 });
        dest.pipe(sock);
        sendStore();
      }, true);
    } else
      sendStore();

    function sendStore() {
      // this callback will be executed multiple times, the first is when server
      // replies with 150, then a final reply after the data connection closes
      // to indicate whether the transfer was actually a success or not
      self._sendc(id, cmd, function(err, text, code) {
        if (sockerr || err) {
          if (zcomp) {
            self._sendc(id, 'MODE S', function() {
              cb(sockerr || err);
            }, true);
          } else
            cb(sockerr || err);
          return;
        }
        
        self._worker[id].pasvProcessing = true;

        if (code === 150 || code === 125) {
          if (isBuffer)
            dest.end(input);
          else if (typeof input === 'string') {
            // check if input is a file path or just string data to store
            fs.stat(input, function(err, stats) {
              if (err)
                dest.end(input);
              else
                fs.createReadStream(input).pipe(dest);
            });
          } else {
            input.pipe(dest);
            input.resume();
          }
        } else {
          if (zcomp)
            self._sendc(id, 'MODE S', cb, true);
          else
            cb();
            
          self._worker[id].pasvProcessing = false;
        }
      }, true);
    }
  });
};

FTP.prototype._send = function(cmd, cb, promote) {
	//push to queue with id = null
	var self = this;
	
	self._sendc(null, cmd, cb, promote);
};

FTP.prototype._sendc = function(id, cmd, cb, promote) {
	var self = this,
		i;
	
	if (id === true) {
		for (var i in this._worker) {
			this._sendc(i, cmd, cb, promote);
		}
		
		return false;
	}
	
	if (id && !this._worker[id]) {
		return false;
	}
	
	if (id) { 
		clearTimeout(this._worker[id].keepAlive);
	}
	
	if (cmd !== undefined) {
		if (promote) {
			this._queue.unshift({
				cmd: cmd,
				cb: cb,
				id: id,
			});
		} else {
			this._queue.push({
				cmd: cmd,
				cb: cb,
				id: id
			});
		}
	}
	
	var noReady = false;
	
	for (i in this._worker) {
		var worker = this._worker[i];
		if (!worker.ready) {
			noReady = true;
		}
		
		if (!worker.currentRequest && !worker.pasvProcessing && worker.socket && worker.socket.readable) {
			var queue = [];
			this._queue.every(function(value, key) {
				if (value.id == i || (!value.id && worker.ready)) {
					queue.push({data: value, key: key});
				}
				
				return true;
			});
			queue.sort(function(a, b) {
				return a.data.id && !b.data.id ? -1 : 0;
			});
			
			if (!queue.length) {
				continue;
			}
			
			worker.currentRequest = queue[0].data;
			this._queue.splice(queue[0].key, 1);
			
			if (typeof worker.currentRequest.cb == 'function') {
				worker.currentRequest.cb = worker.currentRequest.cb.bind({id: i});
			}
			
			if (worker.currentRequest.cmd === 'ABOR' && worker.pasvSock) {
				worker.pasvSock.aborting = true;
			}
			
			this._debug && this._debug('[connection] > ' + inspect(worker.currentRequest.cmd));
			worker.socket.write(worker.currentRequest.cmd + '\r\n');
			worker.lastActive = new Date().getTime();
		}
	}
	
	var noId = this._queue.filter(function(data) {
		if (!data.id) {
			return true;
		}
	});
	
	if (noId.length && this._sockets < this.options.maxSockets && !noReady) {
		this._createWorker(function(id) {
			self._sendc(id);
		});
	}
};

FTP.prototype._reset = function() {
  if (this._pasvSock && this._pasvSock.writable)
    this._pasvSock.end();
  if (this._socket && this._socket.writable)
    this._socket.end();
  this._socket = undefined;
  this._pasvSock = undefined;
  this._feat = undefined;
  this._curReq = undefined;
  this._secstate = undefined;
  clearTimeout(this._keepalive);
  this._keepalive = undefined;
  this._queue = [];
  this._ending = false;
  this._parser = undefined;
  this.options.host = this.options.port = this.options.user
                    = this.options.password = this.options.secure
                    = this.options.connTimeout = this.options.pasvTimeout
                    = this.options.keepalive = this._debug = undefined;
  this.connected = false;
};

FTP.prototype._reset = function(id) {
	if (id) {
		if (!this._worker[id]) {
			return false;
		}
		
		clearTimeout(this._worker[id].keepAlive);
		
		if (this._worker[id].pasvSock) {
			this._worker[id].pasvSock.destroy();
		}
		
		if (this._worker[id].socket) {
			this._worker[id].socket.destroy();
		}
		
		delete this._worker[id];
		this._sockets--;
		
		this._queue = this._queue.filter(function(value) {
			return value.id != id;
		});
	} else {
		for (var i in this._worker) {
			this._reset(i);
		}
		
		clearTimeout(this._multiSocketHandler);
		
		this._worker = {};

		this._ending = false;
		
		this.options.host = this.options.port = this.options.user = this.options.password = this.options.secure = this.options.connTimeout = this.options.pasvTimeout = this.options.keepalive = this._debug = undefined;
		this.connected = false;
	}
};

// Utility functions
function makeError(code, text) {
  var err = new Error(text);
  err.code = code;
  return err;
}
