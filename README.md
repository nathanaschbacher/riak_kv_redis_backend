## Overview

High-performance Redis storage backend for the Riak Distributed Database.


## Installation

**Pre-requisites:** You must already have Erlang R14B04 or later installed on your machine.

	$ git clone git@github.com:nathanaschbacher/riak_kv_redis_backend.git .
	$ cd riak_kv_redis_backend
	$ ./rebar get-deps compile
	
This should automatically build the `redis` dependencies and move the redis-server binary and redis.conf file the `priv/` directory.

Then you need to copy the `riak_kv_redis_backend` to the `lib` directory of your Riak install.

    $ cd ..
    $ cp -R ./riak_kv_redis_backend /path/to/riak/lib
    
Then create a symlink in the `lib` directory that points to the built `hierdis` dependency nested inside the `riak_kv_redis_backend` project.

    $ cd /path/to/riak/lib
    $ ln -s ./riak_kv_redis_backend/deps/hierdis ./hierdis

## Usage

Edit app.config to set your storage backend to use `riak_kv_redis_backend`

```
%% Riak KV config
{riak_kv, [
            {storage_backend, riak_kv_redis_backend},
            ...
          ]},
```

And add a stanza for configuring `hierdis`

```
%% hierdis Config
 {hierdis, [
          {data_root, "./data/hierdis"},
          {config_file, "/path/to/riak/lib/riak_kv_redis_backend/priv/riak_kv_redis_backend.redis.config"}
         ]},
```
    

## License

(The MIT License)

Copyright (c) 2013 Nathan Aschbacher

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
'Software'), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.