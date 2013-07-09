## Overview

High-performance Redis storage backend for the Riak Distributed Database. 


## Installation

**Pre-requisites:** You must already have Erlang R14B04 or later installed on your machine.

	$ git clone git@github.com:nathanaschbacher/riak_kv_redis_backend.git .
	$ cd riak_kv_redis_backend
	$ ./rebar get-deps compile
	
This should automatically build the `redis` dependencies and move the redis-server binary and redis.conf file the `priv/` directory.

## Usage

    $ erl -pa ebin/
    
    Eshell V5.9.1  (abort with ^G)
    1> 
    

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