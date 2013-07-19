#!/bin/sh

set -e

ROOT="$PWD"

REDIS_VSN="2.6.14"

# detecting gmake and if exists use it
# if not use make
# (code from github.com/tuncer/re2/c_src/build_deps.sh
which gmake 1>/dev/null 2>/dev/null && MAKE=gmake
MAKE=${MAKE:-make}

# Changed "make" to $MAKE

case "$1" in
    clean)
	# rm ./c_src/redis
	rm -rf $ROOT/c_src/redis
        ;;

    get-deps)
	#clone redis to ./c_src/redis from git@github.com:antirez/redis.git
	cd c_src

        [ -d redis ] || git clone https://github.com/antirez/redis.git
        cd redis
        git checkout $REDIS_VSN

        cd $ROOT
        ;;

    *)
        #build redis and install redis-server and redis.conf to ./priv/redis
		cd $ROOT/c_src/redis && make

		rm -rf $ROOT/priv/redis
		mkdir -p $ROOT/priv/redis
		cp "$ROOT/c_src/redis/src/redis-server" $ROOT/priv/redis/redis-server
		#cp $ROOT/c_src/redis/redis.conf $ROOT/priv/redis/redis.conf

		cd $ROOT
        ;;
esac
