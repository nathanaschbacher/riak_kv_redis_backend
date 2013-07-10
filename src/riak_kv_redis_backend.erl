% (The MIT License)

% Copyright (c) 2013 Nathan Aschbacher

% Permission is hereby granted, free of charge, to any person obtaining
% a copy of this software and associated documentation files (the
% 'Software'), to deal in the Software without restriction, including
% without limitation the rights to use, copy, modify, merge, publish,
% distribute, sublicense, and/or sell copies of the Software, and to
% permit persons to whom the Software is furnished to do so, subject to
% the following conditions:

% The above copyright notice and this permission notice shall be
% included in all copies or substantial portions of the Software.

% THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY KIND,
% EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
% MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
% IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
% CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
% TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
% SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

-module(riak_kv_redis_backend).
-behavior(riak_kv_backend).

-author('Nathan Aschbacher <nathan@basho.com>').

%% Riak Storage Backend API
-export([api_version/0,
         capabilities/1,
         capabilities/2,
         start/2,
         stop/1,
         get/3,
         put/5,
         delete/4,
         drop/1,
         fold_buckets/4,
         fold_keys/4,
         fold_objects/4,
         is_empty/1,
         status/1,
         callback/3]).

-define(API_VERSION, 1).
-define(CAPABILITIES, []).

-record(state, {redis_context :: term(),
                redis_socket_path :: string(),
                redis_unix_pid :: integer(),
                data_dir :: string(),
                partition :: integer(),
                root :: string()}).

-type state() :: #state{}.
-type config() :: [{atom(), term()}].

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Return the major version of the
%% current API and a capabilities list.
%% The current valid capabilities are async_fold
%% and indexes.
-spec api_version() -> {integer(), [atom()]}.
api_version() ->
    {ok, ?API_VERSION}.

%% @doc Return the capabilities of the backend.
-spec capabilities(state()) -> {ok, [atom()]}.
capabilities(_) ->
    {ok, ?CAPABILITIES}.

%% @doc Return the capabilities of the backend.
-spec capabilities(riak_object:bucket(), state()) -> {ok, [atom()]}.
capabilities(_, _) ->
    {ok, ?CAPABILITIES}.

%% @doc Start the redis backend
-spec start(integer(), config()) -> {ok, state()} | {error, term()}.
start(Partition, Config) ->
    %% Get the data root directory
    DataRoot = app_helper:get_prop_or_env(data_root, Config, hierdis),
    ConfigFile = app_helper:get_prop_or_env(config_file, Config, hierdis),
    DataDir = filename:join([DataRoot, integer_to_list(Partition)]),

    case filelib:ensure_dir(filename:join([DataDir, dummy])) of
        ok ->
            case check_redis_install(DataDir, ConfigFile) of
                {ok, RedisExecutable, RedisConfig} ->
                    case start_redis(RedisExecutable, RedisConfig) of
                        {ok, RedisPidFile, RedisSocketFile} ->
                            case hierdis:connect_unix(RedisSocketFile) of
                                {ok, RedisContext} ->
                                    Result = {ok, #state{
                                        redis_context=RedisContext,
                                        redis_socket_path=RedisSocketFile,
                                        redis_unix_pid=RedisPidFile,
                                        data_dir=DataDir,
                                        partition=Partition,
                                        root=DataRoot
                                    }},
                                    Result;
                                {error, Reason} -> {error, Reason}
                            end;
                        {error, Reason} -> {error, Reason}
                    end;
                {error, Reason} -> {error, {Reason, "Unable to validate Redis install."}}
            end;
        {error, Reason} -> {error, {Reason, "Failed to create data directories for redis backend."}}
    end.


%% @doc Stop the backend
-spec stop(state()) -> ok.
stop(#state{redis_context=Context}=State) ->
    case hierdis:command(Context, [<<"SHUTDOWN">>]) of
        {error,{redis_err_eof,"Server closed the connection"}} ->
            ok;
        {error, Reason} ->
            {error, Reason, State}
    end.

%% @doc Retrieve an object from the backend
-spec get(riak_object:bucket(), riak_object:key(), state()) -> {ok, any(), state()} | {ok, not_found, state()} | {error, term(), state()}.
get(Bucket, Key, #state{redis_context=Context}=State) ->
    CombinedKey = [Bucket, <<",">>, Key],
    case hierdis:command(Context, [<<"GET">>, CombinedKey]) of
        {ok, undefined}  ->
            {error, not_found, State};
        {ok, Value} ->
            {ok, Value, State};
        {error, Reason} ->
            {error, Reason, State}
    end.

%% @doc Insert an object into the backend.
-type index_spec() :: {add, Index, SecondaryKey} | {remove, Index, SecondaryKey}.
-spec put(riak_object:bucket(), riak_object:key(), [index_spec()], binary(), state()) -> {ok, state()} | {error, term(), state()}.
put(Bucket, Key, _IndexSpec, Value, #state{redis_context=Context}=State) ->
    CombinedKey = [Bucket, <<",">>, Key],
    case hierdis:command(Context, [<<"SET">>, CombinedKey, Value]) of
        {ok, <<"OK">>} ->
            {ok, Value};
        {error, Reason} ->
            {error, Reason, State}
    end.

%% @doc Delete an object from the backend
-spec delete(riak_object:bucket(), riak_object:key(), [index_spec()], state()) -> {ok, state()} | {error, term(), state()}.
delete(Bucket, Key, _IndexSpec, #state{redis_context=Context}=State) ->
    CombinedKey = [Bucket, <<",">>, Key],
    case hierdis:command(Context, [<<"DEL">>, CombinedKey]) of
        {ok, 1} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end.

%% @doc Fold over all the buckets
-spec fold_buckets(riak_kv_backend:fold_buckets_fun(), any(), [], state()) -> {ok, any()} | {async, fun()}.
fold_buckets(_Func, _Any, _Thing, #state{redis_context=_Context}=State) ->
    {ok, State}.

%% @doc Fold over all the keys for one or all buckets.
-spec fold_keys(riak_kv_backend:fold_keys_fun(), any(), [{atom(), term()}], state()) -> {ok, term()} | {async, fun()}.
fold_keys(_Func, _Any, _Thing, #state{redis_context=_Context}=State) ->
    {ok, State}.

%% @doc Fold over all the objects for one or all buckets.
-spec fold_objects(riak_kv_backend:fold_objects_fun(), any(), [{atom(), term()}], state()) -> {ok, any()} | {async, fun()}.
fold_objects(_Func, _Any, _Thing, #state{redis_context=_Context}=State) ->
    {ok, State}.

%% @doc Delete all objects from this backend
-spec drop(state()) -> {ok, state()} | {error, term(), state()}.
drop(#state{redis_context=Context}=State) ->
    case hierdis:command(Context, [<<"FLUSHDB">>]) of
        {ok, <<"OK">>} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end.

%% @doc Returns true if this backend contains any keys otherwise returns false.
-spec is_empty(state()) -> boolean() | {error, term()}.
is_empty(#state{redis_context=Context}=State) ->
    case hierdis:command(Context, [<<"DBSIZE">>]) of
        {ok, 0} ->
            true;
        {ok, _} ->
            false;
        {error, Reason} ->
            {error, Reason, State}
    end.

%% @doc Get the status information for this backend
-spec status(state()) -> [{atom(), term()}].
status(#state{redis_context=Context}=State) ->
    case hierdis:command(Context, [<<"INFO">>, <<"all">>]) of
        {ok, Info} ->
            [redis_status, io:format("~p",[Info])];
        {error, Reason} ->
            {error, Reason, State}
    end.

%% @doc Register an asynchronous callback
-spec callback(reference(), any(), state()) -> {ok, state()}.
callback(_Ref, _Msg, State) ->
    {ok, State}.

%% @private
check_redis_install(DataDir, ConfigFilename) ->
    case copy_redis_executable(DataDir) of
        {ok, Executable} ->
            case copy_redis_config(DataDir, ConfigFilename) of
                {ok, ConfigFile} ->
                    {ok, Executable, ConfigFile};
                {error, Reason} ->
                    {error, {Reason, "Failed to copy Redis configuration file."}}
            end;
        {error, Reason} ->
            {error, {Reason, "Failed to copy Redis executable."}}
    end.

%% @private
copy_redis_executable(DataDir) ->
    ExpectedFile = filename:join([DataDir, "redis-server"]),
    case filelib:is_file(ExpectedFile) of
        true ->
            {ok, ExpectedFile};
        false ->
            case file:copy(filename:join([code:priv_dir(?MODULE), "redis", "redis-server"]), filename:join([DataDir,"redis-server"])) of
                {ok, _BytesCopied} ->
                    {ok, ExpectedFile};
                {error, Reason} ->
                    {error, Reason}
            end
    end.

%% @private
copy_redis_config(DataDir, ConfigFile) ->
    ExpectedFile = filename:join([DataDir, filename:basename(ConfigFile)]),
    case filelib:is_file(ExpectedFile) of
        true ->
            {ok, ExpectedFile};
        false ->
            case file:copy(ConfigFile, filename:join([DataDir,filename:basename(ConfigFile)])) of
                {ok, _BytesCopied} ->
                    {ok, ExpectedFile};
                {error, Reason} ->
                    {error, Reason}
            end
    end.

%% @private
start_redis(Executable, ConfigFile) ->
    case file:change_mode(Executable, 8#00755) of
        ok ->
            %% For some reason this isn't doing what I expect.
            Port = erlang:open_port({spawn_executable, Executable}, [{args, [ConfigFile]}, {cd, filename:dirname(Executable)}, exit_status]),
            case erlang:port_info(Port) of
                undefined ->
                    {error, {redis_error, "Could not start and/or link to Redis process as Erlang port."}};
                _Info ->
                    ExpectedSocketFile = filename:join([filename:dirname(Executable), "redis.sock"]),
                    case filelib:last_modified(ExpectedSocketFile) of
                        0 ->
                            {error, {redis_error, "Redis socket file not found."}};
                        _ ->
                            ExpectedPidFile = filename:join([filename:dirname(ExpectedSocketFile), "redis.pid"]),
                            case filelib:is_file(ExpectedPidFile) of
                                true ->
                                    {ok, ExpectedSocketFile, ExpectedPidFile};
                                false ->
                                    {error, {redis_error, "Redis pid file not found."}}
                            end
                    end
            end;
        {error, Reason} -> {error, Reason}
    end.
