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

-export([data_size/1]).

-define(API_VERSION, 1).
-define(CAPABILITIES, [async_fold, size]).

-record(state, {redis_context :: term(),
                redis_socket_path :: string(),
                storage_scheme :: atom(),
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
start(Partition, _Config) ->
    %% Start the hierdis application.
    case start_hierdis_application() of
        ok ->
            %% Get the data root directory
            DataRoot = filename:absname(app_helper:get_env(hierdis, data_root)),
            ConfigFile = filename:absname(app_helper:get_env(hierdis, config_file)),
            DataDir = filename:join([DataRoot, integer_to_list(Partition)]),

            ExpectedExecutable = filename:absname(app_helper:get_env(hierdis, executable)),
            ExpectedSocketFile = lists:flatten([app_helper:get_env(hierdis, unixsocket), integer_to_list(Partition)]),

            case app_helper:get_env(hierdis, storage_scheme) of
                hash -> Scheme = hash;
                _ -> Scheme = kv
            end,

            case filelib:ensure_dir(filename:join([DataDir, dummy])) of
                ok ->
                    case check_redis_install(ExpectedExecutable) of
                        {ok, RedisExecutable} ->
                            case start_redis(RedisExecutable, ConfigFile, ExpectedSocketFile, DataDir) of
                                {ok, RedisSocket} ->
                                    case hierdis:connect_unix(RedisSocket) of
                                        {ok, RedisContext} ->
                                            Result = {ok, #state{
                                                redis_context=RedisContext,
                                                redis_socket_path=RedisSocket,
                                                storage_scheme=Scheme,
                                                data_dir=DataDir,
                                                partition=Partition,
                                                root=DataRoot
                                            }},
                                            io:format("Started redis backend for partition: ~p\n", [Partition]),
                                            Result;                                    
                                        {error, Reason} -> {error, Reason}
                                    end;
                                {error, Reason} -> {error, Reason}
                            end;
                        {error, Reason} -> {error, Reason}
                    end;
                {error, Reason} -> {error, {Reason, "Failed to create data directories for redis backend."}}
            end;
        {error, Reason} -> {error, Reason}
    end.

%% @doc Stop the backend
-spec stop(state()) -> ok.
stop(#state{redis_context=Context}=State) ->
    case app_helper:get_env(hierdis, leave_running) of
        true ->
            ok;
        _ ->
            case hierdis:command(Context, [<<"SHUTDOWN">>]) of
                {error,{redis_err_eof,"Server closed the connection"}} ->
                    ok;
                {error, Reason} ->
                    {error, Reason, State}
            end
    end.

%% @doc Retrieve an object from the backend
-spec get(riak_object:bucket(), riak_object:key(), state()) -> {ok, any(), state()} | {ok, not_found, state()} | {error, term(), state()}.
get(Bucket, Key, #state{storage_scheme=Scheme}=State) ->
    get(Scheme, Bucket, Key, State).

%% @private
get(hash, Bucket, Key, #state{redis_context=Context}=State) ->
    case hierdis:command(Context, [<<"HGET">>, Bucket, Key]) of
        {ok, undefined}  ->
            {error, not_found, State};
        {ok, Value} ->
            {ok, Value, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
get(kv, Bucket, Key, #state{redis_context=Context}=State) ->
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
put(Bucket, Key, _IndexSpec, Value, #state{storage_scheme=Scheme}=State) ->
    put(Scheme, Bucket, Key, _IndexSpec, Value, State).

%% @private
put(hash, Bucket, Key, _IndexSpec, Value, #state{redis_context=Context}=State) ->
    case hierdis:command(Context, [<<"HSET">>, Bucket, Key, Value]) of
        {ok, _Response} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
put(kv, Bucket, Key, _IndexSpec, Value, #state{redis_context=Context}=State) ->
    CombinedKey = [Bucket, <<",">>, Key],
    case hierdis:command(Context, [<<"SET">>, CombinedKey, Value]) of
        {ok, _Response} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end.

%% @doc Delete an object from the backend
-spec delete(riak_object:bucket(), riak_object:key(), [index_spec()], state()) -> {ok, state()} | {error, term(), state()}.
delete(Bucket, Key, _IndexSpec, #state{storage_scheme=Scheme}=State) ->
    delete(Scheme, Bucket, Key, _IndexSpec, State).

%% @private
delete(hash, Bucket, Key, _IndexSpec, #state{redis_context=Context}=State) ->
    case hierdis:command(Context, [<<"HDEL">>, Bucket, Key]) of
        {ok, _Response} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
delete(kv, Bucket, Key, _IndexSpec, #state{redis_context=Context}=State) ->
    CombinedKey = [Bucket, <<",">>, Key],
    case hierdis:command(Context, [<<"DEL">>, CombinedKey]) of
        {ok, _Response} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end.

%% @doc Fold over all the buckets
-spec fold_buckets(riak_kv_backend:fold_buckets_fun(), any(), [], state()) -> {ok, any()} | {async, fun()}.
fold_buckets(FoldBucketsFun, Acc, Opts, #state{storage_scheme=Scheme,redis_context=Context}=State) ->
    FoldFun = fold_buckets_fun(Scheme, FoldBucketsFun),
    BucketFolder =
        fun() ->
            case hierdis:command(Context, [<<"KEYS">>, <<"*">>]) of
                {ok, Response} ->
                    {BucketList, _} = lists:foldl(FoldFun, {Acc, sets:new()}, Response),
                    BucketList;
                {error, Reason} -> 
                    {error, Reason, State}
            end
        end,
    case lists:member(async_fold, Opts) of
        true ->
            {async, BucketFolder};
        false ->
            {ok, BucketFolder()}
    end.

%% @private
fold_buckets_fun(hash, FoldBucketsFun) ->
    fun(B, {Acc, _}) ->
        {FoldBucketsFun(B, Acc), undefined}
    end;
fold_buckets_fun(kv, FoldBucketsFun) ->
    fun(CombinedKey, {Acc, BucketSet}) ->
        [B, _Key] = binary:split(CombinedKey, <<",">>),
        case sets:is_element(B, BucketSet) of
            true ->
                {Acc, BucketSet};
            false ->
                {FoldBucketsFun(B, Acc),
                 sets:add_element(B, BucketSet)}
        end
    end.

%% @doc Fold over all the keys for one or all buckets.
-spec fold_keys(riak_kv_backend:fold_keys_fun(), any(), [{atom(), term()}], state()) -> {ok, term()} | {async, fun()}.
fold_keys(FoldKeysFun, Acc, Opts, #state{storage_scheme=Scheme}=State) ->
    fold_keys(Scheme, FoldKeysFun, Acc, Opts, State).

%% @private
fold_keys(hash, FoldKeysFun, Acc, Opts, #state{redis_context=Context}=State) ->
    Bucket =  proplists:get_value(bucket, Opts),
    FoldFun = fold_keys_fun(hash, FoldKeysFun, Bucket),
    KeyFolder =
        fun() ->
            case hierdis:command(Context, [<<"HKEYS">>, Bucket]) of
                {ok, Response} ->
                    KeyList = lists:foldl(FoldFun, Acc, Response),
                    KeyList;
                {error, Reason} -> 
                    {error, Reason, State}
            end
        end,
    case lists:member(async_fold, Opts) of
        true ->
            {async, KeyFolder};
        false ->
            {ok, KeyFolder()}
    end;
fold_keys(kv, FoldKeysFun, Acc, Opts, #state{redis_context=Context}=State) ->
    Bucket =  proplists:get_value(bucket, Opts),
    FoldFun = fold_keys_fun(kv, FoldKeysFun, Bucket),
    KeyFolder =
        fun() ->
            case hierdis:command(Context, [<<"KEYS">>, [Bucket,",*"]]) of
                {ok, Response} ->
                    KeyList = lists:foldl(FoldFun, Acc, Response),
                    KeyList;
                {error, Reason} -> 
                    {error, Reason, State}
            end
        end,
    case lists:member(async_fold, Opts) of
        true ->
            {async, KeyFolder};
        false ->
            {ok, KeyFolder()}
    end.

%% @private
fold_keys_fun(hash, FoldKeysFun, Bucket) ->
    fun(Key, Acc) ->
        FoldKeysFun(Bucket, Key, Acc)
    end;
fold_keys_fun(kv, FoldKeysFun, _Bucket) ->
    fun(CombinedKey, Acc) ->
        [B, Key] = binary:split(CombinedKey, <<",">>),
        FoldKeysFun(B, Key, Acc)
    end.

%% @doc Fold over all the objects for one or all buckets.
-spec fold_objects(riak_kv_backend:fold_objects_fun(), any(), [{atom(), term()}], state()) -> {ok, any()} | {async, fun()}.
fold_objects(FoldObjectsFun, Acc, Opts, #state{storage_scheme=Scheme}=State) ->
    fold_objects(Scheme, FoldObjectsFun, Acc, Opts, State).
 
%% @private
fold_objects(hash, FoldObjectsFun, Acc, Opts, #state{redis_context=Context}=State) ->   
    Bucket =  proplists:get_value(bucket, Opts),
    FoldFun = fold_objects_fun(hash, FoldObjectsFun, Bucket, Context),
    ObjectFolder =
        fun() ->
            case hierdis:command(Context, [<<"HKEYS">>, Bucket]) of
                {ok, Response} ->
                    ObjectList = lists:foldl(FoldFun, Acc, Response),
                    ObjectList;
                {error, Reason} -> 
                    {error, Reason, State}
            end
        end,
    case lists:member(async_fold, Opts) of
        true ->
            {async, ObjectFolder};
        false ->
            {ok, ObjectFolder()}
    end;
fold_objects(kv, FoldObjectsFun, Acc, Opts, #state{redis_context=Context}=State) ->   
    Bucket =  proplists:get_value(bucket, Opts),
    FoldFun = fold_objects_fun(kv, FoldObjectsFun, Bucket, Context),
    ObjectFolder =
        fun() ->
            case hierdis:command(Context, [<<"KEYS">>, [Bucket,",*"]]) of
                {ok, Response} ->
                    ObjectList = lists:foldl(FoldFun, Acc, Response),
                    ObjectList;
                {error, Reason} -> 
                    {error, Reason, State}
            end
        end,
    case lists:member(async_fold, Opts) of
        true ->
            {async, ObjectFolder};
        false ->
            {ok, ObjectFolder()}
    end.

%% @private
fold_objects_fun(hash, FoldObjectsFun, Bucket, RedisContext) ->
    fun(Key, Acc) ->
        case hierdis:command(RedisContext, [<<"HGET">>, Bucket, Key]) of
            {ok, undefined}  ->
                Acc;
            {ok, Value} ->
                FoldObjectsFun(Bucket, Key, Value, Acc);
            {error, _Reason} ->
                Acc
        end
    end;
fold_objects_fun(kv, FoldObjectsFun, _Bucket, RedisContext) ->
    fun(CombinedKey, Acc) ->
        case hierdis:command(RedisContext, [<<"GET">>, CombinedKey]) of
            {ok, undefined}  ->
                Acc;
            {ok, Value} ->
                [B, Key] = binary:split(CombinedKey, <<",">>),
                FoldObjectsFun(B, Key, Value, Acc);
            {error, _Reason} ->
                Acc
        end
    end.

%% @doc Delete all objects from this backend
-spec drop(state()) -> {ok, state()} | {error, term(), state()}.
drop(#state{redis_context=Context}=State) ->
    case hierdis:command(Context, [<<"FLUSHDB">>]) of
        {ok, _Response} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end.

%% @doc Get the size of the redis database in bytes
-spec data_size(state()) -> undefined | {non_neg_integer(), bytes}.
data_size(#state{redis_context=Context}=_State) ->
    case hierdis:command(Context, [<<"INFO">>, <<"memory">>]) of
        {ok, _Response} ->
            [_JunkHeader | [UsedMemoryElm | _Rest]] = binary:split(_Response, <<"\r\n">>, [global, trim]),
            [_Name | Value] = binary:split(UsedMemoryElm, <<":">>, [global, trim]),
            list_to_integer(binary_to_list(list_to_binary(Value)));
        _ ->
            undefined
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
status(State) ->
    [{state, State}].

%% @doc Register an asynchronous callback
-spec callback(reference(), any(), state()) -> {ok, state()}.
callback(_Ref, _Msg, State) ->
    {ok, State}.

%% @private
start_hierdis_application() ->
    case application:start(hierdis) of
        ok ->
            ok;
        {error, {already_started, hierdis}} ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

%% @private
file_exists(Filepath) ->
    case filelib:last_modified(filename:absname(Filepath)) of
        0 ->
            false;
        _ ->
            true
    end.

%% @private
check_redis_install(Executable) ->
    case file_exists(Executable) of
        true ->
            {ok, Executable};
        false ->            
            {error, {io:format("Failed to locate Redis executable: ~p", [Executable])}}
    end.

%% @private
start_redis(Executable, ConfigFile, SocketFile, DataDir) ->
    case file:change_mode(Executable, 8#00755) of
        ok ->
            case file_exists(SocketFile) of
                true ->
                    {ok, SocketFile};
                false ->
                    Args = [ConfigFile, "--unixsocket", SocketFile, "--daemonize", "yes"],
                    Port = erlang:open_port({spawn_executable, [Executable]}, [{args, Args}, {cd, filename:absname(DataDir)}]),
                    receive
                        {'EXIT', Port, normal} ->
                            wait_for_file(SocketFile, 100, 5);
                        {'EXIT', Port, Reason} ->
                            {error, {redis_error, Port, Reason, io:format("Could not start Redis via Erlang port: ~p\n", [Executable])}}
                    end
            end;
        {error, Reason} -> 
            {error, Reason}
    end.

%% @private
wait_for_file(File, Msec, Attempts) when Attempts > 0 ->
    case file_exists(File) of
        true->
            {ok, File};
        false ->
            timer:sleep(Msec),
            wait_for_file(File, Msec, Attempts-1)
    end;
wait_for_file(File, _Msec, Attempts) when Attempts =< 0 ->
    {error, {redis_error, "Redis isn't running, couldn't find: ~p\n", [File]}}.
