%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc Mock Riak.
%%% @copyright 2012 Klarna AB
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%
%%%   Copyright 2011-2013 Klarna AB
%%%
%%%   Licensed under the Apache License, Version 2.0 (the "License");
%%%   you may not use this file except in compliance with the License.
%%%   You may obtain a copy of the License at
%%%
%%%       http://www.apache.org/licenses/LICENSE-2.0
%%%
%%%   Unless required by applicable law or agreed to in writing, software
%%%   distributed under the License is distributed on an "AS IS" BASIS,
%%%   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%%   See the License for the specific language governing permissions and
%%%   limitations under the License.
%%%

%%%_* Module declaration ===============================================
-module(krc_mock_client).
-behaviour(krc_riak_client).
-behaviour(gen_server).
-compile({no_auto_import, [get/1, put/2]}).

%%%_* Exports ==========================================================
%% riak_client callbacks
-export([ delete/5
        , get/5
        , get_index/5
        , put/4
        , start_link/3
        ]).

%% gen_server callbacks
-export([ code_change/3
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , init/1
        , terminate/2
        ]).

%% API
-export([ connect/0
        , disconnect/0
        , lag/1
        , start/0
        , stop/0
        ]).

%%%_* Includes =========================================================
-include_lib("tulib/include/prelude.hrl").

%%%_* Code =============================================================
%%%_ * API -------------------------------------------------------------
%% The server is started once (by the test harness).
%% All krc_server `connections' link to this single process.
start() -> gen_server:start({local, ?MODULE}, ?MODULE, [], []).

start_link(_IP, _Port, _Options) ->
  case whereis(?MODULE) of
    Pid when is_pid(Pid) ->
      {ok, Pid};
    undefined -> throw(missing_mock_client)
  end.

stop() -> ok = gen_server:call(?MODULE, stop).

%% Fault injection.
lag(Ms)      -> gen_server:call(?MODULE, {lag, Ms}).
connect()    -> gen_server:call(?MODULE, connect).
disconnect() -> gen_server:call(?MODULE, disconnect).

%%%_ * riak_client callbacks -------------------------------------------
delete(P, B, K, Os, T)   -> call(P, {delete,    [B, K, Os, T]}).
get(P, B, K, Os, T)      -> call(P, {get,       [B, K, Os, T]}).
get_index(P, B, I, K, T) -> call(P, {get_index, [B, I, K, T] }).
put(P, O, Os, T)         -> call(P, {put,       [O, Os, T]   }).

%% Simulate Riak timeout via gen_server timeout.
call(Pid, {_F, A} = Req) ->
  case ?lift(gen_server:call(Pid, Req, lists:last(A))) of
    {ok, ok}                               -> ok;
    {ok, _} = Ok                           -> Ok;
    {error, {lifted_exn, {timeout, _}, _}} -> {error, timeout};
    {error, _} = Err                       -> Err
  end.


%%%_ * gen_server callbacks --------------------------------------------
-record(s,
        { tabs=tulib_maps:new()                :: _
        , idxs=tulib_maps:new()                :: _
        , revdxs=tulib_maps:new()              :: _
        , con=true                             :: boolean()
        , lag=0                                :: timeout()
        }).

init([])                                       -> {ok, #s{}}.
terminate(_, _)                                -> ok.
code_change(_, S, _)                           -> {ok, S}.

handle_call(connect,    _, #s{con=false} = S)  -> {reply, ok, S#s{con=true}};
handle_call(disconnect, _, #s{con=true}  = S)  -> {reply, ok, S#s{con=false}};
handle_call({lag, Ms},  _, #s{}          = S)  -> {reply, ok, S#s{lag=Ms}};
handle_call({F, A},     _, #s{}          = S0) -> {S, Ret} = do_call(F, A, S0),
                                                  {reply, Ret, S};
handle_call(stop,       _, S)                  -> {stop, normal, ok, S}.

handle_cast(_Msg, S)                           -> {stop, bad_cast, S}.
handle_info(_Msg, S)                           -> {stop, bad_info, S}.

%%%_ * Internals -------------------------------------------------------
do_call(_, _, #s{con=false} = S) ->
  {S, {error, disconnected}};
do_call(F, A, #s{lag=Lag} = S) ->
  timer:sleep(Lag),
  do(F, A, S).

do(get, [B, K, _, _], #s{tabs=Tabs} = S) ->
  {S, tulib_maps:get(Tabs, [B, K])};
do(get_index, [B, I, K, _], #s{idxs=Idxs} = S) ->
  {S, case tulib_maps:get(Idxs, [B, I, K]) of
        {ok, _} = Ok      -> Ok;
        {error, notfound} -> {ok, []}
      end};
do(put, [O, _, _], #s{tabs=Tabs0, idxs=Idxs0, revdxs=Revdxs0} = S) ->
  Bucket  = krc_obj:bucket(O),
  Key     = krc_obj:key(O),
  Indices = krc_obj:indices(O),
  Tabs    = tulib_maps:set(Tabs0, [Bucket, Key], O),
  Idxs    = lists:foldl(
              fun({Idx, IdxKey}, Idxs) ->
                add(Idxs, [Bucket, Idx, IdxKey], Key)
              end, Idxs0, Indices),
  Revdxs  = lists:foldl(
              fun(Index, Revdxs) ->
                add(Revdxs, [Bucket, Key], Index)
              end, Revdxs0, Indices),
  {S#s{tabs=Tabs, idxs=Idxs, revdxs=Revdxs}, ok};
do(delete, [B, K, _, _], #s{tabs=Tabs0, idxs=Idxs0, revdxs=Revdxs0} = S) ->
  Indices = tulib_maps:get(Revdxs0, [B, K], []),
  Tabs    = tulib_maps:delete(Tabs0, [B, K]),
  Idxs    = lists:foldl(
              fun({Idx, IdxKey}, Idxs) ->
                del(Idxs, [B, Idx, IdxKey], K)
              end, Idxs0, Indices),
  Revdxs  = tulib_maps:delete(Revdxs0, [B, K]),
  {S#s{tabs=Tabs, idxs=Idxs, revdxs=Revdxs}, ok}.

add(Map, Ks, V) -> tulib_maps:update(Map, Ks, tulib_lists:cons(V), [V]).
del(Map, Ks, V) -> tulib_maps:update(Map, Ks, fun(Vs) -> Vs -- [V] end, []).

%%%_* Tests ============================================================
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

get_put_delete_test() ->
  {ok, Pid}         = start(),

  Obj               = krc_obj:new(mah_bucket, mah_key, mah_val),
  {error, notfound} = get(Pid, mah_bucket, mah_key, [], 1000),
  ok                = put(Pid, Obj, [], 1000),
  {ok, Obj}         = get(Pid, mah_bucket, mah_key, [], 1000),
  ok                = delete(Pid, mah_bucket, mah_key, [], 1000),
  {error, notfound} = get(Pid, mah_bucket, mah_key, [], 1000),

  stop().

index_test() ->
  {ok, Pid}        = start(),

  ObjA0            = krc_obj:new(mah_bucket1, mah_key1, mah_val1),
  ObjB0            = krc_obj:new(mah_bucket2, mah_key2, mah_val2),
  Indices          = [ {mah_idx1, mah_idxkey1}
                     , {mah_idx2, mah_idxkey2}
                     ],
  ObjA             = krc_obj:set_indices(ObjA0, Indices),
  ObjB             = krc_obj:set_indices(ObjB0, Indices),
  ok               = put(Pid, ObjA, [], 1000),
  ok               = put(Pid, ObjB, [], 1000),

  {ok, []}         = get_index(Pid, yo_bucket, yo_idx, yo_idxkey, 1000),

  {ok, [mah_key1]} = get_index(Pid, mah_bucket1, mah_idx1, mah_idxkey1, 1000),
  {ok, [mah_key1]} = get_index(Pid, mah_bucket1, mah_idx2, mah_idxkey2, 1000),
  {ok, [mah_key2]} = get_index(Pid, mah_bucket2, mah_idx1, mah_idxkey1, 1000),
  {ok, [mah_key2]} = get_index(Pid, mah_bucket2, mah_idx2, mah_idxkey2, 1000),

  ok               = delete(Pid, mah_bucket1, mah_key1, [], 1000),
  ok               = delete(Pid, mah_bucket2, mah_key2, [], 1000),

  {ok, []}         = get_index(Pid, mah_bucket1, mah_idx1, mah_idxkey1, 1000),
  {ok, []}         = get_index(Pid, mah_bucket1, mah_idx2, mah_idxkey2, 1000),
  {ok, []}         = get_index(Pid, mah_bucket2, mah_idx1, mah_idxkey1, 1000),
  {ok, []}         = get_index(Pid, mah_bucket2, mah_idx2, mah_idxkey2, 1000),

  stop().

coverage_test() ->
  {error, _} = ?lift(start_link(ip, port, opts)),
  {ok, Pid1} = start(),
  Pid1 ! info,
  timer:sleep(100),
  {ok, Pid2} = start(),
  gen_server:cast(Pid2, msg),
  {ok, bar} = code_change(foo, bar, baz).

-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
