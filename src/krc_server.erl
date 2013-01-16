%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc K Riak Client.
%%% The K Riak Client adds a higher-level API and a connection pool to the
%%% official Riak protobuf client. KRC does not pretend to be a generic client
%%% library but rather the simplest thing which works for us - our setup is
%%% described below.
%%%
%%% We have a cluster of N machines. Each machine hosts two BEAM emulators, one
%%% runs our application server and the other runs a Riak server. The Riak
%%% servers form a Riak cluster.
%%% Load-balancers distribute incoming requests amongst those application
%%% servers which are currently up.
%%%
%%% Each application server runs one instance of the gen_server defined in this
%%% file (globally registered name). The KRC gen_server maintains a number of
%%% TCP/IP connections to the Riak node co-located on its machine (localhost).
%%%
%%% The message flow is depicted below.
%%%
%%%
%%% application    ------------------------------------
%%%                \        |
%%% krc_server     ---------+--------------------------
%%%                  \      |
%%% connection     ------------------------------------
%%%                    \   /
%%% riak_pb_socket ------------------------------------
%%%                      \/
%%% riak server    ------------------------------------
%%%
%%%
%%% The application makes a request to the krc_server, which the krc_server
%%% forwards to one of its connection processes.
%%% Requests are buffered in the connection processes' message queues.
%%% Each connection talks to a riak_pb_socket process, which talks to the Riak
%%% server over TCP/IP.
%%%
%%% The failure modes are handled as follows:
%%%   - If an application process crashes, we drop any queued requests so as
%%%     not to send buffered write requests to the Riak server.
%%%   - If krc_server cannot reach its local Riak node, it crashes and the
%%%     application server goes down (this is mainly to avoid having to
%%%     maintain knowledge of the state of the Riak cluster locally, and may be
%%%     changed in a future release).
%%%   - The connection and riak_pb_socket processes are linked, so if either
%%%     dies, the other will be killed as well and all requests in the
%%%     connection's message queue will time out.
%%%
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
-module(krc_server).
-behaviour(gen_server).

%%%_* Exports ==========================================================
%% krc_server API
-export([ start/1
        , start/2
        , start_link/1
        , start_link/2
        , stop/1
        ]).

%% Riak API
-export([ delete/3
        , get/3
        , get_index/4
        , put/2
        ]).

%% gen_server callbacks
-export([ code_change/3
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , init/1
        , terminate/2
        ]).

%% Internal exports
-export([ connection/2
        ]).

%%%_* Includes =========================================================
-include("krc.hrl").
-include_lib("tulib/include/assert.hrl").
-include_lib("tulib/include/logging.hrl").
-include_lib("tulib/include/metrics.hrl").
-include_lib("tulib/include/prelude.hrl").
-include_lib("tulib/include/types.hrl").

%%%_* Macros ===========================================================
%% Make sure we time out internally before our clients time out.
-define(CALL_TIMEOUT,  5000). %gen_server:call/3
-define(TIMEOUT,       (?CALL_TIMEOUT - 2000)).

-define(FAILURES,      100). %max number of worker failures to tolerate

%%%_* Code =============================================================
%%%_ * Types -----------------------------------------------------------
-record(s,
        { client     :: atom()             %krc_riak_client
        , ip         :: inet:ip_address()  %\ Riak
        , port       :: inet:port_number() %/ server
        , pids       :: [pid()]            %Connections
        , failures=0 :: non_neg_integer()  %Connection crash counter
        }).

%%%_ * API -------------------------------------------------------------
delete(GS, B, K)       -> call(GS, {delete,    [B, K]   }).
get(GS, B, K)          -> call(GS, {get,       [B, K]   }).
get_index(GS, B, I, K) -> call(GS, {get_index, [B, I, K]}).
put(GS, O)             -> call(GS, {put,       [O]      }).

start(A)               -> gen_server:start(?MODULE, A, []).
start(Name, A)         -> gen_server:start({local, Name}, ?MODULE, A, []).
start_link(A)          -> gen_server:start_link(?MODULE, A, []).
start_link(Name, A)    -> gen_server:start_link({local, Name}, ?MODULE, A, []).
stop(GS)               -> gen_server:call(GS, stop).

call(GS, Req) ->
  gen_server:call(GS, {tulib_util:timestamp(), Req}, ?CALL_TIMEOUT).

%%%_ * gen_server callbacks --------------------------------------------
init(Args) ->
  process_flag(trap_exit, true),
  Client   = tulib_util:get_arg(client,    Args, krc_pb_client, ?APP),
  IP       = tulib_util:get_arg(riak_ip,   Args, "127.0.0.1",   ?APP),
  Port     = tulib_util:get_arg(riak_port, Args, 8081,          ?APP),
  PoolSize = tulib_util:get_arg(pool_size, Args, 5,             ?APP),
  Pids     = [connection_start(Client, IP, Port) ||
               _ <- tulib_lists:seq(PoolSize)],
  {ok, #s{client=Client, ip=IP, port=Port, pids=Pids}}.

terminate(_, #s{}) -> ok.

code_change(_, S, _) -> {ok, S}.

handle_call(stop, _From, S) ->
  {stop, stopped, ok, S};
handle_call(Req, From, #s{pids=[Pid|Pids]} = S) ->
  Pid ! {handle, Req, From},
  {noreply, S#s{pids=Pids ++ [Pid]}}. %round robin

handle_cast(_Msg, S) -> {stop, bad_cast, S}.

handle_info({'EXIT', Pid, Rsn}, #s{failures=N} = S) when N > ?FAILURES ->
  %% We assume that the system is restarted occasionally anyway (for upgrades
  %% and such), so we don't bother resetting the counter.
  ?critical("EXIT ~p: ~p: too many failures", [Pid, Rsn]),
  ?increment([exits, failures]),
  {stop, failures, S};
handle_info({'EXIT', Pid, disconnected}, #s{pids=Pids} = S)  ->
  %% Die if we can't talk to localhost.
  ?hence(lists:member(Pid, Pids)),
  ?critical("EXIT ~p: disconnected", [Pid]),
  ?increment([exits, disconnected]),
  {stop, disconnected, S};
handle_info({'EXIT', Pid, Rsn},
            #s{client=Client, ip=IP, port=Port, pids=Pids0, failures=N} = S) ->
  ?hence(lists:member(Pid, Pids0)),
  ?error("EXIT ~p: ~p", [Pid, Rsn]),
  ?increment([exits, other]),
  Pids = Pids0 -- [Pid],
  {noreply, S#s{ pids     = [connection_start(Client, IP, Port)|Pids]
               , failures = N+1
               }};
handle_info(Msg, S) ->
  ?warning("~p", [Msg]),
  {noreply, S}.

%%%_ * Internals -------------------------------------------------------
%%%_  * Connections ----------------------------------------------------
connection_start(Client, IP, Port) ->
  proc_lib:spawn_link(?thunk(
    {ok, Pid} = Client:start_link(IP, Port, copts()),
    connection(Client, Pid))).

connection(Client, Pid) ->
  receive
    {handle, {TS, Req}, {Caller, _} = From} ->
      case {tulib_processes:is_up(Caller), time_left(TS)} of
        {true, true} ->
          case ?lift(do(Client, Pid, Req)) of
            {error, disconnected} = Err ->
              gen_server:reply(From, Err),
              exit(disconnected);
            {error, timeout} = Err ->
              ?error("timeout", []),
              ?increment([requests, timeouts]),
              gen_server:reply(From, Err),
              ?MODULE:connection(Client, Pid);
            {error, notfound} = Err ->
              ?debug("notfound", []),
              ?increment([requests, notfound]),
              gen_server:reply(From, Err),
              ?MODULE:connection(Client, Pid);
            {error, Rsn} = Err ->
              ?error("error: ~p", [Rsn]),
              ?increment([requests, errors]),
              gen_server:reply(From, Err),
              ?MODULE:connection(Client, Pid);
            {ok, Res} = Ok ->
              ?increment([requests, ok]),
              gen_server:reply(From, if Res =:= ok -> ok; true -> Ok end),
              ?MODULE:connection(Client, Pid)
          end;
        {false, _} ->
          ?info("dropping request ~p from ~p: DOWN", [Req, Caller]),
          ?increment([requests, dropped]),
          ?MODULE:connection(Client, Pid);
        {_, false} ->
          ?info("dropping request ~p from ~p: out of time", [Req, Caller]),
          ?increment([requests, out_of_time]),
          gen_server:reply(From, {error, timeout}),
          ?MODULE:connection(Client, Pid)
      end
  end.

time_left(T0) ->
  T1        = tulib_util:timestamp(),
  ElapsedMs = (T1 - T0) / 1000,
  (ElapsedMs + ?TIMEOUT) < ?CALL_TIMEOUT.


-spec do(atom(), pid(), {atom(), [_]}) -> maybe(_, _).
do(Client, Pid, {F, A}) ->
  Args = [Pid] ++ A ++ opts(F) ++ [?TIMEOUT],
  ?debug("apply(~p, ~p, ~p)", [Client, F, Args]),
  apply(Client, F, Args).

opts(delete)    -> [dopts()];
opts(get)       -> [ropts()];
opts(get_index) -> [];
opts(put)       -> [wopts()].

%%%_  * Config ---------------------------------------------------------
%% Our app.config sets:
%%   n_val           : 3
%%   allow_mult      : true
%%   last_write_wins : false

%% Connections
copts() ->
  [ {auto_reconnect,  false}         %exit on TCP/IP error
  ].

%% Reads
ropts() ->
  [ {r,               quorum}        %\ Majority
  , {pr,              1}             %/ reads
  , {basic_quorum,    false}
  , {notfound_ok,     true}
  ].

%% Writes
wopts() ->
  [ {w,               quorum}        %\  Majority
  , {pw,              1}             % } disk
  , {dw,              quorum}        %/  writes
  ].

%% Deletes
dopts() ->
  [ {r,               quorum}        %\
  , {pr,              1}             % \
  , {rw,              quorum}        %  \ Majority
  , {w,               quorum}        %  / deletes
  , {pw,              1}             % /
  , {dw,              quorum}        %/
  ].

%%%_* Tests ============================================================
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

%% Test cases.
basic_test() ->
  krc_test:with_mock(?thunk(
    krc_test:spawn_sync(1000, ?thunk(
      Obj       = put_req(),
      {ok, Obj} = get_req())))).

client_down_test() ->
  krc_test:with_mock([{pool_size, 1}], ?thunk(
    krc_mock_client:lag(10),
    Pids = krc_test:spawn_async(10, ?thunk(put_req())), %Fill queue
    [P]  = krc_test:spawn_async(?thunk(timer:sleep(10), put_req())),
    timer:sleep(20),
    tulib_processes:kill(P, [unlink]), %\ Request
    krc_test:sync(Pids))).             %/ dropped

out_of_time_test() ->
  krc_test:with_mock([{pool_size, 1}], ?thunk(
    krc_mock_client:lag(1000),
    krc_test:spawn_async(?thunk({error, notfound} = get_req())),
    krc_test:spawn_async(?thunk({error, notfound} = get_req())),
    krc_test:spawn_sync(?thunk({error, timeout} = get_req())))).

timeout_test() ->
  krc_test:with_mock(?thunk(
    krc_mock_client:lag(3000),
    krc_test:spawn_sync(?thunk({error, timeout} = get_req())))).

failures_test() ->
  ?MODULE:start([{riak_port, 6666}]).

disconnected_test() ->
  krc_test:with_mock(?thunk(
    krc_mock_client:disconnect(),
    krc_test:spawn_sync(?thunk({error, disconnected} = get_req())),
    timer:sleep(100))). %wait for 'EXIT' message

get_index_delete_test() ->
  krc_test:with_mock(?thunk(
    {ok, []} = ?MODULE:get_index(?MODULE, mah_bucket, mah_index, 42),
    ok       = ?MODULE:delete(?MODULE, mah_bucket, mah_key))).

coverage_test() ->
  krc_test:with_mock(?thunk(
     process_flag(trap_exit, true),
     {ok, Pid} = start_link([{client, krc_mock_client}]),
     {ok, _}   = start_link(mah_krc, [{client, krc_mock_client}]),
     Pid ! foo,
     gen_server:cast(mah_krc, foo),
     {ok, bar} = code_change(foo,bar,baz))).

%% Requests.
put_req() ->
  Obj = krc_obj:new(mah_bucket, self(), 42),
  ok  = ?MODULE:put(?MODULE, Obj),
  Obj.

get_req() -> ?MODULE:get(?MODULE, mah_bucket, self()).

-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
