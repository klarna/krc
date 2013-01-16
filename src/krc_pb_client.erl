%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc Protobuf client.
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
-module(krc_pb_client).
-behaviour(krc_riak_client).

%%%_* Exports ==========================================================
-export([ delete/5
        , get/5
        , get_index/5
        , put/4
        , start_link/3
        ]).

%%%_* Includes =========================================================
-include_lib("tulib/include/prelude.hrl").

%%%_* Code =============================================================
delete(Pid, Bucket, Key, Options, Timeout) ->
  case
    riakc_pb_socket:delete(
      Pid, krc_obj:encode(Bucket), krc_obj:encode(Key), Options, Timeout)
  of
    ok               -> ok;
    {error, _} = Err -> Err
  end.

get(Pid, Bucket, Key, Options, Timeout) ->
  case
    riakc_pb_socket:get(
      Pid, krc_obj:encode(Bucket), krc_obj:encode(Key), Options, Timeout)
  of
    {ok, Obj}        -> {ok, krc_obj:from_riakc_obj(Obj)};
    {error, _} = Err -> Err
  end.

get_index(Pid, Bucket, Index, Key, Timeout) ->
  case
    riakc_pb_socket:get_index(Pid,
                              krc_obj:encode(Bucket),
                              krc_obj:encode_idx(Index),
                              krc_obj:encode_idx_key(Key),
                              Timeout,
                              infinity) %gen_server call
  of
    {ok, Keys}       -> {ok, [krc_obj:decode(K) || K <- Keys]};
    {error, _} = Err -> Err
  end.

put(Pid, Obj, Options, Timeout) ->
  case
    riakc_pb_socket:put(Pid, krc_obj:to_riakc_obj(Obj), Options, Timeout)
  of
    ok               -> ok;
    {error, _} = Err -> Err
  end.

start_link(IP, Port, Options) ->
  {ok, Pid} = riakc_pb_socket:start_link(IP, Port, Options),
  pong      = riakc_pb_socket:ping(Pid), %ensure server actually reachable
  {ok, Pid}.

%%%_* Tests ============================================================
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
