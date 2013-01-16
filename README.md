                                  The
           _           _       _           _ _            _
          | | __  _ __(_) __ _| | __   ___| (_) ___ _ __ | |_
          | |/ / | '__| |/ _` | |/ /  / __| | |/ _ \ '_ \| __|
          |   <  | |  | | (_| |   <  | (__| | |  __/ | | | |_
          |_|\_\ |_|  |_|\__,_|_|\_\  \___|_|_|\___|_| |_|\__|

## Overview
A simple wrapper around the official Riak clients for Erlang.

Costs an extra copy, yields nicer API.

## Installation
jakob@moody.primat.es:~/git/klarna/krc$ gmake

jakob@moody.primat.es:~/git/klarna/krc$ gmake test

## Manifest
* krc.erl             -- main API
* krc.hrl             -- internal header
* krc_app.erl         -- application
* krc_mock_client.erl -- mock backend client
* krc_obj.erl         -- like riak_obj
* krc_pb_client.erl   -- protobuffs backend client
* krc_riak_client.erl -- backend client interface
* krc_server.erl      -- worker pool
* krc_sup.erl         -- supervisor
* krc_test.erl        -- test support
