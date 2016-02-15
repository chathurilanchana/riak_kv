%%%-------------------------------------------------------------------
%%% @author chathuri
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 14. Feb 2016 10:12
%%%-------------------------------------------------------------------
-module(riak_kv_causal_service_util).
-author("chathuri").
-include("riak_kv_causal_service.hrl").
%% API
-export([create_label/4]).

create_label(ReqId,BKey,Timestamp,Partition)->
    #label{req_id = ReqId,bkey = BKey,timestamp = Timestamp,node_id =Partition }.
