%%%-------------------------------------------------------------------
%%% @author chathuri
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 14. Feb 2016 10:05
%%%-------------------------------------------------------------------
-author("chathuri").
-record(label,{
    bkey,
    timestamp,
    vector
}).
-define(STABILIZER_PREFIX, "riak_kv_ord_service_").
-define(REMOTE_ORD_SERVER_PREFIX,"riak_kv_remote_ord_service").
-define(RECEIVER_PER_DC_PREFIX, "riak_kv_ord_service_receiver_per_dc").
-define(RECEIVER_PER_NODE,riak_kv_receiver_per_node).