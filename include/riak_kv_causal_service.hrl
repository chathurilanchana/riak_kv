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
    req_id,
    bkey,
    timestamp,
    node_id
}).

-define(SERVICE_PREFIX, "riak_kv_ord_service_").