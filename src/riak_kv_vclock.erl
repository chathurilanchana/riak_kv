%%%-------------------------------------------------------------------
%%% @author chathuri
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 27. Apr 2016 16:55
%%%-------------------------------------------------------------------
-module(riak_kv_vclock).
-author("chathuri").

%% API
-export([is_stable/2,get_max_vector/2]).

%no need to check the sender's clock as deps from him always ordered
is_stable(MyVclock,ReceivedVclock)->
  lists:all(fun(Key) ->
                  MyElem=dict:fetch(Key,MyVclock),
                  ReceivedElem=dict:fetch(Key,ReceivedVclock),
                  ReceivedElem =< MyElem
            end,dict:fetch_keys(ReceivedVclock)).

get_max_vector(MyVclock,ReceivedVclock)->
 lists:foldl(fun(Key, Vector) ->
      MyClock = dict:fetch(Key, MyVclock),
      ReceivedClock = dict:fetch(Key, ReceivedVclock),
      Max= max(MyClock,ReceivedClock),
      dict:store(Key, Max, Vector)
         end, dict:new(),dict:fetch_keys(ReceivedVclock)).
