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
-export([is_label_deliverable/3,get_max_vector/2]).

is_label_deliverable(MyVclock,ReceivedVclock,SenderId)->
  lists:all(fun(Key) ->
    MyElem=dict:fetch(Key,MyVclock),
    ReceivedElem=dict:fetch(Key,ReceivedVclock),
    case Key of
      SenderId->ReceivedElem=:=MyElem+1;  %no need to check this as that dc send them ordered; Just for safty
      _          ->  ReceivedElem =< MyElem
    end
            end,dict:fetch_keys(ReceivedVclock)).

get_max_vector(MyVclock,ReceivedVclock)->
 lists:foldl(fun(Key, Vector) ->
      MyClock = dict:fetch(Key, MyVclock),
      ReceivedClock = dict:fetch(Key, ReceivedVclock),
      Max= max(MyClock,ReceivedClock),
      dict:store(Key, Max, Vector)
         end, dict:new(),dict:fetch_keys(ReceivedVclock)).
