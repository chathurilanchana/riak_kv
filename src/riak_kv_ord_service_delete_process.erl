%%%-------------------------------------------------------------------
%%% @author chathuri
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 01. Apr 2016 16:19
%%%-------------------------------------------------------------------
-module(riak_kv_ord_service_delete_process).
-author("chathuri").

-behaviour(gen_server).

%% API
-export([start_link/0]).

-include("riak_kv_causal_service.hrl").

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {deleted=0,batch_to_deliver,label_delete_interval}).

start_link() ->
    gen_server:start_link({global,riak_kv_ord_service_delete}, ?MODULE, [], []).

init([]) ->
    Batch_Delivery_Size= app_helper:get_env(riak_kv,receiver_batch_size),
    Label_Delete_Interval= app_helper:get_env(riak_kv,label_delete_interval),
    lager:info("batch delivery size is ~p ~n",[Batch_Delivery_Size]),
    erlang:send_after(Label_Delete_Interval, self(), delete_labels),
    erlang:send_after(30000, self(), print_stats),
    {ok, #state{deleted = 0,batch_to_deliver = Batch_Delivery_Size,label_delete_interval =Label_Delete_Interval}}.



handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(delete_labels, State=#state{deleted = Deleted,batch_to_deliver = Batch_Delivery_Size,label_delete_interval = Delete_Interval}) ->
    {Deleted1,Batch_To_Deliver}=deliver_possible_labels(Deleted,Batch_Delivery_Size),
    Diff_Delete=Deleted1-Deleted,
    case Diff_Delete>10000 of
        true->lager:info("*****heavy delete of ~p noticed ~n",[Diff_Delete]);
        _   ->noop
    end,
    case Batch_To_Deliver of
        [] ->noop;
        _  -> riak_kv_ord_service_receiver:deliver_to_receiver(Batch_To_Deliver)
    end,
    erlang:send_after(Delete_Interval, self(), delete_labels),
    {noreply, State#state{deleted = Deleted1}};

handle_info(print_stats, State=#state{deleted =  Deleted}) ->
    {_,{Hour,Min,Sec}} = erlang:localtime(),
    lager:info("timestamp ~p: ~p: ~p: total deleted is ~p ~n",[Hour,Min,Sec,Deleted]),
    erlang:send_after(30000, self(), print_stats),
    {noreply, State};

handle_info(_Info, State) ->
    {noreply, State}.


terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
deliver_possible_labels(Deleted,Batch_Delivery_Size)->
    Min_Stable_Timestamp=get_stable_timestamp(),
    deliver_labels(Min_Stable_Timestamp,Deleted,[],Batch_Delivery_Size).

deliver_labels(Min_Stable_Timestamp,Deleted,Batch_To_Deliver,Batch_Delivery_Size)->
    Batch_To_Deliver1=case length(Batch_To_Deliver)>Batch_Delivery_Size of
                          true->riak_kv_ord_service_receiver:deliver_to_receiver(Batch_To_Deliver),[];
                          _   ->Batch_To_Deliver
                      end,
    case ets:first(?Label_Table_Name)  of
        '$end_of_table' -> {Deleted,Batch_To_Deliver1};
        {Timestamp,_Partition,Label}=Key when Timestamp=<Min_Stable_Timestamp ->
            ets:delete(?Label_Table_Name,Key),
            deliver_labels(Min_Stable_Timestamp,Deleted+1,[Label|Batch_To_Deliver1],Batch_Delivery_Size);
        {_Timestamp,_Partition,_Label}->{Deleted,Batch_To_Deliver1}
    end.


get_stable_timestamp()->
    ets:foldl(fun({_Partition, Timestamp}, Min) ->
        if
            Timestamp<Min-> Timestamp;
            true -> Min
        end
              end, infinity, ?HB_TABLE_NAME).