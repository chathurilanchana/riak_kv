%%%-------------------------------------------------------------------
%%% @author chathuri
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 02. Mar 2016 12:40
%%%-------------------------------------------------------------------
-module(riak_kv_ord_service_ets_ordered).
-author("chathuri").

-include_lib("stdlib/include/ms_transform.hrl").

-behaviour(gen_server).

%% API
-export([start_link/0]).
-include("riak_kv_causal_service.hrl").
-define(Label_Table_Name, labels).


%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-export([add_label/3,test/0,partition_heartbeat/2,print_status/0]).

-define(SERVER, ?MODULE).

-record(state, {heartbeats,reg_name,added,deleted,batch_to_deliver,ignore,is_first_label}).

test()->
    Status=net_kernel:connect_node('riak@127.0.0.1'), %this is the node where we run global server
    global:sync(),
    io:format("calling test ~p ~n",[Status]),
    case catch gen_server:call({global,riak_kv_ord_service_ets_ordered},{test}) of
        {'EXIT', ErrorExit} -> io:fwrite("ErrorExit ~p~n",[ErrorExit]),lager:info("error is ~p ~n",[ErrorExit]);
        {_, _}  ->lager:info("another error occured  ~n");
        ok->lager:info("this is working")
    end.


add_label(Label,Client_Id,MaxTS)->
    %lager:info("label is ready to add to the ordeing service  ~p",[Label]),
    gen_server:cast({global,riak_kv_ord_service_ets_ordered},{add_label,Label,Client_Id,MaxTS}).

partition_heartbeat(Partition,Clock)->
    gen_server:cast({global,riak_kv_ord_service_ets_ordered},{partition_heartbeat,Clock,Partition}).

%to print status when we need
print_status()->
    gen_server:call({global,riak_kv_ord_service_ets_ordered}, {trigger}).


start_link() ->
    gen_server:start_link({global,riak_kv_ord_service_ets_ordered}, ?MODULE, [riak_kv_ord_service_ets_ordered], []).

init([ServerName]) ->
    %lager:info("ordering service started"),
    %{X,Y} =erlang:process_info(global:whereis_name(riak_kv_ord_service_ets_ordered), memory),
    %lager:info("ordering service started ~p ~p ~n",[X,Y]),
    %process_flag(min_heap_size, 100000),
    %memsup:set_procmem_high_watermark(0.6),
    %{P,Q} =erlang:process_info(global:whereis_name(riak_kv_ord_service_ets_ordered), memory),
    %lager:info("after memory is ~p ~p ~n",[P,Q]),
    ClientCount=app_helper:get_env(riak_kv, clients),
    lager:info("client_count is ~p ~n",[ClientCount]),
    Batch_Delivery_Size= app_helper:get_env(riak_kv,receiver_batch_size),
    lager:info("batch delivery size is ~p ~n",[Batch_Delivery_Size]),
    Dict1=get_clients(ClientCount,dict:new()),
    lager:info("dictionary size is ~p ~n",[dict:size(Dict1)]),
    erlang:send_after(60000, self(), print_stats),
    ets:new(?Label_Table_Name, [ordered_set, named_table,private]),
    {ok, #state{heartbeats = Dict1, reg_name = ServerName,added = 0,deleted = 0,batch_to_deliver = Batch_Delivery_Size,ignore=true,is_first_label = true}}.

handle_call({trigger},_From, State=#state{added = Added,deleted = Deleted}) ->
    lager:info("added count is ~p deleted count is ~p ~n",[Added,Deleted]),
    {reply,ok,State};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.


handle_cast({add_label,BatchedLabels,Partition,MaxTS},State=#state{heartbeats = Heartbeats,added = Added,deleted = Deleted,batch_to_deliver = Batch_Delivery_Size,ignore=Should_Ignore,is_first_label = Is_First})->
    % lager:info("received label from ~p ~n",[Partition]),
    State1=case Is_First of
             true->erlang:send_after(10000, self(), disable_ignore),State#state{is_first_label = false};
              _->State
           end,

    State2=case Should_Ignore of
                true->State1;
                   _-> Added1=insert_batch_labels(BatchedLabels,Partition,Added),
                       Heartbeats1= dict:store(Partition,MaxTS,Heartbeats),
                       %todo: test functionality of only send heartbeats when no label has sent fix @ vnode
                       {Deleted1,Batch_To_Deliver}=deliver_possible_labels(Heartbeats1,Deleted,Batch_Delivery_Size),
                       Diff_Delete=Deleted1-Deleted,
                       case Diff_Delete>10000 of
                           true->lager:info("*****heavy delete of ~p noticed ~n",[Diff_Delete]);
                           _   ->noop
                       end,
                       case Batch_To_Deliver of
                           [] ->noop;
                           _  -> riak_kv_ord_service_receiver:deliver_to_receiver(Batch_To_Deliver)
                       end,
                       State#state{heartbeats = Heartbeats1,added = Added1,deleted = Deleted1}
             end,

    {noreply,State2};


handle_cast(_Request, State) ->
    lager:error("received an unexpected  message ~n"),
    {noreply, State}.

handle_info(disable_ignore, State) ->
    {noreply, State#state{ignore = false}};%stabilised to receive labels


handle_info(print_stats, State=#state{added = Added,deleted = Deleted}) ->
    {_,{Hour,Min,Sec}} = erlang:localtime(),
    lager:info("timestamp ~p: ~p: ~p: added ~p deleted ~p ~n",[Hour,Min,Sec,Added,Deleted]),
    erlang:send_after(10000, self(), print_stats),
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


get_clients(N, Dict) -> if
                            N>0 ->Dict1=dict:store(N, 0, Dict) ,get_clients(N-1,Dict1);
                            true ->Dict
                        end.

deliver_possible_labels(Heartbeats,Deleted,Batch_Delivery_Size)->
    Min_Stable_Timestamp=get_stable_timestamp(Heartbeats),
    deliver_labels(Min_Stable_Timestamp,Deleted,[],Batch_Delivery_Size).

insert_batch_labels([],_Partition,Added)->Added;

insert_batch_labels([Head|Rest],Partition,Added)->
    Label_Timestamp=Head#label.timestamp,
    ets:insert(?Label_Table_Name,{{Label_Timestamp,Partition,Head},dt}),
    insert_batch_labels(Rest,Partition,Added+1).

get_stable_timestamp(Heartbeats)->
    HB_List=dict:to_list(Heartbeats),
    [First|Rest]=HB_List,
    {_Partition,Clock}=First,
    lists:foldl(fun({_Key,Val},Min)->
        %lager:info("key is ~p value is ~p ~n",[Key,Val]),
        if
            Val<Min-> Val;
            true -> Min
        end end,Clock,Rest).

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
