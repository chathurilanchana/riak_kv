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

-export([add_label/4,check_ready/0,partition_heartbeat/2,print_status/0,check_node_up/1,stop/0,notify_primary/1,notify_stable_ts/3]).

-define(SERVER, ?MODULE).

-record(state, {heartbeats,added,deleted,reg_name,batch_to_deliver,is_primary,deleted_by_me,current_min_stable,ignore,is_first_label,receiver_name}).

check_ready() ->
    MyId=app_helper:get_env(riak_kv, myid),
    Ord_Service_Name=string:concat(?SERVICE_PREFIX,integer_to_list(MyId)),
    gen_server:call({global, Ord_Service_Name}, check_ready, infinity).

check_node_up(Service_Name)->
    try gen_server:call({global, Service_Name},check_node_up)
    catch
       exit:{_,_} -> timeout
    end.


notify_stable_ts(Service_Name,Stable_TS,Primary_Name)->
    try gen_server:cast({global, Service_Name},{stable_ts,Stable_TS,Primary_Name})
    catch
        exit:{_,_} -> timeout
    end.

%to simulate failures
stop() ->
    MyId=app_helper:get_env(riak_kv, myid),
    Ord_Service_Name=string:concat(?SERVICE_PREFIX,integer_to_list(MyId)),
    gen_server:call({global,Ord_Service_Name}, stop).

notify_primary(Ord_Service_Name)->
    gen_server:call({global,Ord_Service_Name},notify_primary).

add_label(Ord_Service_Name,Label,Client_Id,MaxTS)->
    %lager:info("label is ready to add to the ordeing service  ~p",[Label]),
    gen_server:cast({global,Ord_Service_Name},{add_label,Label,Client_Id,MaxTS}).

partition_heartbeat(Partition,Clock)->
    gen_server:cast({global,riak_kv_ord_service_ets_ordered},{partition_heartbeat,Clock,Partition}).

%to print status when we need
print_status()->
    MyId=app_helper:get_env(riak_kv, myid),
    Ord_Service_Name=string:concat(?SERVICE_PREFIX,integer_to_list(MyId)),
    gen_server:call({global,Ord_Service_Name}, {trigger}).


start_link() ->
    MyId=app_helper:get_env(riak_kv, myid),
    Ord_Service_Name=string:concat(?SERVICE_PREFIX,integer_to_list(MyId)),
    Receiver_Name=string:concat(?RECEIVER_PREFIX,integer_to_list(MyId)),
    lager:info("my id is ~p ~n",[Ord_Service_Name]),
    gen_server:start_link({global,Ord_Service_Name}, ?MODULE, [Ord_Service_Name,Receiver_Name], []).

init([Reg_Name,Receiver_Name]) ->
    lager:info("ordering service started"),
    process_flag(min_heap_size, 100000),
    memsup:set_procmem_high_watermark(0.6),
    Batch_Delivery_Size= app_helper:get_env(riak_kv,receiver_batch_size),
    ClientCount=app_helper:get_env(riak_kv, clients),
    lager:info("client_count is ~p ~n",[ClientCount]),
    Dict1=get_clients(ClientCount,dict:new()),
    lager:info("dictionary size is ~p ~n",[dict:size(Dict1)]),
    erlang:send_after(10000, self(), print_stats),
    ets:new(?Label_Table_Name, [ordered_set, named_table,private]),
    {ok, #state{heartbeats = Dict1,reg_name = Reg_Name,batch_to_deliver = Batch_Delivery_Size,added = 0,deleted = 0,is_primary = false,deleted_by_me = 0,current_min_stable = 0,ignore = true,is_first_label = true,receiver_name = Receiver_Name}}.



handle_call({trigger},_From, State=#state{added = Added,deleted = Deleted}) ->
    lager:info("added count is ~p deleted count is ~p ~n",[Added,Deleted]),
    {reply,ok,State};

handle_call(check_ready, _From, State) ->
    riak_kv_ord_service_failure_detector:test(),
    {reply, ok, State};

handle_call(check_node_up,_From,State)->
    {reply, ok, State};

handle_call(notify_primary,_From,State)->
    lager:info("I'm becoming the primary ~n"),
    {reply, ok, State#state{is_primary = true}};

handle_call(stop, _From, State=#state{added = Added,deleted=Deleted,deleted_by_me = Deleted_By_me}) ->
    lager:info("ord service stopping"),
    lager:info("added ~p deleted ~p added-by-me ~p when stopping",[Added,Deleted,Deleted_By_me]),
    {stop, normal, shutdown_ok, State};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.


handle_cast({add_label,BatchedLabels,Partition,MaxTS},State=#state{heartbeats = Heartbeats,added = Added,deleted = Deleted,is_primary = IsPrimary,reg_name = MyName,deleted_by_me =Deleted_By_Me,current_min_stable = Current_Stable,batch_to_deliver = Batch_Delivery_Size,is_first_label = IsFirst,receiver_name = Receiver_Name})->
    %ignore first 10 sec to tolerate time diffs
    State1=case IsFirst of
               true->erlang:send_after(10000, self(), disable_ignore),State#state{is_first_label = false};
               _->State
           end,

    case MaxTS>Current_Stable of
        true->Added1=insert_batch_labels(BatchedLabels,Partition,Added),
            Heartbeats1= dict:store(Partition,MaxTS,Heartbeats),
            %todo: test functionality of only send heartbeats when no label has sent fix @ vnode
            case (IsPrimary) of
                true -> %lager:info("I'm the primary"),
                    {Deleted1,New_Stable_TS,Batched_Deliverable_Labels}=deliver_possible_labels(Heartbeats1,Deleted,Batch_Delivery_Size,Receiver_Name),
                    case Batched_Deliverable_Labels of
                        []->noop;
                        _ -> riak_kv_ord_service_receiver:deliver_to_receiver(Batched_Deliverable_Labels,Receiver_Name)
                    end,
                    Diff=Deleted1-Deleted,
                    trigger_delete_high_alarm(Diff),
                    Deleted_By_Me1=Deleted_By_Me+Diff,
                    case New_Stable_TS>Current_Stable of
                        true->riak_kv_ord_service_failure_detector:send_stable_ts_to_replicas(New_Stable_TS,MyName);
                        _   ->noop
                    end;
                _ -> Deleted1=Deleted,Deleted_By_Me1=Deleted_By_Me,New_Stable_TS=Current_Stable

            end,

            State2=State1#state{heartbeats = Heartbeats1,added = Added1,deleted = Deleted1,deleted_by_me = Deleted_By_Me1,current_min_stable = New_Stable_TS};

            _   ->State2=State1 %the labels are already delivered,ignore them
    end,

    {noreply,State2};

%todo: fix running 2 primaries by sending a NACK
handle_cast({stable_ts,Stable_TS,_Primary_Name}, State=#state{deleted = Deleted}) ->
    {Deleted1,_Min_Stable_Timestamp}=deliver_labels(Stable_TS,Deleted),
    {noreply, State#state{deleted = Deleted1}};

handle_cast(_Request, State) ->
    lager:error("received an unexpected  message ~n"),
    {noreply, State}.

handle_info(print_stats, State=#state{added = Added,deleted = Deleted,deleted_by_me = Deleted_By_Me}) ->
    {_,{Hour,Min,Sec}} = erlang:localtime(),
    lager:info("timestamp ~p: ~p: ~p: added ~p deleted ~p deleted-by-me ~p ~n",[Hour,Min,Sec,Added,Deleted,Deleted_By_Me]),
    erlang:send_after(10000, self(), print_stats),
    {noreply, State};

handle_info(disable_ignore, State) ->
    {noreply, State#state{ignore = false}};%stabilised to receive labels

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

deliver_possible_labels(Heartbeats,Deleted,Batch_Delivery_Size,Receiver_Name)->
    Min_Stable_Timestamp=get_stable_timestamp(Heartbeats),
    deliver_labels(Min_Stable_Timestamp,Deleted,[],Batch_Delivery_Size,Receiver_Name).

insert_batch_labels([],_Partition,Added)->Added;

insert_batch_labels([Head|Rest],Partition,Added)->
    Label_Timestamp=Head#label.timestamp,
    ets:insert(?Label_Table_Name,{{Label_Timestamp,Partition,Head},rc}),
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


deliver_labels(Min_Stable_Timestamp,Deleted)->
    case ets:first(?Label_Table_Name)  of
        '$end_of_table' -> {Deleted,Min_Stable_Timestamp};
        {Timestamp,_Partition,_Label}=Key when Timestamp=<Min_Stable_Timestamp ->ets:delete(?Label_Table_Name,Key),deliver_labels(Min_Stable_Timestamp,Deleted+1);
        {_Timestamp,_Partition,_Label}->{Deleted,Min_Stable_Timestamp}

    end.

deliver_labels(Min_Stable_Timestamp,Deleted,Batched_Deliverable_Labels,Batch_Size,Receiver_Name)->
    Batch_To_Deliver1=case length(Batched_Deliverable_Labels)>Batch_Size of
                          true->riak_kv_ord_service_receiver:deliver_to_receiver(Batched_Deliverable_Labels,Receiver_Name),[];
                          _   ->Batched_Deliverable_Labels
                      end,
    case ets:first(?Label_Table_Name)  of
        '$end_of_table' -> {Deleted,Min_Stable_Timestamp,Batch_To_Deliver1};
        {Timestamp,_Partition,Head}=Key when Timestamp=<Min_Stable_Timestamp ->ets:delete(?Label_Table_Name,Key),deliver_labels(Min_Stable_Timestamp,Deleted+1,[Head|Batch_To_Deliver1],Batch_Size,Receiver_Name);
        {_Timestamp,_Partition,_Head}->{Deleted,Min_Stable_Timestamp,Batch_To_Deliver1}

    end.

trigger_delete_high_alarm(Diff)->case Diff>10000 of
                                     true->lager:info("****heavy delete of ~p noticed ~n***",[Diff]);
                                     false->noop
                                 end.