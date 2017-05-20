%%%-------------------------------------------------------------------
%%% @author chathuri
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 13. Feb 2016 15:56
%%%-------------------------------------------------------------------
-module(riak_kv_ordering_service).
-author("chathuri").

-behaviour(gen_server).

%% API
-export([start_link/0,add_label/3,partition_heartbeat/3]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3,
    print_status/0
]).

-export([ordered_dict_test/0]).   %to test ordered dict label delivery

-include("riak_kv_causal_service.hrl").
-define(SERVER, ?MODULE).

%delay- average delay between receiving and delivering a label
-record(state, {heartbeats,labels,reg_name,added,deleted,sum_delay,highest_delay,stat_file_name}).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    ServerName=app_helper:get_env(riak_kv,gen_server_register_name),
    gen_server:start_link({global, ServerName}, ?MODULE, [ServerName], []).

add_label(Label,Causal_Service_Id,Partition)->
    %lager:info("label is ready to add to the ordeing service  ~p",[Label]),
    gen_server:cast({global,Causal_Service_Id},{add_label,Label,Partition}).

partition_heartbeat(Partition,Clock,Causal_Service_Id)->
    gen_server:cast({global,Causal_Service_Id},{partition_heartbeat,Clock,Partition}).

%to print status when we need
print_status()->
    ServerName=app_helper:get_env(riak_kv,gen_server_register_name),
    gen_server:call({global,ServerName}, {trigger}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%server name to be sent to other processes, check whether to read from proplist
init([ServerName]) ->
    lager:info("ordering service started"),
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Num_Partitions=riak_core_ring:num_partitions(Ring),
    lager:info("total partitions are ~p and server name is ~p",[Num_Partitions,ServerName]),
    File_Name=app_helper:get_env(riak_kv, stat_name),  % later use to get delay

    GrossPrefLists = riak_core_ring:all_preflists(Ring, 1),
    Dict1 = lists:foldl(fun(PrefList, Dict) ->
        {Partition, _Node} = hd(PrefList),
        riak_kv_vnode:heartbeat(PrefList),
        dict:store(Partition, 0, Dict)
                       end, dict:new(), GrossPrefLists),
    lager:info("dictionary size is ~p ~n",[dict:size(Dict1)]),
    %erlang:send_after(10000, self(), print_stats),
    {ok, #state{heartbeats = Dict1,labels = orddict:new(), reg_name = ServerName,added = 0,deleted = 0,sum_delay = 0,highest_delay = 0,stat_file_name = File_Name}}.


handle_call({trigger},_From, State=#state{added = Added,deleted = Deleted,sum_delay = Delay,highest_delay = Max_Delay}) ->
    Delay_Per_Op=Delay div Deleted,
    lager:info("added count is ~p deleted count is ~p delay-per-op is ~p max-delay is ~p ~n",[Added,Deleted,Delay_Per_Op,Max_Delay]),
    {reply,ok,State};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.


handle_cast({add_label,Label,Partition},State=#state{labels = Labels,heartbeats = Heartbeats,added = Added,deleted = Deleted,sum_delay = Sum_Delay,highest_delay = Max_Delay})->
    %lager:info("received label from ~p ~n",[Partition]),
    Label_Timestamp=Label#label.timestamp,
    Labels1=orddict:append(Label_Timestamp,Label,Labels),
    Heartbeats1= dict:store(Partition,Label_Timestamp,Heartbeats),
    Max_Delay1=get_max_delay(Labels,Max_Delay),
    %todo: test functionality of only send heartbeats when no label has sent fix @ vnode
    {Labels2,Deleted1,Sum_Delay1}=deliver_possible_labels(Labels1,Heartbeats1,Deleted,Sum_Delay),

    State1=State#state{labels = Labels2,heartbeats = Heartbeats1,added = Added+1,deleted = Deleted1,sum_delay = Sum_Delay1,highest_delay = Max_Delay1},

    %lager:info("Label ~p and heartbeat is ~p",[orddict:fetch(Label_Timestamp,Labels1),dict:fetch(Partition,Heartbeats1)]),
    {noreply,State1};

handle_cast({partition_heartbeat,Clock,Partition},State=#state{labels = Labels,heartbeats = Heartbeats,deleted = Deleted,sum_delay = Sum_Delay,highest_delay = Max_Delay})->
    %lager:info("received heartbeat from partition ~p and clock of it is ~p",[Partition,Clock]),
    Heartbeats1=dict:store(Partition,Clock,Heartbeats),
    Max_Delay1=get_max_delay(Labels,Max_Delay),
    {Labels1,Deleted1,Sum_Delay1}= deliver_possible_labels(Labels,Heartbeats1,Deleted,Sum_Delay),
    %lager:info("remaining elements in dictionary is ~p ~n",[orddict:size(Labels1)]),
    State1=State#state{labels = Labels1,heartbeats = Heartbeats1,deleted = Deleted1,sum_delay = Sum_Delay1,highest_delay = Max_Delay1},
    {noreply,State1};

handle_cast(_Request, State) ->
    lager:error("received an unexpected  message ~n"),
    {noreply, State}.

handle_info(print_stats, State=#state{added = Added,deleted = Deleted,highest_delay = Max_Delay,stat_file_name = _FileName}) ->
    {_,{Hour,Min,Sec}} = erlang:localtime(),
    case (State#state.deleted>0) of
        true->
              %add_line_to_file(Added,Deleted,Max_Delay,FileName);
              lager:info("timestamp ~p: ~p: ~p: added ~p deleted ~p max-delay ~p ~n",[Hour,Min,Sec,Added,Deleted,Max_Delay]);
        false->%add_line_to_file(Added,0,Max_Delay,FileName)
            lager:info("timestamp ~p: ~p: ~p: added ~p deleted ~p max-delay ~p ~n",[Hour,Min,Sec,Added,0,Max_Delay])
    end,
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
deliver_possible_labels(Labels,Heartbeats,Deleted,Sum_Delay)->
    Min_Stable_Timestamp=get_stable_timestamp(Heartbeats),
    deliver_labels(Min_Stable_Timestamp,Labels,Deleted,Sum_Delay).

get_max_delay(Labels,Current_Max_Delay)->

    case(orddict:size(Labels)>0) of
        true-> Earliest_Ts=get_earliest_label_timestamp(Labels,Current_Max_Delay),
               New_Max_Delay=riak_kv_util:get_timestamp()-Earliest_Ts,

               case(New_Max_Delay>Current_Max_Delay) of
                    true->New_Max_Delay;
                    false->Current_Max_Delay
               end;
         false->Current_Max_Delay
    end.

get_earliest_label_timestamp([],Current_Max_Delay)->Current_Max_Delay;

get_earliest_label_timestamp(Labels,_Current_Max_Delay)->
    HB_List=orddict:to_list(Labels),
    [First|Rest]=HB_List,
    {Clock,_Label}=First,
    lists:foldl(fun({Key,_Val},Min)->
        %lager:info("key is ~p value is ~p ~n",[Key,Val]),
        if
            Key<Min-> Key;
            true -> Min
        end end,Clock,Rest).


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

deliver_labels(_Min_Clock,[],Deleted,Sum_Deay)->{[],Deleted,Sum_Deay};

deliver_labels(Min_Clock,[Head|Rest],Deleted,Sum_Delay)->
    {Clock,List_Labels}=Head,
    %lager:info("list label is ~p and min clock is ~p ~n",[List_Labels,Min_Clock]),
    case (Clock =< Min_Clock) of
         true-> {Deleted2,Sum_Delay2}=lists:foldl(fun(Label,{Deleted1,Sum_Delay1})->
                                              %deliver labels to the other datacenters
                                              {Deleted1+1,calculate_sum_delay(Label#label.timestamp,Sum_Delay1)}
                                     end,{Deleted,Sum_Delay},List_Labels),
                 %Dict1=orddict:erase(Clock,Dict),%delete all labels with Clock
                 deliver_labels(Min_Clock,Rest,Deleted2,Sum_Delay2);
         false->{[Head|Rest],Deleted,Sum_Delay}
    end.


calculate_sum_delay(Added_Timestamp,Sum_Delay)->
                                                    Current_Time=riak_kv_util:get_timestamp(),
                                                    Diff_in_Msec=(Current_Time-Added_Timestamp) div 1000,

                                                    case (Diff_in_Msec>0) of
                                                               true->(Sum_Delay+Diff_in_Msec);
                                                               false->Sum_Delay   %due to clock drifts or non monotonocity
                                                               end.

