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
-record(state, {heartbeats,labels,reg_name,added,deleted,sum_delay}).

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

    %Delay=app_helper:get_env(riak_kv, causal_service_delay),  % later use to get delay

    GrossPrefLists = riak_core_ring:all_preflists(Ring, 1),
    Dict1 = lists:foldl(fun(PrefList, Dict) ->
        {Partition, _Node} = hd(PrefList),
        riak_kv_vnode:heartbeat(PrefList),
        dict:store(Partition, 0, Dict)
                       end, dict:new(), GrossPrefLists),
    lager:info("dictionary size is ~p ~n",[dict:size(Dict1)]),
    {ok, #state{heartbeats = Dict1,labels = orddict:new(), reg_name = ServerName,added = 0,deleted = 0,sum_delay = 0}}.


handle_call({trigger},_From, State=#state{added = Added,deleted = Deleted,sum_delay = Delay}) ->
    lager:info("added count is ~p deleted count is ~p sum delay is ~p delay-per-label is ~p ~n",[Added,Deleted,Delay,Delay/Deleted]),
    {reply,ok,State};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.


handle_cast({add_label,Label,Partition},State=#state{labels = Labels,heartbeats = Heartbeats,added = Added,deleted = Deleted,sum_delay = Sum_Delay})->
    %lager:info("received label from ~p ~n",[Partition]),
    Label_Timestamp=Label#label.timestamp,
    Labels1=orddict:append(Label_Timestamp,Label,Labels),
    Heartbeats1= dict:store(Partition,Label_Timestamp,Heartbeats),

    %todo: test functionality of only send heartbeats when no label has sent fix @ vnode
    {Labels2,Deleted1,Sum_Delay1}=deliver_possible_labels(Labels1,Heartbeats1,Deleted,Sum_Delay),

    State1=State#state{labels = Labels2,heartbeats = Heartbeats1,added = Added+1,deleted = Deleted1,sum_delay = Sum_Delay1},

    %lager:info("Label ~p and heartbeat is ~p",[orddict:fetch(Label_Timestamp,Labels1),dict:fetch(Partition,Heartbeats1)]),
    {noreply,State1};

handle_cast({partition_heartbeat,Clock,Partition},State=#state{labels = Labels,heartbeats = Heartbeats,deleted = Deleted,sum_delay = Sum_Delay})->
    %lager:info("received heartbeat from partition ~p and clock of it is ~p",[Partition,Clock]),
    Heartbeats1=dict:store(Partition,Clock,Heartbeats),
    {Labels1,Deleted1,Sum_Delay1}= deliver_possible_labels(Labels,Heartbeats1,Deleted,Sum_Delay),
    %lager:info("remaining elements in dictionary is ~p ~n",[orddict:size(Labels1)]),
    State1=State#state{labels = Labels1,heartbeats = Heartbeats1,deleted = Deleted1,sum_delay = Sum_Delay1},
    {noreply,State1};

handle_cast(_Request, State) ->
    lager:error("received an unexpected  message ~n"),
    {noreply, State}.


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
         true-> {Deleted2,Sum_Delay1}=lists:foldl(fun(Label,Deleted1)->
                                              %lager:info("deleted label is ~p type is ~p ~n",[Label,Type]),
                                              %deliver labels to the other datacenters
                                              Sum_Delay_Till_Now=calculate_sum_delay(Label#label.timestamp,Sum_Delay),
                                              {Deleted1+1,Sum_Delay_Till_Now}
                                     end,Deleted,List_Labels),
                 %Dict1=orddict:erase(Clock,Dict),%delete all labels with Clock
                 deliver_labels(Min_Clock,Rest,Deleted2,Sum_Delay1);
         false->{[Head|Rest],Deleted,Sum_Delay}
    end.


calculate_sum_delay(Added_Timestamp,Sum_Delay)->
                                                    Current_Time=riak_kv_util:get_timestamp(),
                                                    Diff=Current_Time-Added_Timestamp,
                                                    case (Diff>0) of
                                                               true->Sum_Delay+Diff;
                                                               false->Sum_Delay   %due to clock drifts or non monotonocity
                                                               end.

%test label delivery
ordered_dict_test()->
    Dic=dict:new(),
    Dic1=dict:store(1,1000,Dic),
    Dic2=dict:store(2,999,Dic1),
    Dic3=dict:store(3,1010,Dic2),
    Dic4=dict:store(4,1020,Dic3),

    List_Clocks=dict:to_list(Dic4),
    Min_Clock=get_stable_timestamp(List_Clocks),
    io:format("*****min is ******* ~p ~n",[Min_Clock]),

    Dict=orddict:new(),
    Dict1=orddict:append(1000,1000,Dict),
    Dict2=orddict:append(999,999,Dict1),
    Dict3=orddict:append(1000,1000,Dict2),
    Dict4=orddict:append(1001,1001,Dict3),
    Dict5=orddict:append(998,998,Dict4),
    Dict7=orddict:append(998,997,Dict5),

    Deleted=0,
    Dict6=deliver_labels(Min_Clock,Dict7,Deleted,"test"),
    print_dict(Dict6).

print_dict(Dict4)->
    lists:foldl(fun({Key,Value},_Care)->io:format("Key is ~p and value is ~p ~n",[Key,Value])  end,dict:new(),Dict4).
