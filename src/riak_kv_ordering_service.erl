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
    code_change/3]).

-export([ordered_dict_test/0]).   %to test ordered dict label delivery

-include("riak_kv_causal_service.hrl").
-define(SERVER, ?MODULE).

-record(state, {heartbeats,labels,reg_name}).

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
    {ok, #state{heartbeats = Dict1,labels = orddict:new(), reg_name = ServerName}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.


handle_cast({add_label,Label,Partition},State=#state{labels = Labels,heartbeats = Heartbeats})->
    %Now=riak_kv_util:current_monotonic_time(),  %time we get at ord service should be monotonic
    lager:info("received label from ~p ~n",[Partition]),
    Label_Timestamp=Label#label.timestamp,
    Labels1=orddict:append(Label_Timestamp,Label,Labels),
    Heartbeats1= dict:store(Partition,Label_Timestamp,Heartbeats),

    %todo: test functionality of only send heartbeats when no label has sent fix @ vnode
    Labels2=deliver_possible_labels(Labels1,Heartbeats1,"add label"),

    State1=State#state{labels = Labels2,heartbeats = Heartbeats1},

    %lager:info("Label ~p and heartbeat is ~p",[orddict:fetch(Label_Timestamp,Labels1),dict:fetch(Partition,Heartbeats1)]),
    {noreply,State1};

handle_cast({partition_heartbeat,Clock,Partition},State=#state{labels = Labels,heartbeats = Heartbeats})->
    %lager:info("received heartbeat from partition ~p and clock of it is ~p",[Partition,Clock]),
    Heartbeats1=dict:store(Partition,Clock,Heartbeats),
    Labels1= deliver_possible_labels(Labels,Heartbeats1,"heartbeat"),
    %lager:info("remaining elements in dictionary is ~p ~n",[orddict:size(Labels1)]),
    State1=State#state{labels = Labels1,heartbeats = Heartbeats1},
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
deliver_possible_labels(Labels,Heartbeats,Type)->
    Min_Stable_Timestamp=get_stable_timestamp(Heartbeats),
    deliver_labels(Min_Stable_Timestamp,Labels,Type).

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

deliver_labels(_Min_Clock,[],_Type)->[];

deliver_labels(Min_Clock,[Head|Rest],Type)->
    {Clock,List_Labels}=Head,
    %lager:info("list label is ~p and min clock is ~p ~n",[List_Labels,Min_Clock]),
    case (Clock =< Min_Clock) of
         true-> lists:foreach(fun(Label)->lager:info("deleted label is ~p type is ~p ~n",[Label,Type]) end,List_Labels),%safe to deliver label to other side
             %Dict1=orddict:erase(Clock,Dict),%delete all labels with Clock
             deliver_labels(Min_Clock,Rest,Type);
         false->[Head|Rest]
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

    Dict6=deliver_labels(Min_Clock,Dict7,"test"),
    print_dict(Dict6).

print_dict(Dict4)->
    lists:foldl(fun({Key,Value},_Care)->io:format("Key is ~p and value is ~p ~n",[Key,Value])  end,dict:new(),Dict4).
