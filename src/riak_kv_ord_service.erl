%%%-------------------------------------------------------------------
%%% @author chathuri
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 02. Mar 2016 12:40
%%%-------------------------------------------------------------------
-module(riak_kv_ord_service).
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

-export([add_label/2,test/0,partition_heartbeat/2,print_status/0]).

-define(SERVER, ?MODULE).

-record(state, {heartbeats,labels,reg_name,added,deleted,sum_delay,highest_delay}).

test()->
    Status=net_kernel:connect_node('riak@127.0.0.1'), %this is the node where we run global server
    global:sync(),
      io:format("calling test ~p ~n",[Status]),
    case catch gen_server:call({global,riak_kv_ord_service},{test}) of
        {'EXIT', ErrorExit} -> io:fwrite("ErrorExit ~p~n",[ErrorExit]),lager:info("error is ~p ~n",[ErrorExit]);
            {_, _}  ->lager:info("another error occured  ~n");
            ok->lager:info("this is working")
    end.


add_label(Label,Client_Id)->
    %lager:info("label is ready to add to the ordeing service  ~p",[Label]),
    gen_server:cast({global,riak_kv_ord_service},{add_label,Label,Client_Id}).

partition_heartbeat(Partition,Clock)->
    gen_server:cast({global,riak_kv_ord_service},{partition_heartbeat,Clock,Partition}).

%to print status when we need
print_status()->
    gen_server:call({global,riak_kv_ord_service}, {trigger}).


start_link() ->
    gen_server:start_link({global,riak_kv_ord_service}, ?MODULE, [riak_kv_ord_service], []).

init([ServerName]) ->
    lager:info("ordering service started"),
    ClientCount=app_helper:get_env(riak_kv, clients),
    lager:info("client_count is ~p ~n",[ClientCount]),
    Dict1=get_clients(ClientCount,dict:new()),
    lager:info("dictionary size is ~p ~n",[dict:size(Dict1)]),
    erlang:send_after(30000, self(), print_stats),
    {ok, #state{heartbeats = Dict1,labels = orddict:new(), reg_name = ServerName,added = 0,deleted = 0,sum_delay = 0,highest_delay = 0}}.



handle_call({trigger},_From, State=#state{added = Added,deleted = Deleted,sum_delay = Delay,highest_delay = Max_Delay}) ->
    Delay_Per_Op=Delay div Deleted,
    lager:info("added count is ~p deleted count is ~p delay-per-op is ~p max-delay is ~p ~n",[Added,Deleted,Delay_Per_Op,Max_Delay]),
    {reply,ok,State};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.


handle_cast({add_label,Label,Partition},State=#state{labels = Labels,heartbeats = Heartbeats,added = Added,deleted = Deleted,sum_delay = Sum_Delay,highest_delay = _Max_Delay})->
    %lager:info("received label from ~p ~n",[Partition]),
    Label_Timestamp=Label#label.timestamp,
    Labels1=orddict:append(Label_Timestamp,Label,Labels),
    Heartbeats1= dict:store(Partition,Label_Timestamp,Heartbeats),
    %todo: test functionality of only send heartbeats when no label has sent fix @ vnode
    {Labels2,Deleted1,Sum_Delay1}=deliver_possible_labels(Labels1,Heartbeats1,Deleted,Sum_Delay),

    State1=State#state{labels = Labels2,heartbeats = Heartbeats1,added = Added+1,deleted = Deleted1,sum_delay = Sum_Delay1},

    %lager:info("Label ~p and heartbeat is ~p",[orddict:fetch(Label_Timestamp,Labels1),dict:fetch(Partition,Heartbeats1)]),
    {noreply,State1};


handle_cast(_Request, State) ->
    lager:error("received an unexpected  message ~n"),
    {noreply, State}.

handle_info(print_stats, State=#state{added = Added,deleted = Deleted,highest_delay = Max_Delay}) ->
    {_,{Hour,Min,Sec}} = erlang:localtime(),
    case (State#state.deleted>0) of
        true->
            %add_line_to_file(Added,Deleted,Max_Delay,FileName);
            lager:info("timestamp ~p: ~p: ~p: added ~p deleted ~p max-delay ~p ~n",[Hour,Min,Sec,Added,Deleted,Max_Delay]);
        false->%add_line_to_file(Added,0,Max_Delay,FileName)
            lager:info("timestamp ~p: ~p: ~p: added ~p deleted ~p max-delay ~p ~n",[Hour,Min,Sec,Added,0,Max_Delay])
    end,
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

get_clients(N, Dict) -> if
                       N>0 ->Dict1=dict:store(N, 0, Dict) ,get_clients(N-1,Dict1);
                       true ->Dict
                   end.

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
        true-> {Deleted2,Sum_Delay2}=lists:foldl(fun(_Label,{Deleted1,_Sum_Delay1})->
            %deliver labels to the other datacenters
                                                        {Deleted1+1,0}
                                                 end,{Deleted,Sum_Delay},List_Labels),
            %Dict1=orddict:erase(Clock,Dict),%delete all labels with Clock
            deliver_labels(Min_Clock,Rest,Deleted2,Sum_Delay2);
        false->{[Head|Rest],Deleted,Sum_Delay}
    end.

