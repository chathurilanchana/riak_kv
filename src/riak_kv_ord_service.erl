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

-export([add_label/1,test/0,partition_heartbeat/2,print_status/0]).

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


add_label(Label)->
    %lager:info("label is ready to add to the ordeing service  ~p",[Label]),
    gen_server:cast({global,riak_kv_ord_service},{add_label,Label}).

partition_heartbeat(Partition,Clock)->
    gen_server:cast({global,riak_kv_ord_service},{partition_heartbeat,Clock,Partition}).

%to print status when we need
print_status()->
    gen_server:call({global,riak_kv_ord_service}, {trigger}).


start_link() ->
    gen_server:start_link({global,riak_kv_ord_service}, ?MODULE, [riak_kv_ord_service], []).

init([ServerName]) ->
    lager:info("ordering service started"),
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Num_Partitions=riak_core_ring:num_partitions(Ring),
    lager:info("total partitions are ~p and server name is ~p",[Num_Partitions,ServerName]),

    GrossPrefLists = riak_core_ring:all_preflists(Ring, 1),
    Dict1 = lists:foldl(fun(PrefList, Dict) ->
        {Partition, _Node} = hd(PrefList),
        riak_kv_vnode:heartbeat(PrefList),
        dict:store(Partition, 0, Dict)
                        end, dict:new(), GrossPrefLists),
    lager:info("dictionary size is ~p ~n",[dict:size(Dict1)]),
    {ok, #state{heartbeats = Dict1,labels = orddict:new(), reg_name = ServerName,added = 0,deleted = 0,sum_delay = 0,highest_delay = 0}}.



handle_call({trigger},_From, State=#state{added = Added,deleted = Deleted,sum_delay = Delay,highest_delay = Max_Delay}) ->
    Delay_Per_Op=Delay div Deleted,
    lager:info("added count is ~p ~n",[Added]),
    {reply,ok,State};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.


handle_cast({add_label,_Label},State=#state{labels = _Labels,heartbeats = _Heartbeats,deleted = _Deleted,sum_delay = _Sum_Delay,highest_delay = _Max_Delay,added=Added})->
    %lager:info("Label ~p and heartbeat is ~p",[orddict:fetch(Label_Timestamp,Labels1),dict:fetch(Partition,Heartbeats1)]),
    {noreply,State#state{added=Added+1};

handle_cast({partition_heartbeat,_Clock,_Partition},State=#state{labels = _Labels,heartbeats = _Heartbeats,deleted = _Deleted,sum_delay = _Sum_Delay,highest_delay = _Max_Delay})->
    {noreply,State};

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
