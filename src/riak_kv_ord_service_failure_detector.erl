%%%-------------------------------------------------------------------
%%% @author chathuri
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 24. Mar 2016 12:33
%%%-------------------------------------------------------------------
-module(riak_kv_ord_service_failure_detector).
-author("chathuri").

-include("riak_kv_causal_service.hrl").

-behaviour(gen_server).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-export([test/0,send_stable_ts_to_replicas/2,stop/0]).

-define(SERVER, ?MODULE).

-record(state, {other_replica_list=[],heartbeat_interval,my_reg_name,my_id,retry_count,original_replica_count,primary,non_primary_votes}).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

stop()->
gen_server:call(?MODULE, stop).

test()->
    gen_server:cast(?MODULE,test).

send_stable_ts_to_replicas(Stable_TS,MyName)->
    gen_server:cast(?MODULE, {stable_ts, Stable_TS, MyName}).

%only the non-primary nodes perform the heartbeats. if primary fails due to network failure, we expect
%other nodes to tell him it is not primary and will move to non primary eventually
init([]) ->
    Total_Replicas=app_helper:get_env(riak_kv, ord_service_replicas),
    Retry_Count=app_helper:get_env(riak_kv, retry_count),
    MyId=app_helper:get_env(riak_kv, myid),
    Ord_Service_Name=string:concat(?ORD_SERVICE_PREFIX,integer_to_list(MyId)),
    Heartbeat_Interval=app_helper:get_env(riak_kv, ord_service_fd_hb_interval),
    Other_Replica_List=generate_list(Total_Replicas,[],MyId),
    Other_Replica_Count=Total_Replicas-1,
    Primary=get_primary([Ord_Service_Name|Other_Replica_List]),

    case is_primary(Ord_Service_Name,Primary,Other_Replica_Count,Other_Replica_Count) of
        true->riak_kv_ord_service_ets_ordered:notify_primary(Ord_Service_Name);
           _-> erlang:send_after(Heartbeat_Interval, self(), heartbeats)
    end,
    lager:info("other replica list is ~p ~n",[Other_Replica_List]),
    {ok, #state{other_replica_list = Other_Replica_List,heartbeat_interval = Heartbeat_Interval,my_reg_name =Ord_Service_Name,my_id = MyId,original_replica_count  = Other_Replica_Count,retry_count = Retry_Count,primary = Primary,non_primary_votes = 0 }}.

handle_call(stop, _From, State) ->
    lager:info("failure detector stopping"),
    {stop, normal, shutdown_ok, State};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(test, State) ->
    lager:info("test message received by the failure detector ~n"),
    {noreply, State};

%send the stable ts to all other replicas
handle_cast({stable_ts,Stable_TS,Primary_Name}, State=#state{other_replica_list = Other_Replica_List}) ->
    notify_stable_to_other_replicas(Other_Replica_List,Stable_TS,Primary_Name),
    {noreply, State}.

handle_info(heartbeats, State=#state{heartbeat_interval = Heartbeat_Interval,other_replica_list = Other_Replicas,my_reg_name = My_Reg_Name,original_replica_count  = Original_Replica_Count,retry_count = Retry_count,primary = Primary}) ->
    Other_Replicas1=send_heartbeats(Other_Replicas,Other_Replicas,My_Reg_Name,Retry_count),
    New_Replica_Count=length(Other_Replicas1),

    %if the membership has changed within a hb interval, check whether u are the primary and notify ordering service if so
    case has_membership_changed(length(Other_Replicas),New_Replica_Count) of
        true-> Primary1=get_primary([My_Reg_Name|Other_Replicas1]),
               IsPrimary=is_primary(My_Reg_Name,Primary1,Original_Replica_Count,New_Replica_Count),
               case IsPrimary of
                    true->riak_kv_ord_service_ets_ordered:notify_primary(My_Reg_Name);%primary does not maintain heartbeats,crashed server rejoins with high id
                       _-> erlang:send_after(Heartbeat_Interval, self(), heartbeats)
                end;
           _-> Primary1=Primary,
               erlang:send_after(Heartbeat_Interval, self(), heartbeats)
    end,
    {noreply, State#state{other_replica_list = Other_Replicas1,primary = Primary1}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
generate_list(0,List,_MyId)->List;

generate_list(Ordering_Service_Node,List,MyId)->
    if
        MyId=:=Ordering_Service_Node -> generate_list(Ordering_Service_Node-1,List,MyId);
        true ->  Service_Name= string:concat(?ORD_SERVICE_PREFIX,integer_to_list(Ordering_Service_Node)),
                 NewList=[Service_Name|List],
                 generate_list(Ordering_Service_Node-1,NewList,MyId)
    end.



has_membership_changed(OldReplicaCount,NewReplicaCount)->
    case NewReplicaCount<OldReplicaCount of
        true ->true;
        _    ->false
    end.


send_heartbeats([],HB_List,_MyRegName,_Retry_count)->
  HB_List;


send_heartbeats([Head|Rest],HB_List,MyRegName,Retry_count)->
    Result=riak_kv_ord_service_ets_ordered:check_node_up(Head),
    case Result of
          ok->send_heartbeats(Rest,HB_List,MyRegName,Retry_count);
          timeout->lager:info("ordering service ~p is not responding ~n",[Head]),
                   New_HB_List=repeat_heartbeats(Head,HB_List,Retry_count-1),
                   send_heartbeats(Rest,New_HB_List,MyRegName,Retry_count)
    end.


repeat_heartbeats(Head,HB_List,Retry_count)->
    case Retry_count of
        0->    lists:delete(Head,HB_List);
        _-> Result=riak_kv_ord_service_ets_ordered:check_node_up(Head),
            case Result of
                ok->HB_List;
                timeout->lager:info("ordering service ~p is not responding ~n",[Head]),
                         repeat_heartbeats(Head,HB_List,Retry_count-1)
            end
    end.

get_primary(ListReplicas)->[Head|_Rest]=lists:sort(ListReplicas),
                           Head.

%to become the primary, we need to have majority of other replicas
is_primary(My_Ord_Reg_name,Primary,_Original_Other_Replica_Count,_Remaining_Replica_Count)->
   string:equal(My_Ord_Reg_name,Primary).
   % case Original_Other_Replica_Count of
      %  1->string:equal(My_Ord_Reg_name,Primary);
       % _->case (Remaining_Replica_Count+1)>(Original_Other_Replica_Count/2) of
        %       true->string:equal(My_Ord_Reg_name,Primary);
        %       _->    false
        %   end
  %  end.

notify_stable_to_other_replicas([],_Stable_TS,_Primary_Name)->noop;

%todo: based on NACKs, we can quickly avoid problem of running 2 primaries,and rejoin the old primary with higher id
notify_stable_to_other_replicas([Head|Rest],Stable_TS,Primary_Name)->
    riak_kv_ord_service_ets_ordered:notify_stable_ts(Head,Stable_TS,Primary_Name),
    notify_stable_to_other_replicas(Rest,Stable_TS,Primary_Name).
