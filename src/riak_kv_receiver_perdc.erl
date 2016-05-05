%%%-------------------------------------------------------------------
%%% @author chathuri
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 26. Apr 2016 15:54
%%%-------------------------------------------------------------------
-module(riak_kv_receiver_perdc).
-author("chathuri").

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

-export([get_receivers/1,assign_convergers/0]).

-define(SERVER, ?MODULE).
-include("riak_kv_causal_service.hrl").

-record(state, {my_propagators,my_dc_id,total_dcs,scattered_receivers,reg_name}).

start_link() ->
  DC_Id=app_helper:get_env(riak_kv,ordering_service_my_dc_id),
  ServerName=string:concat(?RECEIVER_PER_DC_PREFIX,integer_to_list(DC_Id)),
  lager:info("receiver per dc started  ~p",[ServerName]),
  gen_server:start_link({global, ServerName}, ?MODULE, [ServerName], []).

get_receivers(GlobalReceiverName) ->
  gen_server:call({global,GlobalReceiverName}, get_receivers, infinity).

assign_convergers() ->
  DC_Id=app_helper:get_env(riak_kv,ordering_service_my_dc_id),
  ServerName=string:concat(?RECEIVER_PER_DC_PREFIX,integer_to_list(DC_Id)),
  gen_server:call({global, ServerName}, assign_convergers, infinity).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([ServerName]) ->
  Cluster_Ips=app_helper:get_env(riak_kv,myip),
  List_Ips= string:tokens(Cluster_Ips, ","),
  connect_kernal(List_Ips),

  My_DC_Id=app_helper:get_env(riak_kv,ordering_service_my_dc_id),
  Total_Dcs=app_helper:get_env(riak_kv,ordering_service_total_dcs),
  {ok, Ring} = riak_core_ring_manager:get_my_ring(),
  GrossPrefLists = riak_core_ring:all_preflists(Ring, 1),
  ZeroPreflist = lists:foldl(fun(PrefList, Acc) ->
    case hd(PrefList) of
      {0, _Node} ->
        hd(PrefList);
      {_OtherPartition, _Node} ->
        Acc
    end
                             end, not_found, GrossPrefLists),
  Nodes = riak_core_ring:all_members(Ring),
  Propagators =  [list_to_atom(atom_to_list(Node) ++ atom_to_list(?RECEIVER_PER_NODE)) || Node <- Nodes],
  lager:info("PROPAGATORS SEEN BY RECEIVER PER DC ARE ~p ~n",[Propagators]),
  case ZeroPreflist of
    not_found ->
      lager:info("********Zero preflist not found***************", []);
    _ ->
      lists:foreach(fun(Name) ->
        ok = riak_kv_data_propagator:set_zeropl(Name, ZeroPreflist)
                    end, Propagators)
  end,
  lager:info("zero pref list is ~p ~n",[ZeroPreflist]),
  {ok, #state{my_propagators  = Propagators,my_dc_id = My_DC_Id,total_dcs = Total_Dcs,reg_name = ServerName}}.

%assign all receivers grouped by dcid to the vnodes
handle_call(assign_convergers, _From, State=#state{my_dc_id=MyId,total_dcs = Total_Dcs,my_propagators = _Propagators_On_Me}) ->
  Dict=dict:new(),
  %Dict2=dict:store(MyId,Propagators_On_Me,Dict),  %no need of its own, if we dont have replication
  Dict3=get_receivers_from_others(Total_Dcs,MyId,Dict),
  {ok, Ring} = riak_core_ring_manager:get_my_ring(),
  GrossPrefLists = riak_core_ring:all_preflists(Ring, 1),

  %send scattered list to all members in my ring
  lists:foreach(fun(PrefList) ->
      ok = riak_kv_vnode:set_receivers(hd(PrefList), Dict3),
           riak_kv_vnode:send_heartbeat(hd(PrefList)),
           riak_kv_vnode:compute_gst(hd(PrefList))
                end, GrossPrefLists),

  {reply,ok,State#state{scattered_receivers = Dict3}};

handle_call(get_receivers, _From, S0=#state{my_propagators = My_Propagators}) ->
  {reply, {ok, My_Propagators}, S0};


handle_call(_Request, _From, State) ->
  {reply, ok, State}.

handle_cast(_Request, State) ->
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

get_receivers_from_others(0,_MyId,Dict)->Dict;

get_receivers_from_others(Dc_Id,MyId,Dict)->
  case Dc_Id of
    MyId ->  get_receivers_from_others(Dc_Id-1,MyId,Dict);
    _    ->  GlobalReceiverName=string:concat(?RECEIVER_PER_DC_PREFIX,integer_to_list(Dc_Id)),
             {ok,Receivers}=get_receivers(GlobalReceiverName),
             Dict1=dict:store(Dc_Id,Receivers,Dict),
             get_receivers_from_others(Dc_Id-1,MyId,Dict1)

  end.


connect_kernal([])->
  ok;

connect_kernal([Node|Rest])->
  Status=net_kernel:connect_node(list_to_atom(Node)),
  lager:info("connect kernal status ~p ~n",[Status]),
  connect_kernal(Rest).