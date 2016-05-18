%%%-------------------------------------------------------------------
%%% @author chathuri
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 26. Apr 2016 15:55
%%%-------------------------------------------------------------------
-module(riak_kv_data_propagator).
-author("chathuri").

-behaviour(gen_server).

%% API
-export([start_link/0,propagate_data/6]).

%% gen_server callbacks
-export([init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3]).

-define(SERVER, ?MODULE).
-include("riak_kv_causal_service.hrl").

-record(state, {reg_name}).

start_link() ->
  Server = list_to_atom(atom_to_list(node()) ++ atom_to_list(?RECEIVER_PER_NODE)),
  lager:info("************the propagator started and name is ~p *********",[Server]),
  gen_server:start_link({global, Server}, ?MODULE, [Server], []).

propagate_data(BKey,Object,Options,My_Dc_Id,Timestamp,RemoteReceiverName)->
  gen_server:cast({global, RemoteReceiverName},{remote_data,BKey,Object,Options,My_Dc_Id,Timestamp} ).

init([Server]) ->
  Cluster_Ips=app_helper:get_env(riak_kv,myip),
  List_Ips= string:tokens(Cluster_Ips, ","),
  connect_kernal(List_Ips),
  {ok, #state{reg_name = Server}}.


handle_call(_Request, _From, State) ->
  {reply, ok, State}.

%rely on similar ft logic as we implemented
handle_cast({remote_data,BKey,Object,Options,Sender_Dc_Id,Timestamp}, State) ->
  DocIdx = riak_core_util:chash_key(BKey),
  PrefList = riak_core_apl:get_primary_apl(DocIdx, 1,riak_kv),
  [{IndexNode, _Type}] = PrefList,
  riak_kv_vnode:propagate(IndexNode, BKey, Object,Options,Sender_Dc_Id,Timestamp),
  {noreply,State};

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

%connect epmd; otherwise we may not be able to use gen_server calls to this node
connect_kernal([])->
  ok;

connect_kernal([Node|Rest])->
  Status=net_kernel:connect_node(list_to_atom(Node)),
  lager:info("connect kernal status ~p ~n",[Status]),
  connect_kernal(Rest).