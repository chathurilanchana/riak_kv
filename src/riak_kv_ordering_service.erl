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
-export([start_link/0]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).


-include("riak_kv_causal_service.hrl").
-define(SERVER, ?MODULE).

%delay- average delay between receiving and delivering a label
-record(state, {reg_name,my_dc_id}).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    My_DC_Id=app_helper:get_env(riak_kv,ordering_service_my_dc_id),
    ServerName=string:concat(?STABILIZER_PREFIX,integer_to_list(My_DC_Id)),
    gen_server:start_link({global, ServerName}, ?MODULE, [ServerName], []).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%server name to be sent to other processes, check whether to read from proplist
init([ServerName]) ->
    Cluster_Ips=app_helper:get_env(riak_kv,myip),
    List_Ips= string:tokens(Cluster_Ips, ","),
    connect_kernal(List_Ips), %need to connect manually, otherwise gen_server msgs not receive outside cluster

    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Num_Partitions=riak_core_ring:num_partitions(Ring),
    lager:info("total partitions are ~p and server name is ~p",[Num_Partitions,ServerName]),

    My_DC_Id=app_helper:get_env(riak_kv,ordering_service_my_dc_id),
    ServerName=string:concat(?STABILIZER_PREFIX,integer_to_list(My_DC_Id)),

    riak_kv_receiver_perdc:assign_convergers(), %ordering service initiate the converger logic
    {ok, #state{reg_name = ServerName,my_dc_id = My_DC_Id}}.


handle_call(_Request, _From, State) ->
     lager:info("Unexpected message received at hanlde_call"),
    {reply, ok, State}.

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

connect_kernal([])->
    ok;

connect_kernal([Node|Rest])->
    Status=net_kernel:connect_node(list_to_atom(Node)),
    lager:info("connect kernal status ~p ~n",[Status]),
    connect_kernal(Rest).
