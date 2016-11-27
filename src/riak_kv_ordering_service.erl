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
-export([start_link/0,partition_heartbeat/3, add_label/3]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3,
    print_status/0
]).


-include("riak_kv_causal_service.hrl").
-define(SERVER, ?MODULE).

%delay- average delay between receiving and delivering a label
-record(state, {reg_name,remote_dc_list,my_dc_id,my_logical_clock,labels}).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    My_DC_Id=app_helper:get_env(riak_kv,ordering_service_my_dc_id),
    ServerName=string:concat(?STABILIZER_PREFIX,integer_to_list(My_DC_Id)),
    gen_server:start_link({global, ServerName}, ?MODULE, [ServerName], []).

add_label(Label,Causal_Service_Id,Partition)->
    %lager:info("label is ready to add to the ordeing service  ~p",[Label]),
    gen_server:call({global,Causal_Service_Id},{add_label,Label,Partition}).

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
    Cluster_Ips=app_helper:get_env(riak_kv,myip),
    List_Ips= string:tokens(Cluster_Ips, ","),
    connect_kernal(List_Ips), %need to connect manually, otherwise gen_server msgs not receive outside cluster

    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Num_Partitions=riak_core_ring:num_partitions(Ring),
    lager:info("total partitions are ~p and server name is ~p",[Num_Partitions,ServerName]),

    My_DC_Id=app_helper:get_env(riak_kv,ordering_service_my_dc_id),
    ServerName=string:concat(?STABILIZER_PREFIX,integer_to_list(My_DC_Id)),

    Remote_Dc_Count=app_helper:get_env(riak_kv,ordering_service_total_dcs),
    Remote_Dc_List=get_remote_dc_list(Remote_Dc_Count,My_DC_Id,?REMOTE_ORD_SERVER_PREFIX,[]),

    riak_kv_receiver_perdc:assign_convergers(), %ordering service initiate the converger logic

    erlang:send_after(10, self(), deliver_remote_labels),
    {ok, #state{ reg_name = ServerName,remote_dc_list = Remote_Dc_List,my_dc_id = My_DC_Id,my_logical_clock = 0,labels = []}}.

handle_info(deliver_remote_labels,State=#state{my_dc_id = My_Dc_Id,
    remote_dc_list = Remote_Dc_List,labels = Labels})->
    case Labels of
        [] ->
            noop;
        _  ->
            ReversedBatch_To_Deliver=lists:reverse(Labels), %as we append deleted to head, need to reverse to get in ascending order
            riak_kv_remote_os:deliver_to_remote_dcs (ReversedBatch_To_Deliver,My_Dc_Id,Remote_Dc_List)

    end,
    erlang:send_after(10, self(), deliver_remote_labels),
    {noreply, State#state{labels = []}}.

%no batching as in this case frequency is low
handle_call({add_label,Label,_Partition},_From,State=#state{my_logical_clock = My_Logical_Clock,labels = Labels,my_dc_id = My_Dc_Id})->
    My_Logical_Clock1 = My_Logical_Clock+1,
    VClock1=Label#label.vector,
    VClock2=dict:store(My_Dc_Id,My_Logical_Clock1,VClock1),
    Label1=Label#label{vector = VClock2,timestamp=My_Logical_Clock1},
    Batched_Labels_To_Deliver=[Label1|Labels],
    {reply,{ok,My_Logical_Clock1},State#state{my_logical_clock = My_Logical_Clock1, labels = Batched_Labels_To_Deliver}}.


handle_cast(_Request, State) ->
    lager:error("received an unexpected  message ~n"),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
get_remote_dc_list(Remote_Dc_Id,My_Dc_Id,ServerName_Prefix,Remote_DC_List)->
            case Remote_Dc_Id of
                       0-> Remote_DC_List;
                My_Dc_Id-> get_remote_dc_list(Remote_Dc_Id-1,My_Dc_Id,ServerName_Prefix,Remote_DC_List);
                       _-> ServerName=string:concat(ServerName_Prefix,integer_to_list(Remote_Dc_Id)),
                           get_remote_dc_list(Remote_Dc_Id-1,My_Dc_Id,ServerName_Prefix,[ServerName|Remote_DC_List])

            end.





connect_kernal([])->
    ok;

connect_kernal([Node|Rest])->
    Status=net_kernel:connect_node(list_to_atom(Node)),
    lager:info("connect kernal status ~p ~n",[Status]),
    connect_kernal(Rest).
