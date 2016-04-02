%%%-------------------------------------------------------------------
%%% @author chathuri
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 02. Mar 2016 12:40
%%%-------------------------------------------------------------------
-module(riak_kv_ord_service_ets_ordered).
-author("chathuri").

-include_lib("stdlib/include/ms_transform.hrl").

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

-export([add_label/3,test/0,partition_heartbeat/2,print_status/0]).

-define(SERVER, ?MODULE).

-record(state, {reg_name,added,ignore,is_first_label}).

test()->
    Status=net_kernel:connect_node('riak@127.0.0.1'), %this is the node where we run global server
    global:sync(),
    io:format("calling test ~p ~n",[Status]),
    case catch gen_server:call({global,riak_kv_ord_service_ets_ordered},{test}) of
        {'EXIT', ErrorExit} -> io:fwrite("ErrorExit ~p~n",[ErrorExit]),lager:info("error is ~p ~n",[ErrorExit]);
        {_, _}  ->lager:info("another error occured  ~n");
        ok->lager:info("this is working")
    end.


add_label(Label,Client_Id,MaxTS)->
    %lager:info("label is ready to add to the ordeing service  ~p",[Label]),
    gen_server:cast({global,riak_kv_ord_service_ets_ordered},{add_label,Label,Client_Id,MaxTS}).

partition_heartbeat(Partition,Clock)->
    gen_server:cast({global,riak_kv_ord_service_ets_ordered},{partition_heartbeat,Clock,Partition}).

%to print status when we need
print_status()->
    gen_server:call({global,riak_kv_ord_service_ets_ordered}, {trigger}).


start_link() ->
    gen_server:start_link({global,riak_kv_ord_service_ets_ordered}, ?MODULE, [riak_kv_ord_service_ets_ordered], []).

init([ServerName]) ->
    %lager:info("ordering service started"),
    %{X,Y} =erlang:process_info(global:whereis_name(riak_kv_ord_service_ets_ordered), memory),
    %lager:info("ordering service started ~p ~p ~n",[X,Y]),
    %process_flag(min_heap_size, 100000),
    %memsup:set_procmem_high_watermark(0.6),
    %{P,Q} =erlang:process_info(global:whereis_name(riak_kv_ord_service_ets_ordered), memory),
    %lager:info("after memory is ~p ~p ~n",[P,Q]),
    ClientCount=app_helper:get_env(riak_kv, clients),
    lager:info("client_count is ~p ~n",[ClientCount]),
    Batch_Delivery_Size= app_helper:get_env(riak_kv,receiver_batch_size),
    lager:info("batch delivery size is ~p ~n",[Batch_Delivery_Size]),
    ets:new(?Label_Table_Name, [ordered_set, named_table,public]),
    ets:new(?HB_TABLE_NAME, [set, named_table,public,{read_concurrency, true},{write_concurrency, true}]),
    initialize_hb_table(ClientCount),
    erlang:send_after(10000, self(), print_stats),
    {ok, #state{ reg_name = ServerName,added = 0,ignore=true,is_first_label = true}}.

handle_call({trigger},_From, State=#state{added = Added}) ->
    lager:info("added count is ~p ~n",[Added]),
    {reply,ok,State};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.


handle_cast({add_label,BatchedLabels,Partition,MaxTS},State=#state{added = Added,ignore=Should_Ignore,is_first_label = Is_First})->
    % lager:info("received label from ~p ~n",[Partition]),
    State1=case Is_First of
             true->erlang:send_after(10000, self(), disable_ignore),State#state{is_first_label = false};
              _->State
           end,

    State2=case Should_Ignore of
                true->State1;
                   _-> Added1=insert_batch_labels(BatchedLabels,Partition,Added),
                       update_hb_table(Partition,MaxTS),
                       %todo: test functionality of only send heartbeats when no label has sent fix @ vnode
                       State#state{added = Added1}
             end,
    {noreply,State2};


handle_cast(_Request, State) ->
    lager:error("received an unexpected  message ~n"),
    {noreply, State}.

handle_info(disable_ignore, State) ->
    {noreply, State#state{ignore = false}};%stabilised to receive labels


handle_info(print_stats, State=#state{added = Added}) ->
    {_,{Hour,Min,Sec}} = erlang:localtime(),
    lager:info("timestamp ~p: ~p: ~p: added ~p ~n",[Hour,Min,Sec,Added]),
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

initialize_hb_table(ClientId)->
    if
        ClientId>0  -> ets:insert(?HB_TABLE_NAME,{ClientId,0}),initialize_hb_table(ClientId-1) ;
        true -> noop
    end.

update_hb_table(Partition,MaxTS)->
    % todo: check why  ets:update_element(?HB_TABLE_NAME, Partition,{Partition,MaxTS}) not working
    ets:insert(?HB_TABLE_NAME,{Partition,MaxTS}).

insert_batch_labels([],_Partition,Added)->Added;

insert_batch_labels([Head|Rest],Partition,Added)->
    Label_Timestamp=Head#label.timestamp,
    ets:insert(?Label_Table_Name,{{Label_Timestamp,Partition,Head},dt}),
    insert_batch_labels(Rest,Partition,Added+1).

