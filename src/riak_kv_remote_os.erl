%%%-------------------------------------------------------------------
%%% @author chathuri
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 13. Feb 2016 15:56
%%%-------------------------------------------------------------------
-module(riak_kv_remote_os).
-author("chathuri").

-behaviour(gen_server).

%% API
-export([start_link/0,remote_label_applied_by_vnode/1,deliver_to_remote_dcs/3]).

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
-record(state, {reg_name,my_dc_id,my_logical_clock,
    remote_vector,unsatisfied_queues,waiting_for_vnode_reply,remote_applied}).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    My_DC_Id=app_helper:get_env(riak_kv,ordering_service_my_dc_id),
    ServerName=string:concat(?REMOTE_ORD_SERVER_PREFIX,integer_to_list(My_DC_Id)),
    gen_server:start_link({global, ServerName}, ?MODULE, [ServerName], []).

deliver_to_remote_dcs(_ReversedBatch_To_Deliver,_My_Dc_Id,[])->
  noop;

%send all deliverable labels to remote dc stabilizers
deliver_to_remote_dcs(ReversedBatch_To_Deliver,My_Dc_Id,[Head|Rest])->
  gen_server:cast({global,Head},{remote_labels,ReversedBatch_To_Deliver,My_Dc_Id}),
  deliver_to_remote_dcs(ReversedBatch_To_Deliver,My_Dc_Id,Rest).

remote_label_applied_by_vnode(Causal_Service_Id)->
   gen_server:cast({global,Causal_Service_Id},{vnode_applied}).

%to print status when we need
print_status()->
  My_DC_Id=app_helper:get_env(riak_kv,ordering_service_my_dc_id),
  ServerName=string:concat(?REMOTE_ORD_SERVER_PREFIX,integer_to_list(My_DC_Id)),
  gen_server:call({global,ServerName}, {trigger}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%server name to be sent to other processes, check whether to read from proplist
init([ServerName]) ->
    Cluster_Ips=app_helper:get_env(riak_kv,myip),
    List_Ips= string:tokens(Cluster_Ips, ","),
    connect_kernal(List_Ips), %need to connect manually, otherwise gen_server msgs not receive outside cluster

    lager:info("ordering service to receiver remote labels started ~p ~n"),

    My_DC_Id=app_helper:get_env(riak_kv,ordering_service_my_dc_id),
    ServerName=string:concat(?REMOTE_ORD_SERVER_PREFIX,integer_to_list(My_DC_Id)),

    Remote_Dc_Count=app_helper:get_env(riak_kv,ordering_service_total_dcs),
     Remote_Vector=getInitVector(Remote_Dc_Count,My_DC_Id,dict:new()),%indicates the updates received from remote dcs
     Pending_Queues=get_initial_pending_queues(Remote_Dc_Count,My_DC_Id,dict:new()),
    %erlang:send_after(10000, self(), print_stats),
    {ok, #state{reg_name = ServerName,my_dc_id = My_DC_Id,remote_vector = Remote_Vector,my_logical_clock = 0,
      unsatisfied_queues =Pending_Queues,waiting_for_vnode_reply = false,remote_applied = 0}}.

handle_call({trigger}, _From, State=#state{remote_applied = Remote_Applied}) ->
  lager:info("*********remote applied label count is ~p *******",[Remote_Applied]),
  {reply, ok, State};

handle_call(_Request, _From, State) ->
     lager:info("Unexpected message received at hanlde_call"),
    {reply, ok, State}.

%check whether anything is deliverable in list
handle_cast({vnode_applied},State=#state{my_dc_id = My_Id,remote_vector = My_Vclock,unsatisfied_queues = Pendings,remote_applied = Remote_Applied})->
  {Pendings1,My_Vclock1,ShouldWait}=process_unsatisfied_remote_labels(My_Vclock,My_Id,Pendings),
{noreply,State#state{remote_vector = My_Vclock1,unsatisfied_queues = Pendings1,waiting_for_vnode_reply = ShouldWait,remote_applied = Remote_Applied+1}};

handle_cast({remote_labels,Batch_To_Deliver,Sender_Dc_Id},State=#state{remote_vector = My_VClock, unsatisfied_queues = Pendings,my_dc_id = My_Id,waiting_for_vnode_reply = ShouldWait})->
      {Head, Tail, PendingOps_Table} = dict:fetch(Sender_Dc_Id, Pendings),
      Tail1=insert_remote_labels_to_queue(Batch_To_Deliver,Tail,PendingOps_Table),
      Pendings1 = dict:store(Sender_Dc_Id, {Head, Tail1, PendingOps_Table}, Pendings),



      {Pendings2,My_Vclock1,ShouldWait1} =  case ShouldWait of
                                          false ->process_unsatisfied_remote_labels(My_VClock,My_Id,Pendings1);
                                            _   ->{Pendings1,My_VClock,ShouldWait}
                                       end,

    {noreply,State#state{remote_vector = My_Vclock1,unsatisfied_queues = Pendings2,waiting_for_vnode_reply = ShouldWait1}};

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

getInitVector(Total_DC_Count,My_Id,Dict)->
    case Total_DC_Count of
         0->Dict;
      My_Id -> getInitVector(Total_DC_Count-1,My_Id,Dict);
         _-> Dict1=dict:store(Total_DC_Count,0,Dict),
            getInitVector(Total_DC_Count-1,My_Id,Dict1)
    end.

get_initial_pending_queues(Total_DC_Count,My_DC_Id,Dict)->
  case Total_DC_Count of
    0->Dict;
    My_DC_Id-> get_initial_pending_queues(Total_DC_Count-1,My_DC_Id,Dict);
    _->
      Name = list_to_atom(integer_to_list(Total_DC_Count) ++  atom_to_list(unsatisfied_labels)),
      Table = ets:new(Name, [set, named_table, private]),
      Dict1=dict:store(Total_DC_Count, {0, 0, Table}, Dict),
      get_initial_pending_queues(Total_DC_Count-1,My_DC_Id,Dict1)
  end.

%%%===================================================================
%%% Remote Label specific functions
%%%===================================================================
insert_remote_labels_to_queue([],Tail,_PendingOps)->
  Tail;

insert_remote_labels_to_queue([Head_L|Rest_L],Tail,PendingOps)->
  ets:insert(PendingOps, {Tail, Head_L}),
  insert_remote_labels_to_queue(Rest_L,Tail+1,PendingOps).

deliver_head_label_to_vnode(Label,My_VClock,Sender_Dc_Id,My_Id)->
    Received_VClock=Label#label.vector,
    Received_Remote_Vclock=dict:erase(My_Id,Received_VClock),
    IsDeliverable=riak_kv_vclock:is_label_deliverable(My_VClock,Received_Remote_Vclock,Sender_Dc_Id),

    case IsDeliverable of
        true->
              Max_VClock=riak_kv_vclock:get_max_vector(My_VClock,Received_Remote_Vclock),
               %update the clock of the label
              Label1=Label#label{vector = Max_VClock},
              DocIdx = riak_core_util:chash_key(Label#label.bkey),
              PrefList = riak_core_apl:get_primary_apl(DocIdx, 1,riak_kv),
              [{IndexNode, _Type}] = PrefList,
              riak_kv_vnode:deliver_stable_label(Label1,Sender_Dc_Id,IndexNode),
              {Max_VClock,true };

            _ ->{My_VClock,false}   %labels from receiver are in order, if first is not deliverable, then we cant deliver all the rest
    end.

process_unsatisfied_remote_labels(My_Vclock,My_Id,Pending_Queues)->
   Dc_List=dict:fetch_keys(Pending_Queues),
   apply_possible_labels(Dc_List,Pending_Queues,My_Vclock,My_Id).

apply_possible_labels([],Pending_Queues,My_Vclock,_My_Id)->
  {Pending_Queues,My_Vclock,false};

apply_possible_labels([Head_DC|Rest],Pending_Queues,My_Vclock,My_Id)->
  {Head, Tail, PendingOps_Table}= dict:fetch(Head_DC, Pending_Queues),

  case ets:lookup(PendingOps_Table, Head) of
              [{Head,Label}]->{Clock,IsDeliverable}= deliver_head_label_to_vnode(Label,My_Vclock,Head_DC,My_Id),
                case IsDeliverable of
                  true  ->true = ets:delete(PendingOps_Table, Head),
                    Pending_Queues1 = dict:store(Head_DC, {Head+1, Tail, PendingOps_Table}, Pending_Queues),
                    {Pending_Queues1,Clock,true};
                  _ -> apply_possible_labels(Rest,Pending_Queues,My_Vclock,My_Id)
                end;
              _ ->apply_possible_labels(Rest,Pending_Queues,My_Vclock,My_Id)
   end.

