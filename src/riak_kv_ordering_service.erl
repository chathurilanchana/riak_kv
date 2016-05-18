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
-export([start_link/0,partition_heartbeat/3, add_labels/4,remote_label_applied_by_vnode/1]).

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
-define(Label_Table_Name, labels).

%delay- average delay between receiving and delivering a label
-record(state, {heartbeats,reg_name,remote_dc_list,my_dc_id,my_logical_clock,
    remote_vector,unsatisfied_queues,waiting_for_vnode_reply}).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    My_DC_Id=app_helper:get_env(riak_kv,ordering_service_my_dc_id),
    ServerName=string:concat(?STABILIZER_PREFIX,integer_to_list(My_DC_Id)),
    gen_server:start_link({global, ServerName}, ?MODULE, [ServerName], []).

add_labels(Label,Causal_Service_Id,Partition,Clock)->
    %lager:info("label is ready to add to the ordeing service  ~p",[Label]),
    gen_server:cast({global,Causal_Service_Id},{add_labels,Label,Partition,Clock}).

deliver_to_remote_dcs(_ReversedBatch_To_Deliver,_My_Dc_Id,[])->
    noop;

%send all deliverable labels to remote dc stabilizers
deliver_to_remote_dcs(ReversedBatch_To_Deliver,My_Dc_Id,[Head|Rest])->
    gen_server:cast({global,Head},{remote_labels,ReversedBatch_To_Deliver,My_Dc_Id}),
    deliver_to_remote_dcs(ReversedBatch_To_Deliver,My_Dc_Id,Rest).

partition_heartbeat(Partition,Clock,Causal_Service_Id)->
    gen_server:cast({global,Causal_Service_Id},{partition_heartbeat,Clock,Partition}).

remote_label_applied_by_vnode(Causal_Service_Id)->
   gen_server:cast({global,Causal_Service_Id},{vnode_applied}).

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

    GrossPrefLists = riak_core_ring:all_preflists(Ring, 1),
    Dict1 = lists:foldl(fun(PrefList, Dict) ->
        {Partition, _Node} = hd(PrefList),
        riak_kv_vnode:heartbeat(PrefList),
        dict:store(Partition, 0, Dict)
                       end, dict:new(), GrossPrefLists),
    lager:info("dictionary size is ~p ~n",[dict:size(Dict1)]),

    My_DC_Id=app_helper:get_env(riak_kv,ordering_service_my_dc_id),
    ServerName=string:concat(?STABILIZER_PREFIX,integer_to_list(My_DC_Id)),

    Remote_Dc_Count=app_helper:get_env(riak_kv,ordering_service_total_dcs),
    Remote_Dc_List=get_remote_dc_list(Remote_Dc_Count,My_DC_Id,?STABILIZER_PREFIX,[]),
    lager:info("reomte dc list is ~p",[Remote_Dc_List]),
     Remote_Vector=getInitVector(Remote_Dc_Count,My_DC_Id,dict:new()),%indicates the updates received from remote dcs
     Pending_Queues=get_initial_pending_queues(Remote_Dc_Count,My_DC_Id,dict:new()),

    riak_kv_receiver_perdc:assign_convergers(), %ordering service initiate the converger logic

    ets:new(?Label_Table_Name, [ordered_set, named_table,private]),
    %erlang:send_after(10000, self(), print_stats),
    {ok, #state{heartbeats = Dict1, reg_name = ServerName,remote_dc_list = Remote_Dc_List
      ,my_dc_id = My_DC_Id,remote_vector = Remote_Vector,my_logical_clock = 0,
      unsatisfied_queues =Pending_Queues,waiting_for_vnode_reply = false}}.


handle_call(_Request, _From, State) ->
     lager:info("Unexpected message received at hanlde_call"),
    {reply, ok, State}.


%no batching as in this case frequency is low
handle_cast({add_labels,Labels,Partition,Clock},State=#state{heartbeats = Heartbeats,my_dc_id = My_Dc_Id,
    remote_dc_list = Remote_Dc_List,my_logical_clock = My_Logical_Clock})->
    Batched_Labels_To_Insert=batched_labels_to_insert(Labels,Partition,[]),
    insert_batch_to_ets_table(Batched_Labels_To_Insert),
    Heartbeats1= dict:store(Partition,Clock,Heartbeats),
    {Batch_To_Deliver,My_Logical_Clock1}= get_possible_deliveries(Heartbeats1,My_Logical_Clock,My_Dc_Id),

    case Batch_To_Deliver of
        [] ->
              noop;
        _  ->
              ReversedBatch_To_Deliver=lists:reverse(Batch_To_Deliver), %as we append deleted to head, need to reverse to get in ascending order
             deliver_to_remote_dcs(ReversedBatch_To_Deliver,My_Dc_Id,Remote_Dc_List)
    end,
    State1=State#state{heartbeats = Heartbeats1,my_logical_clock = My_Logical_Clock1},
    {noreply,State1};

handle_cast({partition_heartbeat,Clock,Partition},State=#state{heartbeats = Heartbeats,my_dc_id = My_Dc_Id,remote_dc_list = Remote_Dc_List
    ,my_logical_clock = My_Logical_Clock})->

    Heartbeats1=dict:store(Partition,Clock,Heartbeats),
    {Batch_To_Deliver,My_Logical_Clock1}= get_possible_deliveries(Heartbeats1,My_Logical_Clock,My_Dc_Id),

    case Batch_To_Deliver of
        [] ->noop;
        _  -> ReversedBatch_To_Deliver=lists:reverse(Batch_To_Deliver),
             deliver_to_remote_dcs(ReversedBatch_To_Deliver,My_Dc_Id,Remote_Dc_List)
    end,
    State1=State#state{heartbeats = Heartbeats1,my_logical_clock = My_Logical_Clock1},
    {noreply,State1};

%check whether anything is deliverable in list
handle_cast({vnode_applied},State=#state{my_dc_id = My_Id,remote_vector = My_Vclock,unsatisfied_queues = Pendings})->
  lager:info("*******vnode reply received ~n******"),
  {Pendings1,My_Vclock1,ShouldWait}=process_unsatisfied_remote_labels(My_Vclock,My_Id,Pendings),
{noreply,State#state{remote_vector = My_Vclock1,unsatisfied_queues = Pendings1,waiting_for_vnode_reply = ShouldWait}};

%#dcs>3, otherwise we dont need any blocking.
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
get_remote_dc_list(Remote_Dc_Id,My_Dc_Id,ServerName_Prefix,Remote_DC_List)->
            case Remote_Dc_Id of
                       0-> Remote_DC_List;
                My_Dc_Id-> get_remote_dc_list(Remote_Dc_Id-1,My_Dc_Id,ServerName_Prefix,Remote_DC_List);
                       _-> ServerName=string:concat(ServerName_Prefix,integer_to_list(Remote_Dc_Id)),
                           get_remote_dc_list(Remote_Dc_Id-1,My_Dc_Id,ServerName_Prefix,[ServerName|Remote_DC_List])

            end.


get_possible_deliveries(Heartbeats,My_Logical_Clock,My_Dc_Id)->
    Min_Stable_Timestamp=get_stable_timestamp(Heartbeats),
    deliverable_labels(Min_Stable_Timestamp,[],My_Logical_Clock,My_Dc_Id).

batched_labels_to_insert([],_Partition,Batch_To_Insert)->
  Batch_To_Insert;


batched_labels_to_insert([Head|Rest],Partition,Batch_To_Insert)->
  Label_Timestamp=Head#label.timestamp,
  batched_labels_to_insert(Rest,Partition,[{{Label_Timestamp,Partition,Head},dt}|Batch_To_Insert]).

insert_batch_to_ets_table(Batch_To_Insert)->
  ets:insert(?Label_Table_Name,Batch_To_Insert).

%insert_label(Label,Partition)->
   % Label_Timestamp=Label#label.timestamp,
   % ets:insert(?Label_Table_Name,{{Label_Timestamp,Partition,Label},dt}).

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


%for now batch_delivery size does not matter. If there are heavy deletes, do batchwise
deliverable_labels(Min_Stable_Timestamp,Batch_To_Deliver,My_Logical_Clock,My_Dc_Id)->
    case ets:first(?Label_Table_Name)  of
        '$end_of_table' -> {Batch_To_Deliver,My_Logical_Clock};
        {Timestamp,_Partition,Label}=Key when Timestamp=<Min_Stable_Timestamp ->
            ets:delete(?Label_Table_Name,Key),
             My_Logical_Clock1=My_Logical_Clock+1,
             VClock1=Label#label.vector,
             VClock2=dict:store(My_Dc_Id,My_Logical_Clock1,VClock1),
             Label1=Label#label{vector = VClock2},% before delivery, we have to pump this dc's logical clock
             deliverable_labels(Min_Stable_Timestamp,[Label1|Batch_To_Deliver],My_Logical_Clock1,My_Dc_Id);
        {_Timestamp,_Partition,_Label}->{Batch_To_Deliver,My_Logical_Clock}
    end.


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
              lager:info("there is something to deliver to vnode ~n"),
              Max_VClock=riak_kv_vclock:get_max_vector(My_VClock,Received_Remote_Vclock),
               %update the clock of the label
              Label1=Label#label{vector = Max_VClock},
              DocIdx = riak_core_util:chash_key(Label#label.bkey),
              PrefList = riak_core_apl:get_primary_apl(DocIdx, 1,riak_kv),
              [{IndexNode, _Type}] = PrefList,
              riak_kv_vnode:deliver_stable_label(Label1,Sender_Dc_Id,IndexNode,self()),

              %wait_for_response(),
              {Max_VClock,true };

            _ ->{My_VClock,false}   %labels from receiver are in order, if first is not deliverable, then we cant deliver all the rest
    end.

%wait_for_response()->
 %   receive
  %     ok ->ok
   %  after 5000 ->
    %    lager:info("*****TIMEOUT OCCURED AFTER SENDING A REMOTE LABEL TO A VNODE*********")
    %end.

process_unsatisfied_remote_labels(My_Vclock,My_Id,Pending_Queues)->
   Dc_List=dict:fetch_keys(Pending_Queues),
   apply_possible_labels(Dc_List,Pending_Queues,My_Vclock,My_Id).

apply_possible_labels([],Pending_Queues,My_Vclock,_My_Id)->
  {Pending_Queues,My_Vclock,false};

apply_possible_labels([Head_DC|Rest],Pending_Queues,My_Vclock,My_Id)->
  {Head, Tail, PendingOps_Table}= dict:fetch(Head_DC, Pending_Queues),

  case Head=:=Tail of
    true-> Pending_Queues1 = dict:store(Head_DC, {0, 0, PendingOps_Table}, Pending_Queues),
           {Pending_Queues1,My_Vclock,false};
      _ ->  case ets:lookup(PendingOps_Table, Head) of
              [{Head,Label}]->{Clock,IsDeliverable}= deliver_head_label_to_vnode(Label,My_Vclock,Head_DC,My_Id),
                case IsDeliverable of
                  true  ->true = ets:delete(PendingOps_Table, Head),
                    Pending_Queues1 = dict:store(Head_DC, {Head+1, Tail, PendingOps_Table}, Pending_Queues),
                    {Pending_Queues1,Clock,true};
                  _ -> apply_possible_labels(Rest,Pending_Queues,My_Vclock,My_Id)
                end;
              _ ->apply_possible_labels(Rest,Pending_Queues,My_Vclock,My_Id)
            end
   end.

