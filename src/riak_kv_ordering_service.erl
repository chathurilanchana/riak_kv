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
-export([start_link/0,partition_heartbeat/3,add_label/3]).

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
-record(state, {heartbeats,reg_name,added,deleted,remote_dc_list,my_dc_id,my_logical_clock,
    remote_vector,unsatisfied_remote_labels}).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    My_DC_Id=app_helper:get_env(riak_kv,ordering_service_my_dc_id),
    ServerName=string:concat(?STABILIZER_PREFIX,integer_to_list(My_DC_Id)),
    gen_server:start_link({global, ServerName}, ?MODULE, [ServerName], []).

add_label(Label,Causal_Service_Id,Partition)->
    %lager:info("label is ready to add to the ordeing service  ~p",[Label]),
    gen_server:cast({global,Causal_Service_Id},{add_label,Label,Partition}).

deliver_to_remote_dcs(_ReversedBatch_To_Deliver,_My_Dc_Id,[])->
    noop;

%send all deliverable labels to remote dc stabilizers
deliver_to_remote_dcs(ReversedBatch_To_Deliver,My_Dc_Id,[Head|Rest])->
    gen_server:cast({global,Head},{remote_labels,ReversedBatch_To_Deliver,My_Dc_Id}),
    deliver_to_remote_dcs(ReversedBatch_To_Deliver,My_Dc_Id,Rest).

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
    lager:info("reomte dc list is ~p ~n",[Remote_Dc_List]),
     Remote_Vector=getInitVector(Remote_Dc_Count,My_DC_Id,dict:new()),%indicates the updates received from remote dcs
    UnsatisfiedRemoteLabels= get_init_remote_Label_dictionary(dict:new(),Remote_Dc_Count,My_DC_Id),

    riak_kv_receiver_perdc:assign_convergers(), %ordering service initiate the converger logic

    ets:new(?Label_Table_Name, [ordered_set, named_table,private]),
    %erlang:send_after(10000, self(), print_stats),
    {ok, #state{heartbeats = Dict1, reg_name = ServerName,remote_dc_list = Remote_Dc_List,added = 0,
        deleted = 0,my_dc_id = My_DC_Id,remote_vector = Remote_Vector,my_logical_clock = 0,unsatisfied_remote_labels =UnsatisfiedRemoteLabels}}.


handle_call({trigger},_From, State=#state{added = Added,deleted = Deleted}) ->
    lager:info("added count is ~p deleted count is ~p ~n",[Added,Deleted]),
    {reply,ok,State};

handle_call(_Request, _From, State) ->
     lager:info("Unexpected message received at hanlde_call"),
    {reply, ok, State}.


%no batching as in this case frequency is low
handle_cast({add_label,Label,Partition},State=#state{heartbeats = Heartbeats,my_dc_id = My_Dc_Id,
    remote_dc_list = Remote_Dc_List,added = Added,deleted = Deleted,my_logical_clock = My_Logical_Clock})->

    insert_label(Label,Partition),
    Added1=Added+1,
    Heartbeats1= dict:store(Partition,Label#label.timestamp,Heartbeats),
    {Deleted1,Batch_To_Deliver,My_Logical_Clock1}= get_possible_deliveries(Heartbeats1,Deleted,My_Logical_Clock,My_Dc_Id),

    case Batch_To_Deliver of
        [] ->noop;
        _  ->ReversedBatch_To_Deliver=lists:reverse(Batch_To_Deliver), %as we append deleted to head, need to reverse to get in ascending order
             deliver_to_remote_dcs(ReversedBatch_To_Deliver,My_Dc_Id,Remote_Dc_List)
    end,
    State1=State#state{heartbeats = Heartbeats,added = Added1,deleted = Deleted1,my_logical_clock = My_Logical_Clock1},
    {noreply,State1};

handle_cast({partition_heartbeat,Clock,Partition},State=#state{heartbeats = Heartbeats,my_dc_id = My_Dc_Id,remote_dc_list = Remote_Dc_List,deleted = Deleted
    ,my_logical_clock = My_Logical_Clock})->

    Heartbeats1=dict:store(Partition,Clock,Heartbeats),
    {Deleted1,Batch_To_Deliver,My_Logical_Clock1}= get_possible_deliveries(Heartbeats1,Deleted,My_Logical_Clock,My_Dc_Id),

    case Batch_To_Deliver of
        [] ->noop;
        _  -> ReversedBatch_To_Deliver=lists:reverse(Batch_To_Deliver),
             deliver_to_remote_dcs(ReversedBatch_To_Deliver,My_Dc_Id,Remote_Dc_List)
    end,
    State1=State#state{heartbeats = Heartbeats1,my_logical_clock = My_Logical_Clock1,deleted = Deleted1},
    {noreply,State1};

%#dcs>3, otherwise we dont need any blocking.
handle_cast({remote_labels,Batch_To_Deliver,Sender_Dc_Id},State=#state{remote_vector = Remote_VClock, unsatisfied_remote_labels = Remote_LabelDict,my_dc_id = My_Id})->
     [Head|_]=Batch_To_Deliver,
     Remote_Received=dict:erase(My_Id,Head#label.vector),
     IsDeliverable=riak_kv_vclock:is_label_deliverable(Remote_VClock,Remote_Received,Sender_Dc_Id),

     %we only check this if head is deliverable from received batch
{Remote_LabelDict2,My_Vclock2}=
     case IsDeliverable of
         true->%lager:info("YEYYYYYYYYY RECEIVED SOMETHING DELIVERABLE"),
               {Remaining,My_Vclock1,_HasChanged}=do_possible_delivers_to_vnodes(Batch_To_Deliver,Remote_VClock,Sender_Dc_Id,My_Id,false),
               Remote_LabelDict1= case Remaining of
                                           []->Remote_LabelDict;
                                           Labels->Old_From_Receiver=dict:fetch(Sender_Dc_Id,Remote_LabelDict),
                                                   Labels1=Old_From_Receiver ++ Labels,
                                                   dict:store(Sender_Dc_Id,Labels1,Remote_LabelDict)
                                       end,

%after delivering received, we need to check whether we can delete any in the unsatisfied list
                process_unsatisfied_remote_labels(Remote_LabelDict1,My_Vclock1,My_Id);

%nothing to deliver, append received to the sender's list
            _-> Old_From_Receiver=dict:fetch(Sender_Dc_Id,Remote_LabelDict),
                Labels1=Old_From_Receiver ++ Batch_To_Deliver,
                {dict:store(Sender_Dc_Id,Labels1,Remote_LabelDict),Remote_VClock}
     end,
    {noreply,State#state{remote_vector = My_Vclock2,unsatisfied_remote_labels = Remote_LabelDict2}};

handle_cast(_Request, State) ->
    lager:error("received an unexpected  message ~n"),
    {noreply, State}.

handle_info(print_stats, State=#state{added = Added,deleted = Deleted}) ->
    {_,{Hour,Min,Sec}} = erlang:localtime(),
    lager:info("timestamp ~p: ~p: ~p: added ~p deleted ~p ~n",[Hour,Min,Sec,Added,Deleted]),

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
get_remote_dc_list(Remote_Dc_Id,My_Dc_Id,ServerName_Prefix,Remote_DC_List)->
            case Remote_Dc_Id of
                       0-> Remote_DC_List;
                My_Dc_Id-> get_remote_dc_list(Remote_Dc_Id-1,My_Dc_Id,ServerName_Prefix,Remote_DC_List);
                       _-> ServerName=string:concat(ServerName_Prefix,integer_to_list(Remote_Dc_Id)),
                           get_remote_dc_list(Remote_Dc_Id-1,My_Dc_Id,ServerName_Prefix,[ServerName|Remote_DC_List])

            end.


get_possible_deliveries(Heartbeats,Deleted,My_Logical_Clock,My_Dc_Id)->
    Min_Stable_Timestamp=get_stable_timestamp(Heartbeats),
    deliverable_labels(Min_Stable_Timestamp,Deleted,[],My_Logical_Clock,My_Dc_Id).

insert_label(Label,Partition)->
    Label_Timestamp=Label#label.timestamp,
    ets:insert(?Label_Table_Name,{{Label_Timestamp,Partition,Label},dt}).

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
deliverable_labels(Min_Stable_Timestamp,Deleted,Batch_To_Deliver,My_Logical_Clock,My_Dc_Id)->
    case ets:first(?Label_Table_Name)  of
        '$end_of_table' -> {Deleted,Batch_To_Deliver,My_Logical_Clock};
        {Timestamp,_Partition,Label}=Key when Timestamp=<Min_Stable_Timestamp ->
            ets:delete(?Label_Table_Name,Key),
             My_Logical_Clock1=My_Logical_Clock+1,
             VClock1=Label#label.vector,
             VClock2=dict:store(My_Dc_Id,My_Logical_Clock1,VClock1),
             Label1=Label#label{vector = VClock2},% before delivery, we have to pump this dc's logical clock
             deliverable_labels(Min_Stable_Timestamp,Deleted+1,[Label1|Batch_To_Deliver],My_Logical_Clock1,My_Dc_Id);
        {_Timestamp,_Partition,_Label}->{Deleted,Batch_To_Deliver,My_Logical_Clock}
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

get_init_remote_Label_dictionary(Dict,Remote_Dc_Count,My_DC_Id)->
    case Remote_Dc_Count of
               0   ->Dict;
        My_DC_Id   -> get_init_remote_Label_dictionary(Dict,Remote_Dc_Count-1,My_DC_Id);
              _    ->Dict1=dict:store(Remote_Dc_Count,[],Dict),
                     get_init_remote_Label_dictionary(Dict1,Remote_Dc_Count-1,My_DC_Id)
    end.

%%%===================================================================
%%% Remote Label specific functions
%%%===================================================================
do_possible_delivers_to_vnodes([],My_VClock,_Sender_Dc_Id,_My_Id,HasChanged)->
    {[],My_VClock,HasChanged};

do_possible_delivers_to_vnodes([Head|Rest],My_VClock,Sender_Dc_Id,My_Id,HasChanged)->
    Received_VClock=Head#label.vector,
    Received_Remote_Vclock=dict:erase(My_Id,Received_VClock),
    IsDeliverable=riak_kv_vclock:is_label_deliverable(My_VClock,Received_Remote_Vclock,Sender_Dc_Id),

    case IsDeliverable of
        true->
              Max_VClock=riak_kv_vclock:get_max_vector(My_VClock,Received_Remote_Vclock),
               %update the clock of the label
              Head1=Head#label{vector = Max_VClock},
              DocIdx = riak_core_util:chash_key(Head#label.bkey),
              PrefList = riak_core_apl:get_primary_apl(DocIdx, 1,riak_kv),
              [{IndexNode, _Type}] = PrefList,
              riak_kv_vnode:deliver_stable_label(Head1,Sender_Dc_Id,IndexNode,self()),

              %wait_for_response(),

              do_possible_delivers_to_vnodes(Rest,Max_VClock,Sender_Dc_Id,My_Id,true);

            _ ->{[Head|Rest],My_VClock,HasChanged}   %labels from receiver are in order, if first is not deliverable, then we cant deliver all the rest
    end.

%wait_for_response()->
 %   receive
  %     ok ->ok
   %  after 5000 ->
    %    lager:info("*****TIMEOUT OCCURED AFTER SENDING A REMOTE LABEL TO A VNODE*********")
    %end.

process_unsatisfied_remote_labels(Remote_LabelDict,My_Vclock,My_Id)->
    {Remote_LabelDict1,My_Vclock1,New_Scan_Needed1} = lists:foldl(fun(Key, {RLabels,MVector,NScan}) ->
    List= dict:fetch(Key, Remote_LabelDict),
      case List of
            []->{RLabels,MVector,NScan};
            _ ->{Rest,Clock,HasChanged}=do_possible_delivers_to_vnodes(List,MVector,Key,My_Id,false),
                        case HasChanged of
                            true  -> {dict:store(Key,Rest,RLabels),Clock,true};
                                _ -> {RLabels,MVector,NScan}
                        end
      end
    end, {Remote_LabelDict,My_Vclock,false},dict:fetch_keys(Remote_LabelDict)),

%if a label is delivered from 1 dc, we need to scan whole other dc list again to see whether anything else is deliverable
       case New_Scan_Needed1 of
              true-> process_unsatisfied_remote_labels(Remote_LabelDict1,My_Vclock1,My_Id);
              _   -> {Remote_LabelDict1,My_Vclock1}
       end.


