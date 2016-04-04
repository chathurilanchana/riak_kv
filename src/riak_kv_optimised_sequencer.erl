%%%-------------------------------------------------------------------
%%% @author chathuri
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 02. Mar 2016 12:40
%%%-------------------------------------------------------------------
-module(riak_kv_optimised_sequencer).
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

-export([forward_put_to_sequencer/5,test/0]).

-define(SERVER, ?MODULE).

-record(state, {count, sequence_id,next_sequencer_id}).

test()->
    Status=net_kernel:connect_node('riak@127.0.0.1'), %this is the node where we run global server
    global:sync(),
      io:format("calling test ~p ~n",[Status]),
    case catch gen_server:call({global,riak_kv_optimised_sequencer},{test}) of
        {'EXIT', ErrorExit} -> io:fwrite("ErrorExit ~p~n",[ErrorExit]),lager:info("error is ~p ~n",[ErrorExit]);
            {_, _}  ->lager:info("another error occured  ~n");
            ok->lager:info("this is working")
    end.


forward_put_to_sequencer(RObj,Options,Primary_Sequencer_Name,ReqId,Sender)->
    gen_server:cast({global,Primary_Sequencer_Name}, {put,RObj, Options,ReqId,Sender}).

start_link() ->
    MyId=app_helper:get_env(riak_kv, myid),
    Sequencer_Name=string:concat(?SEQUENCER_PREFIX,integer_to_list(MyId)),
    gen_server:start_link({global,Sequencer_Name}, ?MODULE, [], []).

init([]) ->
    Next_Id=app_helper:get_env(riak_kv, next_id),
    lager:info("fault tolerant optimized sequencer strted ~n"),
    erlang:send_after(10000, self(), print_stats),
    {ok, #state{count = 0,sequence_id = 0,next_sequencer_id = Next_Id}}.

handle_call({test}, _From,State) ->
    lager:info("request received by server ~n"),
    {reply,ok, State};

handle_call(Request, _From, State) ->
    lager:info("received msg is ~p ~n",[Request]),
    %lager:info("received msg is ~p ~n",[Request]),
    {reply, ok, State}.

handle_cast({put,RObj, Options,ReqId,Sender},State=#state{sequence_id = SequenceId,count = Count,next_sequencer_id = Next_Sequencer_Id})->
    case Next_Sequencer_Id of
        -1->lager:info("sending reply"), Sender!{ReqId,ok};
        _ ->Next_Seq_Name= string:concat(?SEQUENCER_PREFIX,integer_to_list(Next_Sequencer_Id)),
            forward_put_to_sequencer(RObj,Options,Next_Seq_Name,ReqId,Sender)

    end,
    {noreply,State#state{sequence_id = SequenceId+1,count = Count+1}};

handle_cast({test}, State) ->
    lager:info("put request received by server ~n"),
    {noreply, State}.

handle_info(print_stats, State=#state{count=Count}) ->
    {_,{Hour,Min,Sec}} = erlang:localtime(),
    lager:info("timestamp ~p: ~p: ~p: added ~p ~n",[Hour,Min,Sec,Count]),
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
