%%%-------------------------------------------------------------------
%%% @author chathuri
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 02. Mar 2016 12:40
%%%-------------------------------------------------------------------
-module(riak_kv_optimized_sequencer).
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

-export([forward_put_to_sequencer/5,test/0]).

-define(SERVER, ?MODULE).

-record(state, {myname, sequence_id}).

test()->
lager:info("calling test ~n"),
    case catch gen_server:call({global,riak_kv_optimized_sequencer},{test}) of
        {'EXIT', ErrorExit} -> io:fwrite("ErrorExit ~p~n",[ErrorExit]),lager:info("error is ~p ~n",[ErrorExit]);
            {_, _}  ->lager:info("another error occured  ~n");
            ok->lager:info("this is working")
    end.

forward_put_to_sequencer(RObj, Options, [Node, ClientId],ReqId,Sender)->
    %lager:info("forwarding put to sequncer"),
    gen_server:cast({global,riak_kv_optimized_sequencer}, {put,RObj, Options,  [Node, ClientId],ReqId,Sender}).


start_link() ->
    gen_server:start_link({global,riak_kv_optimized_sequencer}, ?MODULE, [riak_kv_optimized_sequencer], []).

init([ServerName]) ->
    %lager:info("optimized sequencer strted ~n"),
    {ok, #state{myname = ServerName,sequence_id = 0}}.



handle_call(Request, _From, State) ->
    lager:info("received msg is ~p ~n",[Request]),
    %lager:info("received msg is ~p ~n",[Request]),
    {reply, ok, State}.

handle_cast({put,RObj, Options,[Node, _ClientId],ReqId,Sender},State=#state{sequence_id = SequenceId})->
    %lager:info("optimised sequencer received the call node is ~p received is ~p ~n",[node(),Node]),

    %rpc:call(Node, riak_kv_put_fsm, start_link,
    %               [{raw, ReqId, Sender}, RObj, Options]),
    
    %rpc:async_call(Node, riak_kv_put_fsm, start_link,
    %               [{raw, ReqId, Sender}, RObj, Options]),               

    proc_lib:spawn_link(Node, riak_kv_put_fsm, start_link,
                [{raw, ReqId, Sender}, RObj, Options]),

    {noreply,State#state{sequence_id = SequenceId+1}};

handle_cast({test}, State) ->
    lager:info("request received by server ~n"),
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
