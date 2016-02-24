%%%-------------------------------------------------------------------
%%% @author chathuri
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 24. Feb 2016 14:54
%%%-------------------------------------------------------------------
-module(riak_kv_sequencer).
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
    code_change/3,
    get_sequence_number/1
    ]).

-define(SERVER, ?MODULE).

-record(state, {sequence_id}).

start_link() ->
    ServerName=app_helper:get_env(riak_kv,sequencer_register_name),
    gen_server:start_link({global, ServerName}, ?MODULE, [ServerName], []).


get_sequence_number(Sequencer_Name)->
    gen_server:call({global,Sequencer_Name},{sequence}).

init([_Name]) ->
    lager:info("sequencer started"),
    {ok, #state{sequence_id = 0}}.


handle_call({sequence}, _From, State=#state{sequence_id = Current_Id}) ->
    {reply, Current_Id+1, State}.


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
