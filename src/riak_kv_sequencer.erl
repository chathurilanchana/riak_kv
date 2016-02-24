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
    get_sequence_number/1,
    print_status/0
    ]).

-define(SERVER, ?MODULE).

-record(state, {sequence_id,requests_received=0}).

start_link() ->
    ServerName=app_helper:get_env(riak_kv,sequencer_register_name),

    gen_server:start_link({global, ServerName}, ?MODULE, [ServerName], []).


%to print status when we need
print_status()->
    ServerName=app_helper:get_env(riak_kv,sequencer_register_name),
    gen_server:call({global,ServerName}, {trigger}).

get_sequence_number(Sequencer_Name)->
    gen_server:call({global,Sequencer_Name},{sequence}).

init([_Name]) ->
    lager:info("sequencer started"),
    erlang:send_after(10000, self(), print_stats),
    {ok, #state{sequence_id = 0}}.


handle_call({sequence}, _From, State=#state{sequence_id = Current_Id,requests_received = Request_Count}) ->
    {reply, Current_Id+1, State#state{requests_received = Request_Count+1}};

handle_call({trigger},_From, State=#state{requests_received = RequestCount}) ->
    lager:info("sum request count is ~p ~n",[RequestCount]),
    {reply,ok,State}.



handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(print_stats, State=#state{requests_received = RequestCount}) ->
    {_,{Hour,Min,Sec}} = erlang:localtime(),
    lager:info("sum request count at ~p:~p:~p is ~p ~n",[Hour,Min,Sec,RequestCount]),
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
