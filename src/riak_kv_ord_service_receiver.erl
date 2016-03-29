%%%-------------------------------------------------------------------
%%% @author chathuri
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 02. Mar 2016 12:40
%%%-------------------------------------------------------------------
-module(riak_kv_ord_service_receiver).
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

-export([deliver_to_receiver/1]).

-define(SERVER, ?MODULE).

-record(state, {count}).


%%%===================================================================
%%% API Methods
%%%===================================================================

deliver_to_receiver(Batch_To_Deliver)->
    gen_server:call({global,riak_kv_ord_service_receiver},{add_remote_labels,Batch_To_Deliver}).


start_link() ->
    gen_server:start_link({global,riak_kv_ord_service_receiver}, ?MODULE, [riak_kv_ord_service_receiver], []).

init([ServerName]) ->
    lager:info("receiver ~p started ~n",[ServerName]),
    erlang:send_after(10000, self(), print_stats),
    {ok, #state{count = 0}}.


%dummy server just receives messages and ignore them
handle_call({add_remote_labels,_Batch_To_Deliver},_From,State=#state{count = Count})->
    {reply,ok,State#state{count = Count+1}};
    
handle_call(_Request, _From, State) ->
    {reply, ok, State}.
    

handle_cast(_Request, State) ->
    lager:error("received an unexpected  message ~n"),
    {noreply, State}.

handle_info(print_stats, State=#state{count = Count}) ->
    {_,{Hour,Min,Sec}} = erlang:localtime(),
    lager:info("timestamp ~p: ~p: ~p: this server is called ~p times ~n",[Hour,Min,Sec,Count]),
    erlang:send_after(10000, self(), print_stats),
    {noreply, State};


handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
