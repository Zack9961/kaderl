-module(kademlia_node).
-behaviour(gen_server).
-export([start_link/0, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, stop/0]).
-export([ping/1]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, ok, []).

init(State) ->
    io:format("Kademlia node starting, initial state: ~p~n", [State]),
    {ok, State}.

handle_call({ping, Sender}, _From, State) ->
    io:format("Node ~p received ping from ~p~n", [self(), Sender]),
    Sender ! {pong, self()},
    {reply, ok, State};
handle_call(stop, _From, State) ->
    {stop, normal, State};
handle_call(Other, _From, State) ->
    io:format("Node ~p received unknown call: ~p~n", [self(), Other]),
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    io:format("Node ~p received info: ~p~n", [self(), _Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    io:format("Node ~p terminating~n", [self()]),
    ok.

ping(Node) ->
    gen_server:call(Node, {ping, self()}, 1000).

stop() ->
    gen_server:cast(?MODULE, stop).
