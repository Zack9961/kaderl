-module(knode).
-behaviour(gen_server).
-export([start_link/0, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, stop/0]).
-export([ping/0, get_id/0]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, ok, []).

init(State) ->
    Id = generate_node_id(),
    io:format("Kademlia node starting, initial state: ~p, id: ~p~n", [State, Id]),
    {ok, {Id, State}}.

handle_call({ping, Sender}, _From, {Id, State}) ->
    io:format("Node ~p (~p) received ping from ~p~n", [self(), Id, Sender]),
    Sender ! {pong, self()},
    {reply, ok, {Id, State}};
handle_call(stop, _From, {Id, State}) ->
    {stop, normal, {Id, State}};
handle_call(get_id, _From, {Id, State}) ->
    {reply, Id, {Id, State}};
handle_call(Other, _From, {Id, State}) ->
    io:format("Node ~p (~p) received unknown call: ~p~n", [self(), Id, Other]),
    {reply, ok, {Id, State}}.

handle_cast(_Msg, {Id, State}) ->
    {noreply, {Id, State}}.

handle_info(_Info, {Id, State}) ->
    io:format("Node ~p (~p) received info: ~p~n", [self(), Id, _Info]),
    {noreply, {Id, State}}.

terminate(_Reason, {Id, _State}) ->
    io:format("Node ~p (~p) terminating~n", [self(), Id]),
    ok.

ping() ->
    gen_server:call(?MODULE, {ping, self()}).

stop() ->
    gen_server:cast(?MODULE, stop).

get_id() ->
    gen_server:call(?MODULE, get_id).

generate_node_id() ->
    % 1. Genera un intero casuale grande (ad es. a 64 bit)
    % Massimo valore per un intero a 64 bit
    RandomInteger = rand:uniform(18446744073709551615),
    % 2. Applica la funzione hash SHA-1
    HashValue = crypto:hash(sha, integer_to_binary(RandomInteger)),
    % 3. L'HashValue è ora il tuo ID a 160 bit
    HashValue.
