-module(knode).
-behaviour(gen_server).
-export([start_link/0, store/2, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).
-export([ping/0, stop/0, get_id/0, read_store/0]).

%% Client API
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, #{}, []).

%% gen_server callbacks
init(State) ->
    Id = generate_node_id(),
    io:format("Kademlia node starting, initial state: ~p, id: ~p~n", [State, Id]),
    StoreTable = ets:new(kademlia_store, [set, named_table]),
    % Inizializza lo stato con un contatore e una tabella ETS
    {ok, {Id, 0, State, StoreTable}}.

handle_call({ping, Sender}, _From, {Id, Counter, State, StoreTable}) ->
    io:format("Node ~p (~p) received ping from ~p~n", [self(), Id, Sender]),
    Sender ! {pong, self()},
    {reply, ok, {Id, Counter, State, StoreTable}};
handle_call(stop, _From, {Id, Counter, State, StoreTable}) ->
    {stop, normal, {Id, Counter, State, StoreTable}};
handle_call(get_id, _From, {Id, Counter, State, StoreTable}) ->
    {reply, Id, {Id, Counter, State, StoreTable}};
handle_call({store, Key, Value}, _From, {Id, Counter, State, StoreTable}) ->
    io:format("Received store request: Id=~p Key=~p, Value=~p, From=~p~n", [Id, Key, Value, _From]),
    % Incrementa il contatore
    NewCounter = Counter + 1,
    % Inserisci la tupla nella tabella ETS
    ets:insert(StoreTable, {Key, Value}),
    io:format("Inserted in ETS: Key=~p, Value=~p~n", [Key, Value]),
    % Rispondi al client
    {reply, ok, {Id, NewCounter, State, StoreTable}};
handle_call(read_store, _From, {Id, Counter, State, StoreTable}) ->
    io:format("Received read_store request from ~p~p~n", [_From, Id]),
    % Leggi la tabella ETS
    Tuples = ets:tab2list(StoreTable),
    io:format("Table content: ~p~n", [Tuples]),
    {reply, Tuples, {Id, Counter, State, StoreTable}};
handle_call(_Request, _From, State) ->
    io:format("Received unknown request: ~p~n", [_Request]),
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    io:format("Received cast message: ~p~n", [_Msg]),
    {noreply, State}.

handle_info(_Info, State) ->
    io:format("Received info message: ~p~n", [_Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    io:format("Kademlia node terminating~n"),
    ok.

ping() ->
    gen_server:call(?MODULE, {ping, self()}).

stop() ->
    gen_server:cast(?MODULE, stop).

get_id() ->
    gen_server:call(?MODULE, get_id).

store(Key, Value) ->
    gen_server:call(?MODULE, {store, Key, Value}).

read_store() ->
    gen_server:call(?MODULE, read_store).

generate_node_id() ->
    % 1. Genera un intero casuale grande (ad es. a 64 bit)
    % Massimo valore per un intero a 64 bit
    RandomInteger = rand:uniform(18446744073709551615),
    % 2. Applica la funzione hash SHA-1
    HashValue = crypto:hash(sha, integer_to_binary(RandomInteger)),
    % 3. L'HashValue è ora il tuo ID a 160 bit
    HashValue.
