-module(knode).
-behaviour(gen_server).
-export([start_link/0, store/2, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).
-export([ping/0, stop/0, get_id/0, read_store/0, initialize_kbuckets/1, find_node/1]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, #{}, []).

init(State) ->
    Id = generate_node_id(),
    io:format("Kademlia node starting, initial state: ~p, id: ~p~n", [State, Id]),
    StoreTable = ets:new(kademlia_store, [set, named_table]),
    KBuckets = ets:new(buckets, [set, named_table]),
    % Inizializza la tabella KBuckets con 160 intervalli
    initialize_kbuckets(KBuckets),
    % Inizializza lo stato con un contatore (che può servire per fare logging) e una tabella ETS
    {ok, {Id, 0, State, StoreTable, KBuckets}}.

handle_call({ping, Sender}, _From, {Id, Counter, State, StoreTable, KBuckets}) ->
    io:format("Node ~p (~p) received ping from ~p~n", [self(), Id, Sender]),
    Sender ! {pong, self()},
    {reply, ok, {Id, Counter, State, StoreTable, KBuckets}};
handle_call(stop, _From, {Id, Counter, State, StoreTable, KBuckets}) ->
    {stop, normal, {Id, Counter, State, StoreTable, KBuckets}};
handle_call(get_id, _From, {Id, Counter, State, StoreTable, KBuckets}) ->
    {reply, Id, {Id, Counter, State, StoreTable, KBuckets}};
handle_call({store, Key, Value}, _From, {Id, Counter, State, StoreTable, KBuckets}) ->
    io:format("Received store request: Id=~p Key=~p, Value=~p, From=~p~n", [Id, Key, Value, _From]),
    % Incrementa il contatore
    NewCounter = Counter + 1,
    % Inserisci la tupla nella tabella ETS
    ets:insert(StoreTable, {Key, Value}),
    io:format("Inserted in ETS: Key=~p, Value=~p~n", [Key, Value]),
    % Rispondi al client
    {reply, ok, {Id, NewCounter, State, StoreTable, KBuckets}};
handle_call(read_store, _From, {Id, Counter, State, StoreTable, KBuckets}) ->
    io:format("Received read_store request from ~p,~p~n", [_From, Id]),
    % Leggi la tabella ETS
    Tuples = ets:tab2list(StoreTable),
    io:format("Table content: ~p~n", [Tuples]),
    {reply, Tuples, {Id, Counter, State, StoreTable, KBuckets}};
handle_call({find_node, ToFindNodeId}, _From, {Id, Counter, State, StoreTable, KBuckets}) ->
    io:format("Node ~p (~p) received FIND_NODE request for ID ~p from ~p~n", [
        self(), Id, ToFindNodeId, _From
    ]),
    % 1. Recupera i k-bucket dalla tabella ETS
    KBucketsList = ets:tab2list(KBuckets),
    % 2. Implementa la logica per trovare i nodi più vicini
    ClosestNodes = find_closest_nodes(ToFindNodeId, KBucketsList),
    % 3. Rispondi al nodo richiedente con la lista dei nodi più vicini
    _From ! {found_nodes, ClosestNodes},
    {reply, ok, {Id, Counter, State, StoreTable, KBuckets}};
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

find_node(ToFindNodeId) ->
    gen_server:call(?MODULE, {read_store, ToFindNodeId}).

generate_node_id() ->
    % 1. Genera un intero casuale grande (ad es. a 64 bit)
    % Massimo valore per un intero a 64 bit
    RandomInteger = rand:uniform(18446744073709551615),
    % 2. Applica la funzione hash SHA-1
    HashValue = crypto:hash(sha, integer_to_binary(RandomInteger)),
    % 3. L'HashValue è ora il tuo ID a 160 bit
    HashValue.

% Funzione per inizializzare gli intervalli della tabella KBuckets
initialize_kbuckets(KBuckets) ->
    lists:foreach(
        fun(N) ->
            {LowerBound, UpperBound} =
                {LowerBound = 1 bsl N, UpperBound = (1 bsl (N + 1)) - 1},
            % Inserisci l'intervallo vuoto nella tabella KBuckets
            ets:insert(KBuckets, {{LowerBound, UpperBound}, []})
        % Genera una lista di 160 elementi
        end,
        lists:seq(0, 159)
    ).

calcola_distanza(Id1, Id2) ->
    %Converte gli identificatori binari in interi
    Int1 = binary_to_integer(Id1),
    Int2 = binary_to_integer(Id2),
    %Calcola la distanza con XOR
    Int1 bxor Int2.

%FUNZIONE DA DEBUGGARE, MAI PROVATA
find_closest_nodes(ToFindNodeId, KBuckets) ->
    % 1. Ottieni tutti i nodi dai k-buckets.
    Nodi = get_all_nodes_from_kbuckets(KBuckets),

    % 2. Calcola la distanza di ogni nodo rispetto a ToFindNodeId
    NodiConDistanza = aggiungi_distanza(Nodi, ToFindNodeId),

    % 3. Ordina i nodi per distanza crescente.
    NodiOrdinati = lists:sort(
        fun({_, _, Dist1}, {_, _, Dist2}) -> Dist1 < Dist2 end, NodiConDistanza
    ),

    % 4. Restituisce i primi k nodi (o tutti se ce ne sono meno di k).
    NodiOrdinati.

get_all_nodes_from_kbuckets(KBuckets) ->
    %Converte la tabella ETS in una lista
    BucketsList = ets:tab2list(KBuckets),
    %Estrae tutti i nodi dai buckets
    lists:foldl(fun({_, Nodes}, Acc) -> Acc ++ Nodes end, [], BucketsList).

aggiungi_distanza(Nodi, IdRiferimento) ->
    lists:map(
        fun({IdNodo, AltroDato}) ->
            Distanza = calcola_distanza(IdNodo, IdRiferimento),
            {IdNodo, AltroDato, Distanza}
        end,
        Nodi
    ).
