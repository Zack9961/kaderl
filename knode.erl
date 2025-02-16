-module(knode).
-behaviour(gen_server).
-export([start_link/2, store/2, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).
-export([ping/0, stop/0, get_id/0, read_store/0, initialize_kbuckets/1, find_node/1, find_value/1]).

start_link(Name, BootstrapNode) ->
    gen_server:start_link({local, Name}, ?MODULE, #{bootstrap => BootstrapNode}, []).

init(State) ->
    Id = generate_node_id(),
    io:format("Kademlia node starting, initial state: ~p, id: ~p~n", [State, Id]),
    StoreTable = ets:new(kademlia_store, [set]),
    KBuckets = ets:new(buckets, [set]),
    % Inizializza la tabella KBuckets con 160 intervalli
    initialize_kbuckets(KBuckets),
    %recupero il nodo bootstrap
    BootstrapNode = maps:get(bootstrap, State, undefined),

    NewState = {Id, 0, State, StoreTable, KBuckets},
    case BootstrapNode of
        % Sono il primo nodo
        undefined ->
            io:format("Nodo ~p diventato bootstrap~n", [Id]),
            {ok, NewState};
        % Connetto al nodo bootstrap
        _ ->
            io:format("Nodo ~p si connette a bootstrap ~p~n", [Id, BootstrapNode]),
            BootstrapNode ! {join_request, self(), Id},
            {ok, NewState}
    end.

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
handle_call({find_value, Key}, _From, {Id, Counter, State, StoreTable, KBuckets}) ->
    io:format("Node ~p (~p) received FIND_VALUE request for Key ~p from ~p~n", [
        self(), Id, Key, _From
    ]),
    case ets:lookup(StoreTable, Key) of
        [{Key, Value}] ->
            % Il nodo ha il valore, lo restituisce
            _From ! {found_value, Value},
            {reply, ok, {Id, Counter, State, StoreTable, KBuckets}};
        [] ->
            % Il nodo non ha il valore, restituisce i nodi più vicini
            KBucketsList = ets:tab2list(KBuckets),
            ClosestNodes = find_closest_nodes(Key, KBucketsList),
            _From ! {found_nodes, ClosestNodes},
            {reply, ok, {Id, Counter, State, StoreTable, KBuckets}}
    end;
handle_call(_Request, _From, State) ->
    io:format("Received unknown request: ~p~n", [_Request]),
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    io:format("Received cast message: ~p~n", [_Msg]),
    {noreply, State}.

handle_info({join_request, PIDNewNode, IdNewNode}, {Id, Counter, State, StoreTable, KBuckets}) ->
    io:format("Sono il Nodo ~p, ho ricevuto join_request da ~p~n", [Id, PIDNewNode]),
    K = 20,
    % 2. Calcola la distanza tra il mio ID e l'ID del NewNode
    Distanza = calcola_distanza(Id, IdNewNode),

    % 3. Determina a quale k-bucket appartiene questa distanza
    BucketIndex = get_bucket_index(Distanza),

    % 4. Recupera la lista attuale di nodi nel k-bucket
    KBucketsList = ets:tab2list(KBuckets),
    % Ottieni i nodi nel bucket
    {_, NodesInBucket} = lists:nth(BucketIndex, KBucketsList),

    % 5. Aggiungi il NewNode al k-bucket (gestendo l'eviction se necessario)
    NewBucketNodes = add_node_to_bucket(IdNewNode, NodesInBucket, K),

    % 6. Aggiorna la tabella ETS con il nuovo k-bucket
    ets:insert(KBuckets, {{BucketIndex}, NewBucketNodes}),

    % invia i miei k-buckets
    PIDNewNode ! {k_buckets, ets:tab2list(KBuckets)},
    {noreply, {Id, Counter, State, StoreTable, KBuckets}};
handle_info({k_buckets, Buckets}, {Id, Counter, State, StoreTable, KBuckets}) ->
    io:format("Nodo ~p ricevuto k_buckets ~p~n", [Id, Buckets]),
    % aggiungo i contatti a miei kbuckets
    lists:foreach(fun(B) -> ets:insert(KBuckets, B) end, Buckets),
    {noreply, {Id, Counter, State, StoreTable, KBuckets}};
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
    gen_server:call(?MODULE, {find_node, ToFindNodeId}).

find_value(Key) ->
    gen_server:call(?MODULE, {find_value, Key}).

generate_node_id() ->
    % 1. Genera un intero casuale grande (ad es. a 64 bit)
    % Massimo valore per un intero a 64 bit
    RandomInteger = rand:uniform(18446744073709551615),
    % 2. Applica la funzione hash SHA-1
    HashValue = crypto:hash(sha, integer_to_binary(RandomInteger)),
    % 3. Converto l'id in binario grezzo in un intero decimale
    IntHashValue = binary_to_integer_representation(HashValue),
    IntHashValue.

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
    Distanza = Id1 bxor Id2,
    io:format("La distanza calcolata è: ~p ~n", [Distanza]),
    Distanza.

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

get_bucket_index(Distanza) ->
    % Implementa la logica per determinare a quale bucket appartiene la distanza
    % Esempio: bucket_index(Distanza) when Distanza >= 2#0000 and Distanza < 2#0001 -> 0;
    %          ...
    %          bucket_index(Distanza) -> 159.
    Distanza.

% Funzione per aggiungere un nodo al bucket (gestendo l'eviction se necessario)
add_node_to_bucket(NewNodeId, BucketNodes, K) ->
    if
        length(BucketNodes) < K ->
            [NewNodeId | BucketNodes];
        true ->
            [_ | Rest] = BucketNodes,
            [NewNodeId | Rest]
    end.

binary_to_integer_representation(Binary) ->
    io:format("Il binario grezzo è: ~p ~n", [Binary]),
    Bytes = binary_to_list(Binary),
    lists:foldl(fun(Byte, Acc) -> (Acc bsl 8) bor Byte end, 0, Bytes).
