-module(knode).
-behaviour(gen_server).
-include_lib("stdlib/include/ms_transform.hrl").
-define(K, 5).
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
            adding_bootstrap_node_in_kbuckets(BootstrapNode, KBuckets, Id),
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
%Guardare se deve essere call/cast
handle_info({join_request, PIDNewNode, IdNewNode}, {Id, Counter, State, StoreTable, KBuckets}) ->
    io:format("Sono il Nodo ~p, ho ricevuto join_request da ~p~n", [Id, PIDNewNode]),

    % Calcolo la distanza tra il mio ID e l'ID del NewNode
    Distanza = calcola_distanza(Id, IdNewNode),

    % Ottengo il giusto intervallo del k-bucket
    RightKbucket = get_right_bucket_interval(Distanza, KBuckets),
    io:format("Il kbucket giusto è: ~p~n", [RightKbucket]),

    % Recupera il contenuto corrente del k-bucket
    [{Key, CurrentNodes}] = ets:lookup(KBuckets, RightKbucket),
    io:format("L'output di lookup è: ~p~n", [[{Key, CurrentNodes}]]),

    %aggiungo in coda
    UpdatedNodes = CurrentNodes ++ [{PIDNewNode, IdNewNode}],

    %Gestisco la dimensione massima del k-bucket
    NewNodes =
        if
            length(UpdatedNodes) > ?K ->
                % Rimuovi il nodo meno recentemente visto (alla testa della lista)
                %Ricordarsi di aggiungere il ping, quindi di rimuoverlo se pingando non
                %risponde, andando avanti per tutti i nodi seguenti, se tutti rispondono,
                %non aggiungere il nodo
                tl(UpdatedNodes);
            true ->
                UpdatedNodes
        end,

    %Reinserisco la tupla aggiornata nel k-bucket
    ets:insert(KBuckets, {Key, NewNodes}),

    % invia i miei k-buckets
    PIDNewNode ! {k_buckets, KBuckets},
    {noreply, {Id, Counter, State, StoreTable, KBuckets}};
handle_info({k_buckets, BucketsReceived}, {Id, Counter, State, StoreTable, KBuckets}) ->
    io:format("Nodo ~p ricevuto k_buckets ~p~n", [Id, BucketsReceived]),
    % 1. Converte la tabella ETS dei buckets in una lista
    BucketsReceivedList = ets:tab2list(BucketsReceived),

    % 2. Itera sulla lista di buckets ricevuti dal nodo bootstrap
    lists:foreach(
        fun({_, Nodes}) ->
            % Itera sulla lista di nodi in ogni bucket
            lists:foreach(
                fun({NodePid, NodeId}) ->
                    % Calcola la distanza tra il mio ID e l'ID del nodo corrente
                    Distanza = calcola_distanza(Id, NodeId),

                    case Distanza of
                        0 ->
                            % Non fare nulla, il nodo è se stesso
                            ok;
                        _ ->
                            % Determina l'intervallo corretto per questa distanza
                            RightKbucket = get_right_bucket_interval(Distanza, KBuckets),

                            % Recupera i nodi attualmente presenti nel bucket corretto
                            case RightKbucket of
                                false ->
                                    io:format(
                                        "+++++ ERRORE: nessun bucket trovato per la distanza ~p (nodo ~p)~n",
                                        [Distanza, NodeId]
                                    );
                                {_, _} ->
                                    [{Key, CurrentNodes}] = ets:lookup(KBuckets, RightKbucket),

                                    % Aggiungi il nuovo nodo alla lista (se non è già presente)
                                    NewNodes =
                                        case lists:keyfind(NodeId, 2, CurrentNodes) of
                                            % Non trovato, lo aggiunge
                                            false ->
                                                io:format(
                                                    "+++++ Aggiungo nodo ~p (id ~p) al bucket ~p~n",
                                                    [
                                                        NodePid, NodeId, RightKbucket
                                                    ]
                                                ),
                                                CurrentNodes ++ [{NodePid, NodeId}];
                                            % Già presente, non fare nulla
                                            _ ->
                                                io:format(
                                                    "+++++ Nodo ~p (id ~p) già presente nel bucket ~p~n",
                                                    [
                                                        NodePid, NodeId, RightKbucket
                                                    ]
                                                ),
                                                CurrentNodes
                                        end,

                                    % Gestisci la dimensione massima del k-bucket (se necessario)
                                    NewNodesLimited =
                                        if
                                            length(NewNodes) > ?K ->
                                                tl(NewNodes);
                                            true ->
                                                NewNodes
                                        end,

                                    % Aggiorna la tabella ETS con i nuovi nodi nel bucket
                                    ets:insert(KBuckets, {Key, NewNodesLimited})
                            end
                    end
                end,
                Nodes
            )
        end,
        BucketsReceivedList
    ),
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
    % 1. Genera un intero casuale grande (64 bit)
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

get_right_bucket_interval(Distanza, KBuckets) ->
    MS = ets:fun2ms(
        fun({{LowerBound, UpperBound}, ListOfNodes}) when
            Distanza >= LowerBound, Distanza =< UpperBound
        ->
            {LowerBound, UpperBound}
        end
    ),
    case ets:select(KBuckets, MS) of
        [Bucket] ->
            Bucket;
        [] ->
            % Nessun bucket trovato (caso teoricamente impossibile)
            false
    end.

binary_to_integer_representation(Binary) ->
    io:format("Il binario grezzo è: ~p ~n", [Binary]),
    Bytes = binary_to_list(Binary),
    lists:foldl(fun(Byte, Acc) -> (Acc bsl 8) bor Byte end, 0, Bytes).

adding_bootstrap_node_in_kbuckets(BootstrapPID, KBuckets, Id) ->
    BootstrapID = gen_server:call(BootstrapPID, get_id),
    io:format("Il bootstrap id è: ~p~n", [BootstrapID]),

    % Calcolo la distanza tra il mio ID e l'ID del BootstrapNode
    Distanza = calcola_distanza(Id, BootstrapID),

    % Ottengo il giusto intervallo del k-bucket
    RightKbucket = get_right_bucket_interval(Distanza, KBuckets),
    io:format("Il kbucket giusto è: ~p~n", [RightKbucket]),

    % Recupera il contenuto corrente del k-bucket
    [{Key, CurrentNodes}] = ets:lookup(KBuckets, RightKbucket),
    io:format("L'output di lookup è: ~p~n", [[{Key, CurrentNodes}]]),

    %aggiungo in coda
    UpdatedNodes = CurrentNodes ++ [{BootstrapPID, BootstrapID}],

    %Reinserisco la tupla aggiornata nel k-bucket
    ets:insert(KBuckets, {Key, UpdatedNodes}).
