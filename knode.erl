-module(knode).
-behaviour(gen_server).
-include_lib("stdlib/include/ms_transform.hrl").
-define(K, 20).
-define(T, 3600).
-define(A, 3).
-export([start_link/2, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).
-export([
    stop/0,
    get_id/1,
    read_store/0,
    find_node/3,
    %find_value/3,
    ping/1,
    find_value_iterative/4,
    find_node_iterative/3,
    start_nodes/1,
    calcola_tempo_totale_find_value/1,
    start_find_node_iterative/2,
    start_find_value_iterative/2
]).

start_link(Name, BootstrapNode) ->
    gen_server:start_link({local, Name}, ?MODULE, #{bootstrap => BootstrapNode}, []).

init(State) ->
    Id = generate_node_id(),
    io:format("Kademlia node ~p starting, initial state: ~p, id: ~p~n", [self(), State, Id]),
    StoreTable = ets:new(kademlia_store, [set]),
    KBuckets = ets:new(buckets, [set]),
    % Inizializza la tabella KBuckets con 160 intervalli
    initialize_kbuckets(KBuckets),
    %recupero il nodo bootstrap
    BootstrapNodePID = maps:get(bootstrap, State, undefined),
    start_periodic_republish(self()),
    NewState = {Id, State, StoreTable, KBuckets},
    case BootstrapNodePID of
        % Sono il primo nodo
        undefined ->
            %io:format("Nodo ~p diventato bootstrap~n", [Id]),
            {ok, NewState};
        % Connetto al nodo bootstrap
        _ ->
            %io:format("Nodo ~p prova a connettersi a bootstrap ~p~n", [Id, BootstrapNodePID]),
            %case catch gen_server:call(BootstrapNodePID, get_id, 2000) of
            RequestId = generate_requestId(),
            case
                catch gen_server:call(
                    BootstrapNodePID, {join_request, Id, RequestId}, 2000
                )
            of
                {'EXIT', _} ->
                    % Nodo bootstrap non raggiungibile, divento bootstrap
                    % io:format("Nodo bootstrap ~p non raggiungibile (~p), divento bootstrap~n", [
                    %     BootstrapNodePID, Reason
                    % ]),
                    {ok, NewState};
                {k_buckets, ClosestNodesReceived, BootstrapID, RequestIdReceived} ->
                    % Nodo bootstrap raggiungibile, procedo normalmente
                    % io:format("Nodo bootstrap ~p risponde, ID: ~p~n", [
                    %     BootstrapNodePID, BootstrapID
                    % ]),
                    %Controllo se il requestId è corretto
                    case RequestId == RequestIdReceived of
                        true ->
                            %aggiungo il nodo bootstrap ai miei kbucket e poi aggiungo anche i suoi nodi
                            %mandati in risposta
                            add_bootstrap_node_in_kbuckets(
                                BootstrapNodePID, KBuckets, Id, BootstrapID
                            ),
                            %KBucketsList = ets:tab2list(BucketsReceived),
                            %NodesList = get_all_nodes_from_kbuckets_list(KBucketsList),
                            %Potrei mettere la find_closest_nodes, così non metto
                            %tutti i nodi ma solo quelli più vicini
                            add_nodes_to_kbuckets(Id, ClosestNodesReceived, KBuckets),
                            {ok, NewState};
                        _ ->
                            % io:format("Sono il nodo con pid: ~p Request Id non corretto~n", [
                            %     BootstrapNodePID
                            % ]),
                            {ok, NewState}
                    end
            end
    end.
handle_call({ping, RequestId}, _From, {Id, State, StoreTable, KBuckets}) ->
    %{PID, _} = _From,
    %io:format("Node ~p (~p) received ping from ~p~n", [self(), Id, PID]),
    {reply, {pong, self(), RequestId}, {Id, State, StoreTable, KBuckets}};
handle_call(get_id, _From, {Id, State, StoreTable, KBuckets}) ->
    {reply, Id, {Id, State, StoreTable, KBuckets}};
handle_call({get_alpha_nodes, Key, RequestId}, _From, {Id, State, StoreTable, KBuckets}) ->
    KBucketsList = ets:tab2list(KBuckets),
    KClosestNodes = find_closest_nodes(Key, KBucketsList),
    AlphaClosestNodes = lists:sublist(KClosestNodes, ?A),
    {reply, {alpha_nodes, AlphaClosestNodes, RequestId}, {Id, State, StoreTable, KBuckets}};
%Funzione cancellabile?
handle_call({read_store, RequestId}, _From, {Id, State, StoreTable, KBuckets}) ->
    %io:format("Received read_store request from ~p,~p~n", [_From, Id]),
    % Leggi la tabella ETS
    Tuples = ets:tab2list(StoreTable),
    %io:format("Table content: ~p~n", [Tuples]),
    {reply, {Tuples, RequestId}, {Id, State, StoreTable, KBuckets}};
handle_call(
    {find_node, ToFindNodeId, ParentNode, RequestId}, _From, {Id, State, StoreTable, KBuckets}
) ->
    %{PID, _} = _From,
    %io:format("Node ~p (~p) received FIND_NODE request for ID ~p from ~p~n", [
    %    self(), Id, ToFindNodeId, PID
    %]),

    %ParentNode perché se prendessi le informazioni da _From sarebbero
    %le informazioni del processo generato da spawn e non del nodo
    %che invia la richiesta direttamente

    %provo ad aggiungere il nodo che mi ha inviato la richiesta ai miei kbuckets
    add_node_to_kbuckets(Id, ParentNode, KBuckets),

    % Recupera i k-bucket dalla tabella ETS
    KBucketsList = ets:tab2list(KBuckets),
    % Cerco i nodi più vicini nei miei kbucketss
    ClosestNodes = find_closest_nodes(ToFindNodeId, KBucketsList),
    % Rispondi al nodo richiedente con la lista dei nodi più vicini
    {reply, {found_nodes, ClosestNodes, RequestId}, {Id, State, StoreTable, KBuckets}};
handle_call(
    {find_value, Key, ParentNode, RequestId}, _From, {Id, State, StoreTable, KBuckets}
) ->
    %{PID, _} = _From,
    % io:format("Node ~p (~p) received FIND_VALUE request for Key ~p from pid: ~p~n", [
    %     self(), Id, Key, ParentPID
    % ]),

    %Aggiungo il ParentNode, nei miei kbuckets
    add_node_to_kbuckets(Id, ParentNode, KBuckets),

    case ets:lookup(StoreTable, Key) of
        [{Key, Value}] ->
            % Il nodo ha il valore, lo restituisce
            %io:format("Nodo ~p trova il valore ~p", [self(), Key]),
            {reply, {found_value, Value, RequestId}, {Id, State, StoreTable, KBuckets}};
        %PID ! {found_value, Value};
        [] ->
            % Il nodo non ha il valore, restituisce i nodi più vicini
            KBucketsList = ets:tab2list(KBuckets),
            ClosestNodes = find_closest_nodes(Key, KBucketsList),
            {reply, {found_nodes, ClosestNodes, RequestId}, {Id, State, StoreTable, KBuckets}}
        %PID ! {found_nodes, ClosestNodes}
    end;
handle_call(
    {join_request, IdNewNode, RequestId}, _From, {Id, State, StoreTable, KBuckets}
) ->
    {PID, _} = _From,
    %io:format("Sono il Nodo ~p, ho ricevuto join_request da ~p~n", [Id, PID]),

    % Calcolo la distanza tra il mio ID e l'ID del NewNode
    Distanza = calcola_distanza(Id, IdNewNode),

    % Ottengo il giusto intervallo del k-bucket
    RightKbucket = get_right_bucket_interval(Distanza, KBuckets),
    %io:format("Il kbucket giusto è: ~p~n", [RightKbucket]),

    % Recupera il contenuto corrente del k-bucket
    [{Key, CurrentNodes}] = ets:lookup(KBuckets, RightKbucket),
    %io:format("L'output di lookup è: ~p~n", [[{Key, CurrentNodes}]]),

    %aggiungo in coda
    %UpdatedNodes = CurrentNodes ++ [{PID, IdNewNode}],

    %Gestisco la dimensione massima del k-bucket
    NewNodes =
        if
            %se il bucket è pieno
            length(CurrentNodes) == ?K ->
                %creo una nuova lista con i nodi che rispondono al ping
                NewCurrentNodes = lists:foldl(
                    fun({Pid, ID}, Acc) ->
                        case ping(Pid) of
                            {pong, _} ->
                                [Acc | {Pid, ID}];
                            _ ->
                                Acc
                        end
                    end,
                    [],
                    CurrentNodes
                ),
                %se la nuova lista è ancora piena allora non aggiungere il nodo
                %altrimenti aggiungilo in coda
                case length(NewCurrentNodes) == ?K of
                    true ->
                        NewCurrentNodes;
                    _ ->
                        NewCurrentNodes ++ [{PID, IdNewNode}]
                end;
            %se il bucket non è pieno lo aggiungo in coda
            true ->
                CurrentNodes ++ [{PID, IdNewNode}]
        end,

    %Reinserisco la tupla aggiornata nel k-bucket
    ets:insert(KBuckets, {Key, NewNodes}),

    % invia la lista di nodi più vicini a lui
    KBucketsList = ets:tab2list(KBuckets),
    ClosestNodes = find_closest_nodes(IdNewNode, KBucketsList),
    {reply, {k_buckets, ClosestNodes, Id, RequestId}, {Id, State, StoreTable, KBuckets}};
handle_call(
    {start_find_node_iterative, Key, RequestId}, _From, {Id, State, StoreTable, KBuckets}
) ->
    %Prendo dai miei kbuckets la lista dei k nodi più vicini alla chiave
    KBucketsList = ets:tab2list(KBuckets),
    ClosestNodes = find_closest_nodes(Key, KBucketsList),
    %io:format("I nodi più vicini trovati sono:~p~n", [ClosestNodes]),
    %Scelgo alpha nodi da interrogare in parallelo
    AlphaClosestNodes = lists:sublist(ClosestNodes, ?A),

    %ParentNode utile per dare l'informazione del nodo padre ai nodi riceventi
    %in modo che possono aggiungere questo nodo ai propri kbuckets
    ParentNode = {self(), Id},

    %Quindi faccio partire la funzione find_node_iterative
    IterativeNodesFound = find_node_iterative(AlphaClosestNodes, Key, ParentNode),

    {reply, {founded_nodes_from_iteration, IterativeNodesFound, RequestId},
        {Id, State, StoreTable, KBuckets}};
handle_call(
    {start_find_value_iterative, Key, RequestId}, _From, {Id, State, StoreTable, KBuckets}
) ->
    %Prendo dai miei kbuckets la lista dei k nodi più vicini alla chiave
    KBucketsList = ets:tab2list(KBuckets),
    ClosestNodes = find_closest_nodes(Key, KBucketsList),
    %io:format("I nodi più vicini trovati sono:~p~n", [ClosestNodes]),
    %Scelgo alpha nodi da interrogare in parallelo
    AlphaClosestNodes = lists:sublist(ClosestNodes, ?A),

    %Quindi faccio partire la funzione find_value_iterative
    ParentNode = {self(), Id},
    IterativeFound = find_value_iterative(AlphaClosestNodes, Key, ParentNode, KBuckets),

    %io:format("IterativeFound è :~p~n", [IterativeFound]),

    case IterativeFound of
        {found_value, Value} ->
            {reply, {found_value, Value, RequestId}, {Id, State, StoreTable, KBuckets}};
        _ ->
            {reply, {value_not_found, IterativeFound, RequestId}, {Id, State, StoreTable, KBuckets}}
    end;
handle_call(_Request, _From, State) ->
    %io:format("Received unknown request: ~p~n", [_Request]),
    {reply, {error, unknown_request}, State}.

handle_cast({store, Value}, {Id, State, StoreTable, KBuckets}) ->
    %io:format("Received store request: Id=~p, Value=~p, From=?~n", [Id, Value]),
    %Calcola la key
    HashValue = crypto:hash(sha, integer_to_binary(Value)),
    Key = binary_to_integer_representation(HashValue),
    % Inserisci la tupla nella tabella ETS
    ets:insert(StoreTable, {Key, Value}),
    %io:format("Inserted in ETS: Key=~p, Value=~p~n", [Key, Value]),
    % Rispondi al client
    {noreply, {Id, State, StoreTable, KBuckets}};
handle_cast({store, Key, Value}, {Id, State, StoreTable, KBuckets}) ->
    %io:format("Received store request: Id=~p Key=~p, Value=~p, From=?~n", [Id, Key, Value]),

    % Inserisci la tupla nella tabella ETS
    ets:insert(StoreTable, {Key, Value}),
    %io:format("Inserted in ETS: Key=~p, Value=~p~n", [Key, Value]),
    % Rispondi al client
    {noreply, {Id, State, StoreTable, KBuckets}};
handle_cast(republish, {Id, State, StoreTable, KBuckets}) ->
    io:format("Received republish message, sono il nodo con il pid:~p~n", [self()]),
    % 1. Ripubblica i dati
    republish_data(StoreTable, KBuckets),
    % 2. Reimposta il timer per la prossima ripubblicazione
    start_periodic_republish(self()),
    {noreply, {Id, State, StoreTable, KBuckets}};
handle_cast(stop, State) ->
    {stop, normal, State};
handle_cast(_Msg, State) ->
    io:format("Received cast message: ~p~n", [_Msg]),
    {noreply, State}.

handle_info(_Info, State) ->
    io:format("Received info message: ~p~n", [_Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    io:format("Kademlia node terminating~p~n", [_Reason]),
    ok.

% ping() ->
%     gen_server:call(?MODULE, {ping, self()}).

ping(PID) ->
    %ets:insert(KBuckets, {{nuhu, vv}, []}),
    RequestId = generate_requestId(),
    case gen_server:call(PID, {ping, RequestId}, 2000) of
        {pong, ReceiverPID, ReceiverRequestId} ->
            case RequestId == ReceiverRequestId of
                true ->
                    io:format("Node with PID ~p received pong from ~p~n", [self(), ReceiverPID]),
                    {pong, ReceiverPID};
                _ ->
                    io:format(
                        "Sono il nodo con pid ~p, requestId ricevuto da ~p non corretto ~n", [
                            self(), ReceiverPID
                        ]
                    ),
                    {invalid_requestid, ReceiverPID, ReceiverRequestId}
            end;
        {'EXIT', Reason} ->
            io:format("Nodo con pid ~p non raggiungibile (~p)~n", [PID, Reason]),
            {error, Reason};
        _ ->
            io:format("Risposta non gestita"),
            {error}
    end.

stop() ->
    gen_server:cast(?MODULE, stop).

get_id(PID) ->
    gen_server:call(PID, get_id, 2000).

% store(Key, Value) ->
%     gen_server:cast(?MODULE, {store, Key, Value}).

store(PID, Key, Value) ->
    gen_server:cast(PID, {store, Key, Value}).

read_store() ->
    gen_server:call(?MODULE, read_store).

find_node(PID, ToFindNodeId, RequestId) ->
    gen_server:call(PID, {find_node, ToFindNodeId, RequestId}).

% find_value(PID, Key, RequestId) ->
%     gen_server:call(PID, {find_value, Key, ParentPID, RequestId}).

generate_node_id() ->
    % 1. Genera un intero casuale grande (64 bit)
    % Massimo valore per un intero a 64 bit
    RandomInteger = rand:uniform(18446744073709551615),
    % 2. Applica la funzione hash SHA-1
    HashValue = crypto:hash(sha, integer_to_binary(RandomInteger)),
    % 3. Converto l'id in binario grezzo in un intero decimale
    IntHashValue = binary_to_integer_representation(HashValue),
    IntHashValue.

generate_requestId() ->
    rand:uniform(18446744073709551615).

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
    %io:format("La distanza calcolata è: ~p ~n", [Distanza]),
    Distanza.

find_closest_nodes(Key, KBuckets) ->
    %Controllare che sia un ID quindi fare check del formato
    % 1. Ottieni tutti i nodi dai k-buckets.
    Nodi = get_all_nodes_from_kbuckets(KBuckets),
    %io:format("Nodi estratti: ~p~n", [Nodi]),

    % 2. Calcola la distanza di ogni nodo rispetto a ToFindNodeId
    NodiConDistanza = aggiungi_distanza(Nodi, Key),
    %io:format("Nodi con distanza: ~p~n", [NodiConDistanza]),

    % NodiConDistanzaNo0 = lists:filter(fun({_, _, X}) -> X =/= 0 end, NodiConDistanza),
    % io:format("Nodi con distanza senza 0: ~p~n", [NodiConDistanzaNo0]),

    % 3. Ordina i nodi per distanza crescente.
    NodiOrdinati = lists:sort(
        fun({_, _, Dist1}, {_, _, Dist2}) -> Dist1 < Dist2 end, NodiConDistanza
    ),
    %io:format("Nodi ordinati: ~p~n", [NodiOrdinati]),

    % 4. Restituisce i primi K nodi (o tutti se ce ne sono meno di K) senza la distanza.
    KClosestNodesWithDistance = lists:sublist(NodiOrdinati, ?K),
    KClosestNodes = lists:map(
        fun({PIDNodo, IdNodo, _}) -> {PIDNodo, IdNodo} end, KClosestNodesWithDistance
    ),

    %io:format("Nodi più vicini (limitati a K e senza distanza): ~p~n", [KClosestNodes]),
    KClosestNodes.

get_all_nodes_from_kbuckets(BucketsList) ->
    %Estrae tutti i nodi dai buckets
    lists:foldl(fun({_, Nodes}, Acc) -> Acc ++ Nodes end, [], BucketsList).

aggiungi_distanza(Nodi, IdRiferimento) ->
    lists:map(
        fun({PIDNodo, IdNodo}) ->
            Distanza = calcola_distanza(IdNodo, IdRiferimento),
            {PIDNodo, IdNodo, Distanza}
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
    Bytes = binary_to_list(Binary),
    lists:foldl(fun(Byte, Acc) -> (Acc bsl 8) bor Byte end, 0, Bytes).

add_bootstrap_node_in_kbuckets(BootstrapPID, KBuckets, Id, BootstrapID) ->
    %io:format("Il bootstrap id è: ~p~n", [BootstrapID]),

    % Calcolo la distanza tra il mio ID e l'ID del BootstrapNode
    Distanza = calcola_distanza(Id, BootstrapID),

    % Ottengo il giusto intervallo del k-bucket
    RightKbucket = get_right_bucket_interval(Distanza, KBuckets),
    %io:format("Il kbucket giusto è: ~p~n", [RightKbucket]),

    % Recupera il contenuto corrente del k-bucket
    [{Key, CurrentNodes}] = ets:lookup(KBuckets, RightKbucket),
    %io:format("L'output di lookup è: ~p~n", [[{Key, CurrentNodes}]]),

    %aggiungo in coda
    UpdatedNodes = CurrentNodes ++ [{BootstrapPID, BootstrapID}],

    %Reinserisco la tupla aggiornata nel k-bucket
    ets:insert(KBuckets, {Key, UpdatedNodes}).

start_periodic_republish(NodePID) ->
    %erlang:send_after(timer:seconds(?T), NodePID, republish).
    timer:apply_after(timer:seconds(?T), gen_server, cast, [NodePID, republish]).

republish_data(StoreTable, KBuckets) ->
    %io:format("Sono il nodo con pid: ~p, sto ripubblicando i dati...~n", [self()]),

    % 1. Ottieni tutti i dati (coppie chiave-valore) dalla tabella ETS (StoreTable).
    Data = ets:tab2list(StoreTable),
    %io:format("Dati da ripubblicare: ~p\n", [Data]),

    % 2. Ottieni la lista dei k-buckets
    KBucketsList = ets:tab2list(KBuckets),

    % 3. Per ogni dato (coppia chiave-valore):
    lists:foreach(
        fun({Key, Value}) ->
            %io:format("Ripubblicazione della chiave ~p\n", [Key]),

            % 4. Trova i k nodi più vicini alla chiave.
            ClosestNodes = find_closest_nodes(Key, KBucketsList),

            % case self() == list_to_pid("<0.91.0>") of
            %     true ->
            %         io:format("Sono il pid 0.91.0, i nodi k trovati sono:~p", [ClosestNodes]);
            %     _ ->
            %         ok
            % end,

            % 5. Invia una richiesta STORE a ciascuno dei k nodi più vicini.
            lists:foreach(
                fun({NodePID, _}) ->
                    %io:format("Invio richiesta STORE a nodo ~p (ID: ~p)\n", [NodePID, NodeId]),
                    % Invia la richiesta STORE in modo asincrono (cast) per non bloccare il processo di ripubblicazione
                    %NodePID ! {store, Key, Value}
                    store(NodePID, Key, Value)
                %gen_server:cast(NodePID, {store, Key, Value})
                end,
                ClosestNodes
                %,
            )
        %io:format("Richieste di store mandate per la chiave ~p\n", [Key])
        end,
        Data
    ).

add_nodes_to_kbuckets(Id, NodesListToAdd, MyKBuckets) ->
    %io:format("Nodo ~p ricevuto sta per aggiungere questi nodi: ~p~n", [self(), NodesListToAdd]),

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
                    RightKbucket = get_right_bucket_interval(Distanza, MyKBuckets),

                    % Recupera i nodi attualmente presenti nel bucket corretto
                    case RightKbucket of
                        %false ->
                        % io:format(
                        %     "+++++ ERRORE: nessun bucket trovato per la distanza ~p (nodo ~p)~n",
                        %     [Distanza, NodeId]
                        % );
                        {_, _} ->
                            [{Key, CurrentNodes}] = ets:lookup(MyKBuckets, RightKbucket),
                            NewNodes =
                                case length(CurrentNodes) == ?K of
                                    %se il bucket è pieno
                                    true ->
                                        %creo una nuova lista con i nodi che rispondono al ping
                                        NewCurrentNodes = lists:foldl(
                                            fun({Pid, ID}, Acc) ->
                                                case ping(Pid) of
                                                    {pong, _} ->
                                                        [Acc | {Pid, ID}];
                                                    _ ->
                                                        Acc
                                                end
                                            end,
                                            [],
                                            CurrentNodes
                                        ),
                                        %se la nuova lista è ancora piena allora non aggiungere il nodo
                                        %altrimenti aggiungilo in coda
                                        case length(NewCurrentNodes) == ?K of
                                            true ->
                                                NewCurrentNodes;
                                            _ ->
                                                NewCurrentNodes ++ [{NodePid, NodeId}]
                                        end;
                                    _ ->
                                        % Aggiungi il nuovo nodo alla lista (se non è già presente)

                                        case lists:keyfind(NodeId, 2, CurrentNodes) of
                                            % Non trovato, lo aggiunge
                                            false ->
                                                % io:format(
                                                %     "+++++ Aggiungo nodo ~p (id ~p) al bucket ~p~n",
                                                %     [
                                                %         NodePid, NodeId, RightKbucket
                                                %     ]
                                                % ),
                                                CurrentNodes ++ [{NodePid, NodeId}];
                                            % Già presente, non fare nulla
                                            _ ->
                                                % io:format(
                                                %     "+++++ Nodo ~p (id ~p) già presente nel bucket ~p~n",
                                                %     [
                                                %         NodePid, NodeId, RightKbucket
                                                %     ]
                                                % ),
                                                CurrentNodes
                                        end
                                end,

                            % Aggiorna la tabella ETS con i nuovi nodi nel bucket
                            ets:insert(MyKBuckets, {Key, NewNodes})
                    end
            end
        end,
        NodesListToAdd
    ).

add_node_to_kbuckets(Id, NodeReceived, MyKBuckets) ->
    %io:format("Nodo ~p ricevuto k_buckets ~p~n", [Id, BucketsReceived]),
    {NodePid, NodeId} = NodeReceived,

    % Calcola la distanza tra il mio ID e l'ID del nodo corrente
    Distanza = calcola_distanza(Id, NodeId),

    case Distanza of
        0 ->
            % Non fare nulla, il nodo è se stesso
            ok;
        _ ->
            % Determina l'intervallo corretto per questa distanza
            RightKbucket = get_right_bucket_interval(Distanza, MyKBuckets),

            % Recupera i nodi attualmente presenti nel bucket corretto
            case RightKbucket of
                {_, _} ->
                    [{Key, CurrentNodes}] = ets:lookup(MyKBuckets, RightKbucket),
                    NewNodes =
                        case length(CurrentNodes) == ?K of
                            %se il bucket è pieno
                            true ->
                                %creo una nuova lista con i nodi che rispondono al ping
                                NewCurrentNodes = lists:foldl(
                                    fun({Pid, ID}, Acc) ->
                                        case ping(Pid) of
                                            {pong, _} ->
                                                [Acc | {Pid, ID}];
                                            _ ->
                                                Acc
                                        end
                                    end,
                                    [],
                                    CurrentNodes
                                ),
                                %se la nuova lista è ancora piena allora non aggiungere il nodo
                                %altrimenti aggiungilo in coda
                                case length(NewCurrentNodes) == ?K of
                                    true ->
                                        NewCurrentNodes;
                                    _ ->
                                        NewCurrentNodes ++ [{NodePid, NodeId}]
                                end;
                            %se il kbucket non è pieno
                            _ ->
                                % Aggiungi il nuovo nodo alla lista (se non è già presente)
                                case lists:keyfind(NodeId, 2, CurrentNodes) of
                                    % Non trovato, lo aggiunge
                                    false ->
                                        % io:format(
                                        %     "+++++ Aggiungo nodo ~p (id ~p) al bucket ~p~n",
                                        %     [
                                        %         NodePid, NodeId, RightKbucket
                                        %     ]
                                        % ),
                                        CurrentNodes ++ [{NodePid, NodeId}];
                                    % Già presente, non fare nulla
                                    _ ->
                                        % io:format(
                                        %     "+++++ Nodo ~p (id ~p) già presente nel bucket ~p~n",
                                        %     [
                                        %         NodePid, NodeId, RightKbucket
                                        %     ]
                                        % ),
                                        CurrentNodes
                                end
                        end,

                    % Aggiorna la tabella ETS con i nuovi nodi nel bucket
                    ets:insert(MyKBuckets, {Key, NewNodes});
                _ ->
                    % io:format(
                    %     "+++++ ERRORE: nessun bucket trovato per la distanza ~p (nodo ~p)~n",
                    %     [Distanza, NodeId]
                    % )
                    {error, rightbucket_not_found}
            end
    end.

find_node_iterative(AlphaClosestNodes, Key, ParentNode) ->
    find_node_iterative(AlphaClosestNodes, Key, ParentNode, []).

find_node_iterative(AlphaClosestNodes, Key, ParentNode, BestNodes) ->
    case BestNodes == [] of
        %caso base
        true ->
            io:format("La lista:~p~n", [AlphaClosestNodes]),
            %Creo la lista di soli pid dalla lista di tuple
            ParentPID = self(),
            AlphaClosestNodesPID = lists:map(
                fun({PIDNodo, _}) ->
                    PIDNodo
                end,
                AlphaClosestNodes
            ),
            %α
            % Eseguo la spawn di tanti processi quanti sono gli elementi della lista AlphaClosestNodesPID
            lists:foreach(
                fun(PIDNodo) ->
                    % Utilizzo di erlang:spawn/3 per eseguire la richiesta in un nuovo processo
                    spawn(fun() ->
                        % Eseguo la richiesta al nodo
                        RequestId = generate_requestId(),
                        Response = gen_server:call(
                            PIDNodo, {find_node, Key, ParentNode, RequestId}
                        ),
                        %Controllo il requestId, in caso trova nodi ma il request id è sbagliato
                        %allora manda invalid_request id, negli altri casi manda una response
                        case Response of
                            {found_nodes, _, ReceivedRequestId} ->
                                case RequestId == ReceivedRequestId of
                                    true ->
                                        ParentPID ! {ok, Response};
                                    _ ->
                                        ParentPID ! {invalid_requestid, Response}
                                end;
                            _ ->
                                ParentPID ! {ok, Response}
                        end
                    % Invio la risposta al processo principale
                    end)
                end,
                AlphaClosestNodesPID
            ),

            % Gestisco le risposte, ricevo una lista di liste di nodi ricevuti
            Responses = receive_responses_find_node(length(AlphaClosestNodesPID), []),
            FlatResponse = lists:flatten(Responses),

            %Qui aggiungo anche i nodi che ho interrogato
            ReceivedNodesAndTriedNodes = FlatResponse ++ AlphaClosestNodes,

            NoDuplicatedList = ordsets:to_list(ordsets:from_list(ReceivedNodesAndTriedNodes)),

            NodiConDistanza = aggiungi_distanza(NoDuplicatedList, Key),
            %Ordina i nodi per distanza crescente.
            NodiOrdinati = lists:sort(
                fun({_, _, Dist1}, {_, _, Dist2}) -> Dist1 < Dist2 end, NodiConDistanza
            ),

            % Restituisce i primi K nodi (o tutti se ce ne sono meno di K) senza la distanza.
            NewBestNodesWithDistance = lists:sublist(NodiOrdinati, ?K),
            NewBestNodes = lists:map(
                fun({PIDNodo, IdNodo, _}) -> {PIDNodo, IdNodo} end, NewBestNodesWithDistance
            ),

            NewBestNodesNoSelf = lists:filter(
                fun({Pid, _}) -> Pid /= ParentPID end, NewBestNodes
            ),

            %Ora interrogo alpha nodi che prendo dalla lista di nodi che ho ricavato
            find_node_iterative(
                lists:sublist(NewBestNodesNoSelf, ?A), Key, ParentNode, NewBestNodesWithDistance
            );
        %in questo caso ho dei best nodes da confrontare con quelli che tireranno fuori
        %i processi alla prossima iterazione
        _ ->
            %Creo la lista di soli pid dalla lista di tuple
            ParentPID = self(),
            AlphaClosestNodesPID = lists:map(
                fun({PIDNodo, _}) ->
                    PIDNodo
                end,
                AlphaClosestNodes
            ),
            %α
            % Eseguo la spawn di tanti processi quanti sono gli elementi della lista AlphaClosestNodesPID
            lists:foreach(
                fun(PIDNodo) ->
                    % Utilizzo di erlang:spawn/3 per eseguire la richiesta in un nuovo processo
                    spawn(fun() ->
                        % Eseguo la richiesta al nodo
                        RequestId = generate_requestId(),
                        Response = gen_server:call(
                            PIDNodo, {find_node, Key, ParentNode, RequestId}
                        ),
                        %Controllo il requestId, in caso trova nodi ma il request id è sbagliato
                        %allora manda invalid_request id, negli altri casi manda una response
                        case Response of
                            {found_nodes, _, ReceivedRequestId} ->
                                case RequestId == ReceivedRequestId of
                                    true ->
                                        ParentPID ! {ok, Response};
                                    _ ->
                                        ParentPID ! {invalid_requestid, Response}
                                end;
                            _ ->
                                ParentPID ! {ok, Response}
                        end
                    % Invio la risposta al processo principale
                    end)
                end,
                AlphaClosestNodesPID
            ),

            % Gestisco le risposte, ricevo una lista di liste di nodi ricevuti
            Responses = receive_responses_find_node(length(AlphaClosestNodesPID), []),

            FlatResponse = lists:flatten(Responses),

            %Qui aggiungo anche i nodi che ho interrogato
            ReceivedNodesAndTriedNodes = FlatResponse ++ AlphaClosestNodes,

            NoDuplicatedList = ordsets:to_list(ordsets:from_list(ReceivedNodesAndTriedNodes)),

            NodiConDistanza = aggiungi_distanza(NoDuplicatedList, Key),
            %Ordina i nodi per distanza crescente.
            NodiOrdinati = lists:sort(
                fun({_, _, Dist1}, {_, _, Dist2}) -> Dist1 < Dist2 end, NodiConDistanza
            ),

            % Restituisce i primi K nodi (o tutti se ce ne sono meno di K) senza la distanza.
            NewBestNodesWithDistance = lists:sublist(NodiOrdinati, ?K),
            NewBestNodes = lists:map(
                fun({PIDNodo, IdNodo, _}) -> {PIDNodo, IdNodo} end, NewBestNodesWithDistance
            ),
            io:format(
                "La lista dei kclosest appena trovata da~p è:~p, mentre quella dei bestnodes è: ~p~n ",
                [ParentPID, NewBestNodesWithDistance, BestNodes]
            ),

            %forse per far convergere devo mettere come argomento tutti i nodi che sono stati trovati
            %e se i nuovi nodi trovati sono un sottoinsieme di tutti i nodi trovati allora converge

            case lists:all(fun(X) -> lists:member(X, BestNodes) end, NewBestNodesWithDistance) of
                true ->
                    %non ho trovato i nodi più vicini
                    %io:format("non ho trovato nodi più vicini"),
                    lists:sublist(BestNodes, ?K);
                _ ->
                    AllReceivedNodesWithDuplicates = NewBestNodesWithDistance ++ BestNodes,
                    AllReceivedNodesNosort = ordsets:to_list(
                        ordsets:from_list(AllReceivedNodesWithDuplicates)
                    ),
                    AllReceivedNodes = lists:sort(
                        fun({_, _, Dist1}, {_, _, Dist2}) -> Dist1 < Dist2 end,
                        AllReceivedNodesNosort
                    ),
                    %Tolgo il pid che sta facendo l'iterazione, perché non
                    %ha senso che mandi un messaggio a se stesso
                    NewBestNodesNoSelf = lists:filter(
                        fun({Pid, _}) -> Pid /= ParentPID end, NewBestNodes
                    ),

                    find_node_iterative(
                        lists:sublist(NewBestNodesNoSelf, ?A), Key, ParentNode, AllReceivedNodes
                    )
            end
    end.

receive_responses_find_node(0, Responses) ->
    % Ho ricevuto tutte le risposte, posso elaborarle
    %elaborate_responses(Responses);
    Responses;
receive_responses_find_node(N, Responses) ->
    receive
        {ok, Response} ->
            case Response of
                {found_nodes, NodeList, _} ->
                    receive_responses_find_node(N - 1, [NodeList | Responses]);
                _ ->
                    receive_responses_find_node(N - 1, [Responses])
            end;
        {invalid_requestid, _} ->
            receive_responses_find_node(N - 1, [Responses])
        % Ricevo una risposta e la aggiungo alla lista
    end.

find_value_iterative(AlphaClosestNodes, Key, ParentNode, ParentKBuckets) ->
    find_value_iterative(AlphaClosestNodes, Key, ParentNode, ParentKBuckets, []).
find_value_iterative(AlphaClosestNodes, Key, ParentNode, ParentKBuckets, BestNodes) ->
    case BestNodes == [] of
        %caso base
        true ->
            io:format("La lista:~p~n", [AlphaClosestNodes]),
            ParentPID = self(),
            %Creo la lista di soli pid dalla lista di tuple
            AlphaClosestNodesPID = lists:map(
                fun({PIDNodo, _}) ->
                    PIDNodo
                end,
                AlphaClosestNodes
            ),
            % Eseguo la spawn di tanti processi quanti sono gli elementi della lista AlphaClosestNodesPID
            lists:foreach(
                fun(PIDNodo) ->
                    % Utilizzo di erlang:spawn/3 per eseguire la richiesta in un nuovo processo
                    spawn(fun() ->
                        % Eseguo la richiesta al nodo
                        RequestId = generate_requestId(),

                        Response = gen_server:call(
                            PIDNodo, {find_value, Key, ParentNode, RequestId}
                        ),
                        %Controllo il requestId, in caso trova nodi ma il request id è sbagliato
                        %allora manda invalid_request id, negli altri casi manda una response
                        io:format("La risposta del PID ~p è ~p~n", [PIDNodo, Response]),

                        case Response of
                            {found_value, _, ReceivedRequestId} ->
                                case RequestId == ReceivedRequestId of
                                    true ->
                                        ParentPID ! {found_value, Response};
                                    _ ->
                                        ParentPID ! {invalid_requestid, Response}
                                end;
                            {found_nodes, _, ReceivedRequestId} ->
                                case RequestId == ReceivedRequestId of
                                    true ->
                                        ParentPID ! {found_nodes, Response};
                                    _ ->
                                        ParentPID ! {invalid_requestid, Response}
                                end;
                            _ ->
                                ParentPID ! {ok, Response}
                        end
                    % Invio la risposta al processo principale
                    end)
                end,
                AlphaClosestNodesPID
            ),

            % Gestisco le risposte, ricevo una lista di liste di nodi ricevuti
            Responses = receive_responses_find_value(length(AlphaClosestNodesPID), []),
            FlatResponse = lists:flatten(Responses),
            io:format("La flatresponse è:~p~n ", [FlatResponse]),

            %qui faccio l'if, se ho trovato il valore ritorno direttamente il valore altrimenti
            %come find_node

            case lists:any(fun({Value, _}) -> Value == found_value end, FlatResponse) of
                true ->
                    Value = hd(
                        lists:filter(fun({Value, _}) -> Value == found_value end, FlatResponse)
                    ),
                    Value;
                _ ->
                    %Qui aggiungo anche i nodi che ho interrogato
                    ReceivedNodesAndTriedNodes = FlatResponse ++ AlphaClosestNodes,

                    NoDuplicatedList = ordsets:to_list(
                        ordsets:from_list(ReceivedNodesAndTriedNodes)
                    ),

                    NodiConDistanza = aggiungi_distanza(NoDuplicatedList, Key),
                    %Ordina i nodi per distanza crescente.
                    NodiOrdinati = lists:sort(
                        fun({_, _, Dist1}, {_, _, Dist2}) -> Dist1 < Dist2 end, NodiConDistanza
                    ),

                    % Restituisce i primi K nodi (o tutti se ce ne sono meno di K) senza la distanza.
                    NewBestNodesWithDistance = lists:sublist(NodiOrdinati, ?K),
                    NewBestNodes = lists:map(
                        fun({PIDNodo, IdNodo, _}) -> {PIDNodo, IdNodo} end, NewBestNodesWithDistance
                    ),

                    NewBestNodesNoSelf = lists:filter(
                        fun({Pid, _}) -> Pid /= ParentPID end, NewBestNodes
                    ),
                    {_, ParentID} = ParentNode,
                    %Qui aggiorno i kbuckets,
                    %NewBestNodesWithDistance non va bene così in ingresso
                    add_nodes_to_kbuckets(ParentID, NewBestNodesWithDistance, ParentKBuckets),

                    %Ora interrogo alpha nodi che prendo dalla lista di nodi che ho ricavato
                    find_value_iterative(
                        lists:sublist(NewBestNodesNoSelf, ?A),
                        Key,
                        ParentNode,
                        ParentKBuckets,
                        NewBestNodesWithDistance
                    )
            end;
        %in questo caso ho dei best nodes da confrontare con quelli che tireranno fuori
        %i processi alla prossima iterazione
        _ ->
            %Creo la lista di soli pid dalla lista di tuple
            ParentPID = self(),
            AlphaClosestNodesPID = lists:map(
                fun({PIDNodo, _}) ->
                    PIDNodo
                end,
                AlphaClosestNodes
            ),
            %α
            % Eseguo la spawn di tanti processi quanti sono gli elementi della lista AlphaClosestNodesPID
            lists:foreach(
                fun(PIDNodo) ->
                    % Utilizzo di erlang:spawn/3 per eseguire la richiesta in un nuovo processo
                    spawn(fun() ->
                        % Eseguo la richiesta al nodo
                        RequestId = generate_requestId(),
                        Response = gen_server:call(
                            PIDNodo, {find_value, Key, ParentNode, RequestId}
                        ),
                        %Controllo il requestId, in caso trova nodi ma il request id è sbagliato
                        %allora manda invalid_request id, negli altri casi manda una response
                        case Response of
                            {found_value, _, ReceivedRequestId} ->
                                case RequestId == ReceivedRequestId of
                                    true ->
                                        ParentPID ! {found_value, Response};
                                    _ ->
                                        ParentPID ! {invalid_requestid, Response}
                                end;
                            {found_nodes, _, ReceivedRequestId} ->
                                case RequestId == ReceivedRequestId of
                                    true ->
                                        ParentPID ! {found_nodes, Response};
                                    _ ->
                                        ParentPID ! {invalid_requestid, Response}
                                end;
                            _ ->
                                ParentPID ! {ok, Response}
                        end
                    % Invio la risposta al processo principale
                    end)
                end,
                AlphaClosestNodesPID
            ),

            % Gestisco le risposte, ricevo una lista di liste di nodi ricevuti
            Responses = receive_responses_find_value(length(AlphaClosestNodesPID), []),

            FlatResponse = lists:flatten(Responses),

            %io:format("La flatresponse è:~p~n ", [FlatResponse]),

            %qui faccio l'if, se trovo il nodo termino altrimenti come find_node
            case lists:any(fun({Value, _}) -> Value == found_value end, FlatResponse) of
                true ->
                    Value = hd(
                        lists:filter(fun({Value, _}) -> Value == found_value end, FlatResponse)
                    ),
                    Value;
                _ ->
                    %Qui aggiungo anche i nodi che ho interrogato
                    ReceivedNodesAndTriedNodes = FlatResponse ++ AlphaClosestNodes,

                    NoDuplicatedList = ordsets:to_list(
                        ordsets:from_list(ReceivedNodesAndTriedNodes)
                    ),

                    NodiConDistanza = aggiungi_distanza(NoDuplicatedList, Key),
                    %Ordina i nodi per distanza crescente.
                    NodiOrdinati = lists:sort(
                        fun({_, _, Dist1}, {_, _, Dist2}) -> Dist1 < Dist2 end, NodiConDistanza
                    ),

                    % Restituisce i primi K nodi (o tutti se ce ne sono meno di K) senza la distanza.
                    NewBestNodesWithDistance = lists:sublist(NodiOrdinati, ?K),
                    NewBestNodes = lists:map(
                        fun({PIDNodo, IdNodo, _}) -> {PIDNodo, IdNodo} end, NewBestNodesWithDistance
                    ),
                    io:format(
                        "La lista dei kclosest appena trovata da~p è:~p, mentre quella dei bestnodes è: ~p~n ",
                        [ParentPID, NewBestNodesWithDistance, BestNodes]
                    ),

                    %controllo se i nodi che ho trovato sono un sottoinsieme dei nodi trovati in precedenze
                    %se lo sono allora termino l'esecuzione e restituisco i migliori nodi trovati fino ad ora
                    %altrimenti aggiungo i nodi appena trovati alla lista di tutti i nodi trovati,
                    %e faccio partire la nuova iterazione della funzione mettendo come argomento i primi alpha
                    %nodi, la chiave e tutti i nodi trovati fino ad ora
                    case
                        lists:all(
                            fun(X) -> lists:member(X, BestNodes) end, NewBestNodesWithDistance
                        )
                    of
                        %non ho trovato i nodi più vicini, restituisco i migliori nodi trovati
                        true ->
                            %io:format("non ho trovato nodi più vicini"),
                            lists:sublist(BestNodes, ?K);
                        %ho trovato nodi mai trovati prima, continuo con l'iterazione
                        _ ->
                            %aggiungo i nuovi nodi trovati alla lista di tutti i nodi
                            %trovati, tolgo i doppioni e riordino la lista
                            AllReceivedNodesWithDuplicates = NewBestNodesWithDistance ++ BestNodes,
                            AllReceivedNodesNosort = ordsets:to_list(
                                ordsets:from_list(AllReceivedNodesWithDuplicates)
                            ),
                            AllReceivedNodes = lists:sort(
                                fun({_, _, Dist1}, {_, _, Dist2}) -> Dist1 < Dist2 end,
                                AllReceivedNodesNosort
                            ),
                            %In caso tolgo il pid che sta facendo l'iterazione, perché non
                            %ha senso che mandi un messaggio a se stesso
                            NewBestNodesNoSelf = lists:filter(
                                fun({Pid, _}) -> Pid /= ParentPID end, NewBestNodes
                            ),

                            find_value_iterative(
                                lists:sublist(NewBestNodesNoSelf, ?A),
                                Key,
                                ParentNode,
                                ParentKBuckets,
                                AllReceivedNodes
                            )
                    end
            end
    end.

receive_responses_find_value(0, Responses) ->
    % Ho ricevuto tutte le risposte, posso elaborarle
    %elaborate_responses(Responses);
    Responses;
receive_responses_find_value(N, Responses) ->
    receive
        {found_nodes, Response} ->
            % io:format(
            %     "Sono il processo numero ~p e ho trovato nodi: ~p~n ",
            %     [N, Response]
            % ),
            case Response of
                {found_nodes, NodeList, _} ->
                    receive_responses_find_value(N - 1, [NodeList | Responses]);
                _ ->
                    receive_responses_find_value(N - 1, [Responses])
            end;
        {found_value, Response} ->
            % io:format(
            %     "Sono il processo numero ~p e ho trovato il valore: ~p~n ",
            %     [N, Response]
            % ),
            case Response of
                %Se la risposta è del tipo giusto aggiungo il valore alla lista di risposte
                {found_value, Value, _} ->
                    receive_responses_find_value(N - 1, [{found_value, Value} | Responses]);
                _ ->
                    receive_responses_find_value(N - 1, [Responses])
            end;
        {invalid_requestid, _} ->
            receive_responses_find_value(N - 1, [Responses])
        % Ricevo una risposta e la aggiungo alla lista
    end.

%mando un messaggio al nodo che deve coordinare la routine iterativa
%della ricerca, comando per la shell in modo da testare il nodo
start_find_node_iterative(NodePID, Key) ->
    RequestId = generate_requestId(),
    case gen_server:call(NodePID, {start_find_node_iterative, Key, RequestId}, timer:seconds(2)) of
        {founded_nodes_from_iteration, FoundedNodes, ReceivedRequestId} ->
            case RequestId == ReceivedRequestId of
                true ->
                    {ok, FoundedNodes, NodePID};
                _ ->
                    {error, invalid_requestid, NodePID}
            end;
        _ ->
            {error, not_found, NodePID}
    end.

%mando un messaggio al nodo che deve coordinare la routine iterativa della ricerca
start_find_value_iterative(NodePID, Key) ->
    RequestId = generate_requestId(),
    case gen_server:call(NodePID, {start_find_value_iterative, Key, RequestId}, timer:seconds(2)) of
        {found_value, Value, ReceivedRequestId} ->
            case RequestId == ReceivedRequestId of
                true ->
                    {found_value, Value, NodePID};
                _ ->
                    {error, invalid_requestid, NodePID}
            end;
        {value_not_found, NodeList, ReceivedRequestId} ->
            case RequestId == ReceivedRequestId of
                true ->
                    {value_not_found, NodeList, NodePID};
                _ ->
                    {error, invalid_requestid, NodePID}
            end;
        _ ->
            {error, NodePID}
    end.

%% Avvia più nodi Kademlia per test
start_nodes(NumNodes) ->
    start_nodes(NumNodes, {}).

start_nodes(0, _) ->
    ok;
start_nodes(NumNodes, BootstrapNode) ->
    Name = list_to_atom("knode_" ++ integer_to_list(NumNodes)),
    case BootstrapNode of
        % Primo nodo, diventa bootstrap
        {} ->
            start_link(Name, undefined),
            start_nodes(NumNodes - 1, {Name, whereis(Name)});
        % Nodi successivi, si connettono al bootstrap
        {NameBootstrap, BootstrapNodePID} ->
            start_link(Name, BootstrapNodePID),
            start_nodes(NumNodes - 1, {NameBootstrap, BootstrapNodePID})
    end.

calcola_tempo_totale_find_value(Key) ->
    NumNodes = 100,
    ListaNodi = [list_to_atom("knode_" ++ integer_to_list(N)) || N <- lists:seq(1, NumNodes)],
    NodiETempi = [
        {Node, Tempo}
     || Node <- ListaNodi,
        {ok, _, Tempo} <- [calcola_tempo(Node, Key)]
    ],
    Tempi = [Tempo || {_, Tempo} <- NodiETempi],
    Media = lists:sum(Tempi) / length(Tempi),
    Percentuale = (length(Tempi) / NumNodes) * 100,
    io:format("Media dei tempi: ~p~n", [Media]),
    io:format("Percentuale nodi che hanno trovato il valore: ~p~n", [Percentuale]),
    lists:foreach(
        fun({Node, Tempo}) -> io:format("Nodo: ~p, Tempo: ~p~n", [Node, Tempo]) end, NodiETempi
    ).

calcola_tempo(Node, Key) ->
    Inizio = erlang:monotonic_time(microsecond),
    Risultato = start_find_value_iterative(Node, Key),
    Fine = erlang:monotonic_time(microsecond),
    Tempo = Fine - Inizio,
    case Risultato of
        {found_value, Value, _} -> {ok, Value, Tempo};
        _ -> false
    end.
