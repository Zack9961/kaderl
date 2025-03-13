-module(knode).
-behaviour(gen_server).
-include_lib("stdlib/include/ms_transform.hrl").
-define(K, 20).
-define(T, 3).
-define(A, 3).
-export([start_link/2, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).
-export([
    stop/0,
    ping/1,
    find_value_iterative/3,
    find_node_iterative/3,
    start_nodes/1,
    calcola_tempo_totale_find_value/3,
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
            RequestId = generate_requestId(),
            case
                (catch gen_server:call(
                    BootstrapNodePID, {join_request, Id, RequestId}, 2000
                ))
            of
                {k_buckets, ClosestNodesReceived, BootstrapID, RequestIdReceived} ->
                    % Nodo bootstrap raggiungibile, procedo normalmente
                    case RequestId == RequestIdReceived of
                        true ->
                            BootstrapNode = {BootstrapNodePID, BootstrapID},

                            %aggiungo il nodo bootstrap  e i suoi nodi
                            %mandati in risposta ai miei KBuckets
                            add_nodes_to_kbuckets(
                                Id, ClosestNodesReceived ++ [BootstrapNode], KBuckets
                            ),
                            {ok, NewState};
                        _ ->
                            {ok, NewState}
                    end;
                _ ->
                    % Nodo bootstrap non raggiungibile, divento bootstrap
                    {ok, NewState}
            end
    end.
handle_call({ping, RequestId}, _From, {Id, State, StoreTable, KBuckets}) ->
    %io:format("Node ~p (~p) received ping from ~p~n", [self(), Id, PID]),
    {reply, {pong, self(), RequestId}, {Id, State, StoreTable, KBuckets}};
handle_call(
    {find_node, ToFindNodeId, ParentNode, RequestId}, _From, {Id, State, StoreTable, KBuckets}
) ->
    %ParentNode perché se prendessi le informazioni da _From sarebbero
    %le informazioni del processo generato da spawn e non del nodo
    %che invia la richiesta direttamente
    %provo ad aggiungere il nodo che mi ha inviato la richiesta ai miei kbuckets
    %mandandomi un messaggio cast
    gen_server:cast(self(), {add_nodes_to_kbuckets, [ParentNode]}),

    % Recupera i k-bucket dalla tabella ETS
    KBucketsList = ets:tab2list(KBuckets),
    % Cerco i nodi più vicini nei miei kbuckets
    ClosestNodes = find_closest_nodes(ToFindNodeId, KBucketsList),
    % Rispondi al nodo richiedente con la lista dei nodi più vicini
    {reply, {found_nodes, ClosestNodes, RequestId}, {Id, State, StoreTable, KBuckets}};
handle_call(
    {find_value, Key, ParentNode, RequestId}, _From, {Id, State, StoreTable, KBuckets}
) ->
    %Aggiungo il ParentNode, nei miei kbuckets
    gen_server:cast(self(), {add_nodes_to_kbuckets, [ParentNode]}),

    case ets:lookup(StoreTable, Key) of
        [{Key, Value}] ->
            % Il nodo ha il valore, lo restituisce
            {reply, {found_value, Value, RequestId}, {Id, State, StoreTable, KBuckets}};
        [] ->
            % Il nodo non ha il valore, restituisce i nodi più vicini
            KBucketsList = ets:tab2list(KBuckets),
            ClosestNodes = find_closest_nodes(Key, KBucketsList),
            {reply, {found_nodes, ClosestNodes, RequestId}, {Id, State, StoreTable, KBuckets}}
    end;
handle_call(
    {join_request, IdNewNode, RequestId}, _From, {Id, State, StoreTable, KBuckets}
) ->
    {PidNewNode, _} = _From,
    %io:format("Sono il Nodo ~p, ho ricevuto join_request da ~p~n", [Id, PID]),

    %mando un messaggio a me stesso per aggiungere in modo asincrono il nodo ai kbuckets
    gen_server:cast(self(), {add_nodes_to_kbuckets, [{PidNewNode, IdNewNode}]}),

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
    %Scelgo alpha nodi da interrogare in parallelo
    AlphaClosestNodes = lists:sublist(ClosestNodes, ?A),
    %Quindi faccio partire la funzione find_value_iterative
    ParentNode = {self(), Id},

    IterativeFound = find_value_iterative(AlphaClosestNodes, Key, ParentNode),

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
    %Calcola la key
    HashValue = crypto:hash(sha, integer_to_binary(Value)),
    Key = binary_to_integer_representation(HashValue),
    % Inserisci la tupla nella tabella ETS
    ets:insert(StoreTable, {Key, Value}),
    {noreply, {Id, State, StoreTable, KBuckets}};
handle_cast({store, Key, Value}, {Id, State, StoreTable, KBuckets}) ->
    % Inserisci la tupla nella tabella ETS
    ets:insert(StoreTable, {Key, Value}),
    {noreply, {Id, State, StoreTable, KBuckets}};
handle_cast(republish_data_refresh_kbuckets, {Id, State, StoreTable, KBuckets}) ->
    %io:format("Received republish message, sono il nodo con il pid:~p~n", [self()]),
    republish_data(StoreTable, KBuckets),
    %refresh_kbuckets(Id, KBuckets),
    start_periodic_republish(self()),
    {noreply, {Id, State, StoreTable, KBuckets}};
handle_cast({add_nodes_to_kbuckets, NodesToAdd}, {Id, State, StoreTable, KBuckets}) ->
    add_nodes_to_kbuckets(Id, NodesToAdd, KBuckets),
    {noreply, {Id, State, StoreTable, KBuckets}};
handle_cast(stop, State) ->
    {stop, normal, State};
handle_cast(_Msg, State) ->
    io:format("Received cast message: ~p~n", [_Msg]),
    {noreply, State}.

handle_info(_Info, State) ->
    %io:format("Received info message: ~p~n", [_Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    %io:format("Kademlia node ~p terminating ~p~n", [self(), _Reason]),
    ok.

stop() ->
    gen_server:cast(?MODULE, stop).

store(PID, Key, Value) ->
    gen_server:cast(PID, {store, Key, Value}).

ping(PID) ->
    RequestId = generate_requestId(),
    case (catch gen_server:call(PID, {ping, RequestId}, 2000)) of
        {pong, ReceiverPID, ReceiverRequestId} ->
            case RequestId == ReceiverRequestId of
                true ->
                    {pong, ReceiverPID};
                _ ->
                    {invalid_requestid, ReceiverPID, ReceiverRequestId}
            end;
        {'EXIT', Reason} ->
            {error, Reason};
        _ ->
            io:format("Risposta non gestita"),
            {error}
    end.

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

find_closest_nodes(Key, KBucketsList) ->
    %Controllare che sia un ID quindi fare check del formato
    % Ottiengo tutti i nodi dai k-buckets.
    Nodi = get_all_nodes_from_kbuckets(KBucketsList),

    % Calcolo la distanza di ogni nodo rispetto a Key
    NodiConDistanza = aggiungi_distanza(Nodi, Key),

    % Ordino i nodi per distanza crescente
    NodiOrdinati = lists:sort(
        fun({_, _, Dist1}, {_, _, Dist2}) -> Dist1 < Dist2 end, NodiConDistanza
    ),

    % Restituisco i primi K nodi (o tutti se ce ne sono meno di K) senza la distanza.
    KClosestNodesWithDistance = lists:sublist(NodiOrdinati, ?K),
    KClosestNodes = lists:map(
        fun({PIDNodo, IdNodo, _}) -> {PIDNodo, IdNodo} end, KClosestNodesWithDistance
    ),
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

start_periodic_republish(NodePID) ->
    timer:apply_after(timer:seconds(?T), gen_server, cast, [
        NodePID, republish_data_refresh_kbuckets
    ]).

republish_data(StoreTable, KBuckets) ->
    % Ottengo tutti i dati dalla tabella ETS (StoreTable).
    Data = ets:tab2list(StoreTable),
    % Ottengo la lista dei k-buckets
    KBucketsList = ets:tab2list(KBuckets),
    lists:foreach(
        fun({Key, Value}) ->
            ClosestNodes = find_closest_nodes(Key, KBucketsList),
            % Invio una richiesta STORE a ciascuno dei k nodi più vicini.
            lists:foreach(
                fun({NodePID, _}) ->
                    %io:format("Invio richiesta STORE a nodo ~p (ID: ~p)\n", [NodePID, NodeId]),
                    store(NodePID, Key, Value)
                end,
                ClosestNodes
            )
        end,
        Data
    ).

% refresh_kbuckets(MyId, KBuckets) ->
%     KBucketsList = ets:tab2list(KBuckets),
%     % Genera un numero tra 0 e 159
%     RandomBucketIndex = rand:uniform(160) - 1,
%     {LowerBound, UpperBound} = get_kbucket_range(RandomBucketIndex),
%     RandomId = LowerBound + rand:uniform(UpperBound - LowerBound + 1) - 1,
%     %Cerco gli alpha nodi più vicini a cui mandare la ricerca iterativa della find_node
%     ClosestNodes = find_closest_nodes(RandomId, KBucketsList),
%     AlphaClosestNodes = lists:sublist(ClosestNodes, ?A),
%     %una volta trovati avvio la ricerca
%     find_node_iterative(AlphaClosestNodes, RandomId, {self(), MyId}).

% get_kbucket_range(Index) ->
%     LowerBound = 1 bsl Index,
%     UpperBound = (1 bsl (Index + 1)) - 1,
%     {LowerBound, UpperBound}.

add_nodes_to_kbuckets(Id, NodesListToAdd, MyKBuckets) ->
    lists:foreach(
        fun({NodeToAddPid, NodeToAddId}) ->
            Distanza = calcola_distanza(Id, NodeToAddId),
            case Distanza of
                0 ->
                    % Non fare nulla, il nodo è se stesso
                    ok;
                _ ->
                    % Determino l'intervallo corretto per questa distanza
                    RightKbucket = get_right_bucket_interval(Distanza, MyKBuckets),

                    % Recupero i nodi attualmente presenti nel bucket corretto
                    case RightKbucket of
                        {_, _} ->
                            [{Key, CurrentNodes}] = ets:lookup(MyKBuckets, RightKbucket),
                            NewNodes =
                                case length(CurrentNodes) == ?K of
                                    %se il bucket è pieno
                                    true ->
                                        %pingo il primo nodo del bucket, se risponde allora
                                        %il bucket rimane invariato, altrimenti tolgo il primo
                                        %elemento dalla lista (nodo pingato) e in coda ci
                                        %aggiungo il nuovo nodo
                                        {PidCurrentNode, IdCurrentNode} = hd(CurrentNodes),
                                        case ping(PidCurrentNode) of
                                            {pong, _} ->
                                                tl(CurrentNodes) ++
                                                    [{PidCurrentNode, IdCurrentNode}];
                                            _ ->
                                                tl(CurrentNodes) ++ [{NodeToAddPid, NodeToAddId}]
                                        end;
                                    _ ->
                                        % Aggiungi il nuovo nodo alla lista (se non è già presente)
                                        case lists:keyfind(NodeToAddId, 2, CurrentNodes) of
                                            % Non trovato, lo aggiunge
                                            false ->
                                                CurrentNodes ++ [{NodeToAddPid, NodeToAddId}];
                                            % Già presente, non fare nulla
                                            _ ->
                                                CurrentNodes
                                        end
                                end,

                            % Aggiorna la tabella ETS con i nuovi nodi nel bucket
                            ets:insert(MyKBuckets, {Key, NewNodes});
                        _ ->
                            {error}
                    end
            end
        end,
        NodesListToAdd
    ).

find_node_iterative(AlphaClosestNodes, Key, ParentNode) ->
    find_node_iterative(AlphaClosestNodes, Key, ParentNode, []).

find_node_iterative(AlphaClosestNodes, Key, ParentNode, BestNodes) ->
    ParentPID = self(),
    case BestNodes == [] of
        true ->
            Responses = find_node_spawn(AlphaClosestNodes, Key, ParentNode, ParentPID),

            %Qui aggiungo anche i nodi che ho interrogato
            ReceivedNodesAndTriedNodes = Responses ++ AlphaClosestNodes,

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

            %Qui aggiorno i kbuckets del nodo che ha avviato la funzione (Parent)
            %dato che sono nel caso iniziale, passo come argomento solo i nodi migliori
            %che ho appena trovato (NewBestNodes), dato che non ne ho altri
            gen_server:cast(ParentPID, {add_nodes_to_kbuckets, NewBestNodes}),

            %Ora interrogo alpha nodi che prendo dalla lista di nodi che ho ricavato
            find_node_iterative(
                lists:sublist(NewBestNodesNoSelf, ?A),
                Key,
                ParentNode,
                NewBestNodesWithDistance
            );
        %in questo caso ho dei best nodes da confrontare con quelli che tireranno fuori
        %i processi alla prossima iterazione
        _ ->
            % Gestisco le risposte, ricevo una lista di liste di nodi ricevuti
            Responses = find_node_spawn(AlphaClosestNodes, Key, ParentNode, ParentPID),

            %Qui aggiungo anche i nodi che ho interrogato
            ReceivedNodesAndTriedNodes = Responses ++ AlphaClosestNodes,

            NoDuplicatedList = ordsets:to_list(ordsets:from_list(ReceivedNodesAndTriedNodes)),

            NodiConDistanza = aggiungi_distanza(NoDuplicatedList, Key),
            %Ordina i nodi per distanza crescente.
            NodiOrdinati = lists:sort(
                fun({_, _, Dist1}, {_, _, Dist2}) -> Dist1 < Dist2 end, NodiConDistanza
            ),

            % Restituisce i primi K nodi (o tutti se ce ne sono meno di K) senza la distanza.
            NewBestNodesWithDistance = lists:sublist(NodiOrdinati, ?K),

            case lists:all(fun(X) -> lists:member(X, BestNodes) end, NewBestNodesWithDistance) of
                true ->
                    %non ho trovato i nodi più vicini,
                    % restituisco i nodi migliori trovati
                    lists:sublist(BestNodes, ?K);
                _ ->
                    %aggiungo i nuovi nodi trovati alla lista di tutti i nodi
                    %trovati, tolgo i doppioni e riordino la lista
                    AllReceivedNodesWithDuplicates =
                        NewBestNodesWithDistance ++ BestNodes,
                    AllReceivedNodesNosort = ordsets:to_list(
                        ordsets:from_list(AllReceivedNodesWithDuplicates)
                    ),
                    AllReceivedNodes = lists:sort(
                        fun({_, _, Dist1}, {_, _, Dist2}) -> Dist1 < Dist2 end,
                        AllReceivedNodesNosort
                    ),
                    AllReceivedNodesNoDistance = lists:map(
                        fun({PIDNodo, IdNodo, _}) -> {PIDNodo, IdNodo} end,
                        AllReceivedNodes
                    ),
                    %In caso tolgo il pid che sta facendo l'iterazione, perché non
                    %ha senso che mandi un messaggio a se stesso
                    AllReceivedNodesNoDistanceNoSelf = lists:filter(
                        fun({Pid, _}) -> Pid /= ParentPID end,
                        AllReceivedNodesNoDistance
                    ),

                    %Qui prendo la lista di tutti i nodi che sono stati trovati
                    %durante le iterazioni e li aggiungo ai kbuckets del
                    %nodo che ha avviato la funzione (ParentNode)
                    gen_server:cast(
                        ParentPID,
                        {add_nodes_to_kbuckets, AllReceivedNodesNoDistanceNoSelf}
                    ),

                    find_value_iterative(
                        lists:sublist(AllReceivedNodesNoDistanceNoSelf, ?A),
                        Key,
                        ParentNode,
                        AllReceivedNodes
                    )
            end
    end.

receive_responses_find_node(0, Responses) ->
    lists:flatten(Responses);
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
            receive_responses_find_node(N - 1, [Responses]);
        _ ->
            receive_responses_find_value(N - 1, [Responses])
    after 2000 ->
        %Se va in timeout ignoro e vado avanti
        receive_responses_find_node(N - 1, [Responses])
    end.

find_node_spawn(AlphaClosestNodes, Key, ParentNode, ParentPID) ->
    %Creo la lista di soli pid dalla lista di tuple
    AlphaClosestNodesPID = lists:map(fun({PIDNodo, _}) -> PIDNodo end, AlphaClosestNodes),

    % Eseguo la spawn di tanti processi quanti sono gli elementi della lista AlphaClosestNodesPID
    lists:foreach(
        fun(PIDNodo) ->
            spawn(fun() ->
                RequestId = generate_requestId(),
                Response =
                    (catch gen_server:call(
                        PIDNodo, {find_node, Key, ParentNode, RequestId}, 2000
                    )),
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
    receive_responses_find_node(length(AlphaClosestNodesPID), []).

find_value_iterative(AlphaClosestNodes, Key, ParentNode) ->
    find_value_iterative(AlphaClosestNodes, Key, ParentNode, []).
find_value_iterative(AlphaClosestNodes, Key, ParentNode, BestNodes) ->
    ParentPID = self(),
    case BestNodes == [] of
        true ->
            %faccio la spawn dei nodi e ritorno la lista dei nodi ricevuti o una risposta find_value
            Responses = find_value_spawn(AlphaClosestNodes, Key, ParentNode, ParentPID),

            %se ho trovato il valore ritorno direttamente il valore altrimenti
            %come find_node
            case lists:any(fun({Value, _}) -> Value == found_value end, Responses) of
                true ->
                    Value = hd(
                        lists:filter(fun({Value, _}) -> Value == found_value end, Responses)
                    ),
                    Value;
                _ ->
                    %Aggiungo anche i nodi che ho interrogato
                    ReceivedNodesAndTriedNodes = Responses ++ AlphaClosestNodes,

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

                    %Qui aggiorno i kbuckets del nodo che ha avviato la funzione (Parent)
                    %dato che sono nel caso iniziale, passo come argomento solo i nodi migliori
                    %che ho appena trovato (NewBestNodes), dato che non ne ho altri
                    gen_server:cast(ParentPID, {add_nodes_to_kbuckets, NewBestNodes}),

                    %Ora interrogo alpha nodi che prendo dalla lista di nodi che ho ricavato
                    find_value_iterative(
                        lists:sublist(NewBestNodesNoSelf, ?A),
                        Key,
                        ParentNode,
                        NewBestNodesWithDistance
                    )
            end;
        %in questo caso ho dei best nodes da confrontare con quelli che tireranno fuori
        %i processi alla prossima iterazione
        _ ->
            %faccio la spawn dei nodi e ritorno la lista dei nodi ricevuti o una risposta find_value
            Responses = find_value_spawn(AlphaClosestNodes, Key, ParentNode, ParentPID),

            %qui faccio l'if, se trovo il nodo termino altrimenti come find_node
            case lists:any(fun({Value, _}) -> Value == found_value end, Responses) of
                true ->
                    Value = hd(
                        lists:filter(fun({Value, _}) -> Value == found_value end, Responses)
                    ),
                    Value;
                _ ->
                    %Qui aggiungo anche i nodi che ho interrogato
                    ReceivedNodesAndTriedNodes = Responses ++ AlphaClosestNodes,

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
                            lists:sublist(BestNodes, ?K);
                        %ho trovato nodi mai trovati prima, continuo con l'iterazione
                        _ ->
                            %aggiungo i nuovi nodi trovati alla lista di tutti i nodi
                            %trovati, tolgo i doppioni e riordino la lista
                            AllReceivedNodesWithDuplicates =
                                NewBestNodesWithDistance ++ BestNodes,
                            AllReceivedNodesNosort = ordsets:to_list(
                                ordsets:from_list(AllReceivedNodesWithDuplicates)
                            ),
                            AllReceivedNodes = lists:sort(
                                fun({_, _, Dist1}, {_, _, Dist2}) -> Dist1 < Dist2 end,
                                AllReceivedNodesNosort
                            ),
                            AllReceivedNodesNoDistance = lists:map(
                                fun({PIDNodo, IdNodo, _}) -> {PIDNodo, IdNodo} end,
                                AllReceivedNodes
                            ),
                            %In caso tolgo il pid che sta facendo l'iterazione, perché non
                            %ha senso che mandi un messaggio a se stesso
                            AllReceivedNodesNoDistanceNoSelf = lists:filter(
                                fun({Pid, _}) -> Pid /= ParentPID end,
                                AllReceivedNodesNoDistance
                            ),

                            %Qui prendo la lista di tutti i nodi che sono stati trovati
                            %durante le iterazioni e li aggiungo ai kbuckets del
                            %nodo che ha avviato la funzione (ParentNode)
                            gen_server:cast(
                                ParentPID,
                                {add_nodes_to_kbuckets, AllReceivedNodesNoDistanceNoSelf}
                            ),

                            find_value_iterative(
                                lists:sublist(AllReceivedNodesNoDistanceNoSelf, ?A),
                                Key,
                                ParentNode,
                                AllReceivedNodes
                            )
                    end
            end
    end.

receive_responses_find_value(0, Responses) ->
    % Ho ricevuto tutte le risposte, ritorno la lista flatten
    lists:flatten(Responses);
receive_responses_find_value(N, Responses) ->
    receive
        {found_nodes, Response} ->
            case Response of
                {found_nodes, NodeList, _} ->
                    receive_responses_find_value(N - 1, [NodeList | Responses]);
                _ ->
                    receive_responses_find_value(N - 1, [Responses])
            end;
        {found_value, Response} ->
            case Response of
                %Se la risposta è del tipo giusto aggiungo il valore alla lista di risposte
                {found_value, Value, _} ->
                    receive_responses_find_value(N - 1, [{found_value, Value} | Responses]);
                _ ->
                    receive_responses_find_value(N - 1, [Responses])
            end;
        {invalid_requestid, _} ->
            receive_responses_find_value(N - 1, [Responses]);
        _ ->
            receive_responses_find_value(N - 1, [Responses])
    after 2000 ->
        %Se va in timeout ignoro e vado avanti
        receive_responses_find_value(N - 1, [Responses])
    end.

find_value_spawn(AlphaClosestNodes, Key, ParentNode, ParentPID) ->
    %Creo la lista di soli pid dalla lista di tuple
    AlphaClosestNodesPID = lists:map(fun({PIDNodo, _}) -> PIDNodo end, AlphaClosestNodes),

    % Eseguo la spawn di tanti processi quanti sono gli elementi della lista AlphaClosestNodesPID
    lists:foreach(
        fun(PIDNodo) ->
            spawn(fun() ->
                RequestId = generate_requestId(),
                Response =
                    (catch gen_server:call(
                        PIDNodo, {find_value, Key, ParentNode, RequestId}, 2000
                    )),
                %Controllo il requestId, in caso trova nodi ma il request id è sbagliato
                %allora manda invalid_request id, negli altri casi manda la risposta al
                % nodo che ha avviato la ricerca
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
            end)
        end,
        AlphaClosestNodesPID
    ),
    receive_responses_find_value(length(AlphaClosestNodesPID), []).

%mando un messaggio al nodo che deve coordinare la routine iterativa
%della ricerca, comando per la shell in modo da testare il nodo
start_find_node_iterative(NodePID, Key) ->
    RequestId = generate_requestId(),
    case
        (catch gen_server:call(
            NodePID, {start_find_node_iterative, Key, RequestId}, 2000
        ))
    of
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
    case
        (catch gen_server:call(
            NodePID, {start_find_value_iterative, Key, RequestId}, 2000
        ))
    of
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

calcola_tempo_totale_find_value(NumNodesToKillPerIter, Key, NumNodes) ->
    calcola_tempo_totale_find_value(NumNodesToKillPerIter, Key, NumNodes, []).

calcola_tempo_totale_find_value(NumNodesToKillPerIter, Key, NumNodes, NodesKilled) ->
    %creo la lista con i nomi dei nodi
    ListaNodi = [
        {N, list_to_atom("knode_" ++ integer_to_list(N))}
     || N <- lists:seq(1, NumNodes)
    ],
    case NodesKilled of
        [] ->
            start_nodes(NumNodes),
            %aggiungo il valore ad un nodo a caso poi aspetto che facciano la republish 10 volte
            gen_server:cast(
                list_to_atom("knode_" ++ integer_to_list(rand:uniform(NumNodes))), {store, 123}
            ),
            timer:sleep((?T * 1000) * 2),

            %faccio eseguire la find_value_iterative a tutti i nodi
            start_routine_find_value_iterative(ListaNodi, Key),
            %Prendo dei nodi a caso e li uccido, il numero di questo nodi a caso viene scelto da NodesToKillPerIteration
            NodesToKill = select_random_tuples(ListaNodi, NumNodesToKillPerIter),
            %io:format("NodesToKill:~p~n", [NodesToKill]),
            lists:foreach(
                fun({Num, _}) ->
                    gen_server:cast(list_to_atom("knode_" ++ integer_to_list(Num)), stop)
                end,
                NodesToKill
            ),

            calcola_tempo_totale_find_value(
                NumNodesToKillPerIter, Key, NumNodes, NodesKilled ++ NodesToKill
            );
        _ ->
            %aspetto che rieseguono la republish
            timer:sleep((?T * 1000) + 500),

            %io:format("NodesKilled: ~p~n", [NodesKilled]),
            %prendo i nodi che non sono nella lista dei NodesKilled
            SurvidedNodes = ListaNodi -- NodesKilled,
            %io:format("SurvidedNodes: ~p~n", [SurvidedNodes]),

            %faccio la routine della find_value_iterative
            start_routine_find_value_iterative(SurvidedNodes, Key),

            case length(SurvidedNodes) =< NumNodesToKillPerIter of
                %i nodi da uccidere sono di più o uguali ai nodi rimasti
                true ->
                    case length(SurvidedNodes) == 1 of
                        true ->
                            ok;
                        _ ->
                            %seleziono e uccido tutti i nodi tranne uno (il primo)
                            NodesToKill = tl(SurvidedNodes),
                            %io:format("NodesToKill: ~p~n", [NodesToKill]),
                            lists:foreach(
                                fun({Num, _}) ->
                                    gen_server:cast(
                                        list_to_atom("knode_" ++ integer_to_list(Num)), stop
                                    )
                                end,
                                NodesToKill
                            ),
                            %eseguo per l'ultima volta la routine per i tempi dato che la eseguo ad un nodo solo
                            %io:format("Primo nodo sopravvissuto: ~p~n", [[hd(SurvidedNodes)]]),
                            start_routine_find_value_iterative([hd(SurvidedNodes)], Key)
                    end;
                _ ->
                    %riprendo dei nodi a caso e li uccido
                    NodesToKill = select_random_tuples(SurvidedNodes, NumNodesToKillPerIter),
                    lists:foreach(
                        fun({Num, _}) ->
                            gen_server:cast(list_to_atom("knode_" ++ integer_to_list(Num)), stop)
                        end,
                        NodesToKill
                    ),

                    calcola_tempo_totale_find_value(
                        NumNodesToKillPerIter, Key, NumNodes, NodesKilled ++ NodesToKill
                    )
            end
    end.

start_routine_find_value_iterative(ListaNodi, Key) ->
    NodeListWithTimeResult = lists:foldl(
        fun({Index, NameNode}, Acc) ->
            Inizio = erlang:monotonic_time(microsecond),
            Risultato = start_find_value_iterative(NameNode, Key),
            Fine = erlang:monotonic_time(microsecond),
            Tempo = Fine - Inizio,
            Acc ++ [{Index, NameNode, Tempo, Risultato}]
        end,
        [],
        ListaNodi
    ),
    Tempi = [Tempo || {_, _, Tempo, _} <- NodeListWithTimeResult],
    %io:format("Tempi: ~p, su ~p nodi totali~n", [Tempi, length(ListaNodi)]),
    Media = round(lists:sum(Tempi) / length(Tempi)),
    io:format("Media dei tempi di lookup di tutti i nodi: ~p, su ~p nodi totali~n", [
        Media, length(ListaNodi)
    ]),

    ListaNodiFound = lists:filter(
        fun
            ({_, _, _, {found_value, _, _}}) -> true;
            ({_, _, _, _}) -> false
        end,
        NodeListWithTimeResult
    ),

    case ListaNodiFound of
        [] ->
            io:format("Nessun nodo ha trovato il valore su ~p nodi totali ~n", [length(ListaNodi)]);
        _ ->
            Percentuale =
                round((length(ListaNodiFound) / length(NodeListWithTimeResult)) * 100 * 10) / 10,
            ListaTempiNodiFound = lists:map(fun({_, _, Tempo, _}) -> Tempo end, ListaNodiFound),
            MediaFound = round(lists:sum(ListaTempiNodiFound) / length(ListaTempiNodiFound)),

            io:format("Percentuale nodi che hanno trovato il valore: ~p, su ~p nodi totali ~n", [
                Percentuale, length(ListaNodi)
            ]),

            io:format(
                "Media dei tempi di lookup dei nodi che hanno trovato il valore: ~p (~p su ~p nodi totali)~n",
                [
                    MediaFound, length(ListaNodiFound), length(ListaNodi)
                ]
            ),
            io:format("~n")
    end.

select_random_tuples(List, Num) ->
    select_random_tuples(List, Num, []).

select_random_tuples([], _, Acc) ->
    lists:reverse(Acc);
select_random_tuples(List, Num, Acc) when Num > 0 ->
    Index = rand:uniform(length(List)),
    {Selected, _} = lists:split(Index, List),
    select_random_tuples(List -- [lists:last(Selected)], Num - 1, [lists:last(Selected)] ++ Acc);
select_random_tuples(_, 0, Acc) ->
    lists:reverse(Acc).
