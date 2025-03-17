-module(knode).
-behaviour(gen_server).
-import(iterative_search, [find_node_iterative/3, find_value_iterative/3]).
-include_lib("stdlib/include/ms_transform.hrl").
-include("parameters.hrl").

-export([
    start_link/1, start_link/2, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2
]).
-export([
    stop/1,
    ping/1,
    store_value/2,
    aggiungi_distanza/2,
    generate_requestId/0,
    find_node_for_spawn/3,
    find_value_for_spawn/3,
    add_nodes_to_kbuckets_cast/2
]).

start_link(Name) ->
    start_link(Name, undefined).
start_link(Name, BootstrapNodePID) ->
    gen_server:start_link({local, Name}, ?MODULE, BootstrapNodePID, []).

init(BootstrapNodePID) ->
    Id = generate_node_id(),
    % io:format("Nodo kademlia con PID ~p, ID ~p e argomento ~p in avvio...~n", [
    %     self(), Id, BootstrapNodePID
    % ]),
    StoreTable = ets:new(kademlia_store, [set]),
    KBuckets = ets:new(buckets, [set]),
    % Inizializza la tabella KBuckets
    initialize_kbuckets(KBuckets),
    start_periodic_republish(self()),
    NewState = {Id, StoreTable, KBuckets},
    case is_pid(BootstrapNodePID) of
        % Mi connetto al nodo bootstrap
        true ->
            %io:format("Nodo ~p prova a connettersi a bootstrap ~p~n", [Id, BootstrapNodePID]),
            join_request(BootstrapNodePID, Id, NewState);
        % Sono il nodo bootstrap
        _ ->
            %io:format("Nodo ~p diventato bootstrap~n", [Id]),
            {ok, NewState}
    end.
handle_call({ping, RequestId}, _From, {Id, StoreTable, KBuckets}) ->
    % {PIDFrom, _} = _From,
    % io:format("Nodo con PID ~p e ID ~p ha ricevuto ping da ~p~n", [self(), Id, PIDFrom]),
    {reply, {pong, self(), RequestId}, {Id, StoreTable, KBuckets}};
handle_call(
    {find_node, ToFindNodeId, ParentNode, RequestId}, _From, {Id, StoreTable, KBuckets}
) ->
    %ParentNode perché se prendessi le informazioni da _From sarebbero
    %le informazioni del processo generato da spawn e non del nodo
    %che invia la richiesta direttamente
    %provo ad aggiungere il nodo che mi ha inviato la richiesta ai miei kbuckets
    %mandandomi un messaggio cast
    add_nodes_to_kbuckets_cast(self(), [ParentNode]),

    % Recupero i k-buckets dalla tabella ETS
    KBucketsList = ets:tab2list(KBuckets),
    % Cerco i nodi più vicini nei miei kbuckets
    ClosestNodes = find_closest_nodes(ToFindNodeId, KBucketsList),
    % Rispondo al nodo richiedente con la lista dei nodi più vicini
    {reply, {found_nodes, ClosestNodes, RequestId}, {Id, StoreTable, KBuckets}};
handle_call(
    {find_value, Key, ParentNode, RequestId}, _From, {Id, StoreTable, KBuckets}
) ->
    %Aggiungo il ParentNode nei miei kbuckets
    add_nodes_to_kbuckets_cast(self(), [ParentNode]),

    case ets:lookup(StoreTable, Key) of
        [{Key, Value}] ->
            % Il nodo ha il valore, lo restituisce
            {reply, {found_value, Value, RequestId}, {Id, StoreTable, KBuckets}};
        [] ->
            % Il nodo non ha il valore, restituisce i nodi più vicini come find_node
            KBucketsList = ets:tab2list(KBuckets),
            ClosestNodes = find_closest_nodes(Key, KBucketsList),
            {reply, {found_nodes, ClosestNodes, RequestId}, {Id, StoreTable, KBuckets}}
    end;
handle_call(
    {join_request, IdNewNode, RequestId}, _From, {Id, StoreTable, KBuckets}
) ->
    {PidNewNode, _} = _From,
    %io:format("Sono il Nodo ~p, ho ricevuto join_request da ~p~n", [Id, PID]),

    %mando un messaggio a me stesso per aggiungere in modo asincrono il nodo ai kbuckets
    add_nodes_to_kbuckets_cast(self(), [{PidNewNode, IdNewNode}]),

    % invia la lista di nodi più vicini a lui
    KBucketsList = ets:tab2list(KBuckets),
    ClosestNodes = find_closest_nodes(IdNewNode, KBucketsList),
    {reply, {k_buckets, ClosestNodes, Id, RequestId}, {Id, StoreTable, KBuckets}};
handle_call(
    {start_find_node_iterative, Key, RequestId}, _From, {Id, StoreTable, KBuckets}
) ->
    %Prendo dai miei kbuckets la lista dei k nodi più vicini alla chiave
    KBucketsList = ets:tab2list(KBuckets),
    ClosestNodes = find_closest_nodes(Key, KBucketsList),
    %Scelgo alpha nodi da interrogare in parallelo
    AlphaClosestNodes = lists:sublist(ClosestNodes, ?A),
    %controllo alpha
    case AlphaClosestNodes of
        [] ->
            {reply, {empty_kbuckets, [], RequestId}, {Id, StoreTable, KBuckets}};
        _ ->
            %ParentNode utile per dare l'informazione del nodo padre ai nodi riceventi
            %in modo che possono aggiungere questo nodo ai propri kbuckets
            ParentNode = {self(), Id},

            %Quindi faccio partire la funzione find_node_iterative
            IterativeNodesFound = find_node_iterative(AlphaClosestNodes, Key, ParentNode),

            {reply, {founded_nodes_from_iteration, IterativeNodesFound, RequestId},
                {Id, StoreTable, KBuckets}}
    end;
handle_call(
    {start_find_value_iterative, Key, RequestId}, _From, {Id, StoreTable, KBuckets}
) ->
    case ets:lookup(StoreTable, Key) of
        % Il nodo ha il valore, lo restituisce
        [{Key, Value}] ->
            {reply, {found_value, Value, RequestId}, {Id, StoreTable, KBuckets}};
        % Il nodo non ha il valore, inizia la ricerca iterativa
        [] ->
            %Prendo dai miei kbuckets la lista dei k nodi più vicini alla chiave
            KBucketsList = ets:tab2list(KBuckets),
            ClosestNodes = find_closest_nodes(Key, KBucketsList),
            %Scelgo alpha nodi da interrogare in parallelo
            AlphaClosestNodes = lists:sublist(ClosestNodes, ?A),

            case AlphaClosestNodes of
                [] ->
                    {reply, {empty_kbuckets, [], RequestId}, {Id, StoreTable, KBuckets}};
                _ ->
                    %Quindi faccio partire la funzione find_value_iterative
                    ParentNode = {self(), Id},

                    IterativeFound = find_value_iterative(AlphaClosestNodes, Key, ParentNode),

                    case IterativeFound of
                        {found_value, Value} ->
                            {reply, {found_value, Value, RequestId}, {Id, StoreTable, KBuckets}};
                        _ ->
                            {reply, {value_not_found, IterativeFound, RequestId},
                                {Id, StoreTable, KBuckets}}
                    end
            end
    end;
handle_call(_Request, _From, State) ->
    %io:format("Received unknown request: ~p~n", [_Request]),
    {reply, {error, unknown_request}, State}.

handle_cast({store, Value}, {Id, StoreTable, KBuckets}) ->
    %controllo se è un intero, se non lo è non inserisco nulla
    case is_integer(Value) of
        true ->
            %Calcola la key
            HashValue = crypto:hash(sha, integer_to_binary(Value)),
            Key = binary_to_integer_representation(HashValue),
            % Inserisci la tupla nella tabella ETS
            ets:insert(StoreTable, {Key, Value}),
            {noreply, {Id, StoreTable, KBuckets}};
        _ ->
            {noreply, {Id, StoreTable, KBuckets}}
    end;
handle_cast({store, Key, Value}, {Id, StoreTable, KBuckets}) ->
    % Inserisci la tupla nella tabella ETS
    ets:insert(StoreTable, {Key, Value}),
    {noreply, {Id, StoreTable, KBuckets}};
handle_cast(republish_data_refresh_kbuckets, {Id, StoreTable, KBuckets}) ->
    %io:format("Received republish message, sono il nodo con il pid:~p~n", [self()]),
    republish_data(StoreTable, KBuckets),
    %refresh_kbuckets(Id, KBuckets),
    start_periodic_republish(self()),
    {noreply, {Id, StoreTable, KBuckets}};
handle_cast({add_nodes_to_kbuckets, NodesToAdd}, {Id, StoreTable, KBuckets}) ->
    add_nodes_to_kbuckets(Id, NodesToAdd, KBuckets),
    {noreply, {Id, StoreTable, KBuckets}};
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

stop(PID) ->
    gen_server:cast(PID, stop).

store_value(PID, Value) ->
    gen_server:cast(PID, {store, Value}).

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
            {error}
    end.

find_node_for_spawn(PID, Key, ParentNode) ->
    {ParentPID, _} = ParentNode,
    RequestId = generate_requestId(),
    Response = catch gen_server:call(PID, {find_node, Key, ParentNode, RequestId}, 2000),
    %Invio la risposta al processo principale
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
        {'EXIT', Reason} ->
            ParentPID ! {error, Reason};
        _ ->
            ParentPID ! {error, Response}
    end.

find_value_for_spawn(PIDNodo, Key, ParentNode) ->
    {ParentPID, _} = ParentNode,
    RequestId = generate_requestId(),
    Response =
        (catch gen_server:call(
            PIDNodo, {find_value, Key, ParentNode, RequestId}, 2000
        )),
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
        {'EXIT', Reason} ->
            ParentPID ! {error, Reason};
        _ ->
            ParentPID ! {error, Response}
    end.

join_request(BootstrapNodePID, Id, NewState) ->
    RequestId = generate_requestId(),
    {Id, _, KBuckets} = NewState,
    case catch gen_server:call(BootstrapNodePID, {join_request, Id, RequestId}, 2000) of
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
    end.

add_nodes_to_kbuckets_cast(PID, NodesToAdd) ->
    gen_server:cast(PID, {add_nodes_to_kbuckets, NodesToAdd}).

generate_node_id() ->
    % Genero un intero casuale grande (64 bit)
    RandomInteger = rand:uniform(18446744073709551615),
    % Applico la funzione hash SHA-1
    HashValue = crypto:hash(sha, integer_to_binary(RandomInteger)),
    % Converto l'id in binario grezzo in un intero decimale
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
            % Inserisco l'intervallo vuoto nella tabella KBuckets
            ets:insert(KBuckets, {{LowerBound, UpperBound}, []})
        end,
        % Genero una lista di 160 elementi
        lists:seq(0, 159)
    ).

calcola_distanza(Id1, Id2) -> Id1 bxor Id2.

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
                            %controllo se è già presente
                            case lists:keyfind(NodeToAddId, 2, CurrentNodes) of
                                %Non trovato
                                false ->
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
                                                        tl(CurrentNodes) ++
                                                            [{NodeToAddPid, NodeToAddId}]
                                                end;
                                            _ ->
                                                % Aggiungi il nuovo nodo alla lista
                                                CurrentNodes ++ [{NodeToAddPid, NodeToAddId}]
                                        end,
                                    % Aggiorna la tabella ETS con i nuovi nodi nel bucket
                                    ets:insert(MyKBuckets, {Key, NewNodes});
                                % Già presente
                                _ ->
                                    %il nodo viene cancellato dalla lista e poi viene messo in coda
                                    lists:delete({NodeToAddPid, NodeToAddId}, CurrentNodes),
                                    CurrentNodes ++ [{NodeToAddPid, NodeToAddId}]
                            end;
                        _ ->
                            {error}
                    end
            end
        end,
        NodesListToAdd
    ).
