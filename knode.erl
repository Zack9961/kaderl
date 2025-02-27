-module(knode).
-behaviour(gen_server).
-include_lib("stdlib/include/ms_transform.hrl").
-define(K, 20).
-define(T, 30).
-export([start_link/2, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).
-export([
    stop/0,
    get_id/1,
    read_store/0,
    find_node/3,
    find_value/3,
    ping/1,
    find_value_iterative/2,
    reiterate_find_node/2,
    reiterate_find_node/4,
    start_nodes/1,
    calcola_tempo_totale/1
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
    NewState = {Id, 0, State, StoreTable, KBuckets},
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
                {'EXIT', Reason} ->
                    % Nodo bootstrap non raggiungibile, divento bootstrap
                    % io:format("Nodo bootstrap ~p non raggiungibile (~p), divento bootstrap~n", [
                    %     BootstrapNodePID, Reason
                    % ]),
                    {ok, NewState};
                {k_buckets, BucketsReceived, BootstrapID, RequestIdReceived} ->
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
                            add_nodes_to_kbuckets(Id, BucketsReceived, KBuckets),
                            {ok, NewState};
                        _ ->
                            % io:format("Sono il nodo con pid: ~p Request Id non corretto~n", [
                            %     BootstrapNodePID
                            % ]),
                            {ok, NewState}
                    end
            end
        % case catch get_id(BootstrapNodePID) of
        %     {'EXIT', Reason} ->
        %         % Nodo bootstrap non raggiungibile, divento bootstrap
        %         io:format("Nodo bootstrap ~p non raggiungibile (~p), divento bootstrap~n", [
        %             BootstrapNodePID, Reason
        %         ]),
        %         {ok, NewState};
        %     BootstrapID ->
        %         % Nodo bootstrap raggiungibile, procedo normalmente
        %         io:format("Nodo bootstrap ~p risponde, ID: ~p~n", [
        %             BootstrapNodePID, BootstrapID
        %         ]),
        %         adding_bootstrap_node_in_kbuckets(BootstrapNodePID, KBuckets, Id, BootstrapID),
        %         %gen_server:cast({join_request, self(), Id}),
        %         %RequestId = rand:uniform(18446744073709551615),
        %         %BootstrapNodePID ! {join_request, self(), Id},
        %         RispostaBuckets = gen_server:call(
        %             BootstrapNodePID, {join_request, Id, generate_requestId()}
        %         ),
        %         {ok, NewState}
        % end
    end.
% handle_call(
%     {ping_from_shell, IdToPing}, _From, {Id, Counter, State, StoreTable, KBuckets}
% ) ->
%     {PID, _} = _From,
%     ping(IdToPing),
%     io:format("Node ~p (~p) received ping_from_shell from ~p~n", [self(), Id, PID]),
%     {reply, ok, {Id, Counter, State, StoreTable, KBuckets}};
% handle_call({ping, Id, RequestId}, _From, {Id, Counter, State, StoreTable, KBuckets}) ->
%     {PID, _} = _From,
%     %Cerco nella mia tabella ets il pid
%     %Una volta trovato faccio partire la funzione ping(PID)
%     io:format("Node ~p (~p) received ping from ~p~n", [self(), Id, PID]),
handle_call({ping, RequestId}, _From, {Id, Counter, State, StoreTable, KBuckets}) ->
    {PID, _} = _From,
    %io:format("Node ~p (~p) received ping from ~p~n", [self(), Id, PID]),
    {reply, {pong, self(), RequestId}, {Id, Counter, State, StoreTable, KBuckets}};
handle_call(get_id, _From, {Id, Counter, State, StoreTable, KBuckets}) ->
    {reply, Id, {Id, Counter, State, StoreTable, KBuckets}};
%Funzione cancellabile?
handle_call({read_store, RequestId}, _From, {Id, Counter, State, StoreTable, KBuckets}) ->
    %io:format("Received read_store request from ~p,~p~n", [_From, Id]),
    % Leggi la tabella ETS
    Tuples = ets:tab2list(StoreTable),
    %io:format("Table content: ~p~n", [Tuples]),
    {reply, {Tuples, RequestId}, {Id, Counter, State, StoreTable, KBuckets}};
handle_call(
    {find_node, ToFindNodeId, RequestId}, _From, {Id, Counter, State, StoreTable, KBuckets}
) ->
    {PID, _} = _From,
    %io:format("Node ~p (~p) received FIND_NODE request for ID ~p from ~p~n", [
    %    self(), Id, ToFindNodeId, PID
    %]),
    % 1. Recupera i k-bucket dalla tabella ETS
    KBucketsList = ets:tab2list(KBuckets),
    % 2. Implementa la logica per trovare i nodi più vicini
    ClosestNodes = find_closest_nodes(ToFindNodeId, KBucketsList),
    % 3. Rispondi al nodo richiedente con la lista dei nodi più vicini
    {reply, {found_nodes, ClosestNodes, RequestId}, {Id, Counter, State, StoreTable, KBuckets}};
handle_call({find_value, Key, RequestId}, _From, {Id, Counter, State, StoreTable, KBuckets}) ->
    {PID, _} = _From,
    %io:format("Node ~p (~p) received FIND_VALUE request for Key ~p from pid: ~p~n", [
    %    self(), Id, Key, PID
    %]),
    %[{Key, Value}] = ets:lookup(StoreTable, Key),
    case ets:lookup(StoreTable, Key) of
        [{Key, Value}] ->
            % Il nodo ha il valore, lo restituisce
            %io:format("Nodo ~p trova il valore ~p", [self(), Key]),
            {reply, {found_value, Value, RequestId}, {Id, Counter, State, StoreTable, KBuckets}};
        %PID ! {found_value, Value};
        [] ->
            % Il nodo non ha il valore, restituisce i nodi più vicini
            KBucketsList = ets:tab2list(KBuckets),
            ClosestNodes = find_closest_nodes(Key, KBucketsList),
            {reply, {found_nodes, ClosestNodes, RequestId},
                {Id, Counter, State, StoreTable, KBuckets}}
        %PID ! {found_nodes, ClosestNodes}
    end;
handle_call(
    {join_request, IdNewNode, RequestId}, _From, {Id, Counter, State, StoreTable, KBuckets}
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
    UpdatedNodes = CurrentNodes ++ [{PID, IdNewNode}],

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
    {reply, {k_buckets, KBuckets, Id, RequestId}, {Id, Counter, State, StoreTable, KBuckets}};
handle_call(_Request, _From, State) ->
    %io:format("Received unknown request: ~p~n", [_Request]),
    {reply, {error, unknown_request}, State}.

handle_cast({store, Value}, {Id, Counter, State, StoreTable, KBuckets}) ->
    %io:format("Received store request: Id=~p, Value=~p, From=?~n", [Id, Value]),
    % Incrementa il contatore
    NewCounter = Counter + 1,
    %Calcola la key
    HashValue = crypto:hash(sha, integer_to_binary(Value)),
    Key = binary_to_integer_representation(HashValue),
    % Inserisci la tupla nella tabella ETS
    ets:insert(StoreTable, {Key, Value}),
    %io:format("Inserted in ETS: Key=~p, Value=~p~n", [Key, Value]),
    % Rispondi al client
    {noreply, {Id, NewCounter, State, StoreTable, KBuckets}};
handle_cast({store, Key, Value}, {Id, Counter, State, StoreTable, KBuckets}) ->
    %io:format("Received store request: Id=~p Key=~p, Value=~p, From=?~n", [Id, Key, Value]),
    % Incrementa il contatore
    NewCounter = Counter + 1,
    % Inserisci la tupla nella tabella ETS
    ets:insert(StoreTable, {Key, Value}),
    %io:format("Inserted in ETS: Key=~p, Value=~p~n", [Key, Value]),
    % Rispondi al client
    {noreply, {Id, NewCounter, State, StoreTable, KBuckets}};
handle_cast(republish, {Id, Counter, State, StoreTable, KBuckets}) ->
    io:format("Received republish message, sono il nodo con il pid:~p~n", [self()]),
    % 1. Ripubblica i dati
    republish_data(StoreTable, KBuckets),
    % 2. Reimposta il timer per la prossima ripubblicazione
    start_periodic_republish(self()),
    {noreply, {Id, Counter, State, StoreTable, KBuckets}};
handle_cast(stop, State) ->
    {stop, normal, State};
handle_cast(_Msg, State) ->
    io:format("Received cast message: ~p~n", [_Msg]),
    {noreply, State}.

handle_info(_Info, State) ->
    io:format("Received info message: ~p~n", [_Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    io:format("Kademlia node terminating~n"),
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
                    io:format("Node with PID ~p received pong from ~p~n", [self(), ReceiverPID]);
                _ ->
                    io:format("Sono il nodo con pid ~p, requestId ricevuto da ~p non corretto ~n", [
                        self(), ReceiverPID
                    ])
            end;
        {'EXIT', Reason} ->
            io:format("Nodo con pid ~p non raggiungibile (~p)~n", [PID, Reason]);
        _ ->
            io:format("Risposta non gestita")
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

find_value(PID, Key, RequestId) ->
    gen_server:call(PID, {find_value, Key, RequestId}).

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
            %         A = 1
            % end,

            % 5. Invia una richiesta STORE a ciascuno dei k nodi più vicini.
            lists:foreach(
                fun({NodePID, NodeId}) ->
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
%io:format("Ripubblicazione dati completata per il nodo con pid: ~p\n", [self()]).

add_nodes_to_kbuckets(Id, BucketsReceived, MyKBuckets) ->
    %io:format("Nodo ~p ricevuto k_buckets ~p~n", [Id, BucketsReceived]),
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

                                    % Aggiungi il nuovo nodo alla lista (se non è già presente)
                                    NewNodes =
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
                                    ets:insert(MyKBuckets, {Key, NewNodesLimited})
                            end
                    end
                end,
                Nodes
            )
        end,
        BucketsReceivedList
    ).

find_value_iterative(NodePID, Key) ->
    find_value_iterative(NodePID, Key, [], []).

find_value_iterative(NodePID, Key, TriedNodes, ClosestNodes) ->
    %io:format("Interrogazione del nodo ~p per la chiave ~p\n", [NodePID, Key]),

    RequestId = generate_requestId(),
    case catch gen_server:call(NodePID, {find_value, Key, RequestId}, 2000) of
        {'EXIT', _} ->
            %io:format("Il nodo ~p non risponde\n", [NodePID]),
            handle_node_failure(NodePID, Key, TriedNodes, ClosestNodes);
        {found_value, Value, RequestId} ->
            %io:format("Valore ~p trovato sul nodo ~p\n", [Value, NodePID]),
            {ok, Value};
        {found_nodes, Nodes, RequestId} ->
            %Trasformare qui la lista di tuple in lista pid
            NodesPIDList = lists:map(
                fun({PIDNodo, _}) ->
                    PIDNodo
                end,
                Nodes
            ),
            NewNodes = lists:filter(fun(N) -> not lists:member(N, TriedNodes) end, NodesPIDList),
            case NewNodes of
                [] ->
                    %%Nessun nuovo nodo da interrogare
                    % io:format(
                    %     "Valore non trovato, nessun nuovo nodo da interrogare vicino alla chiave ~p\n",
                    %     [Key]
                    % ),
                    {error, not_found};
                % case ClosestNodes of
                %     [] ->
                %         %% Non ci sono nodi vicini, la ricerca fallisce
                %         io:format("Nessun nodo trovato vicino alla chiave ~p\n", [Key]),
                %         {error, not_found};
                %     _ ->
                %         io:format("Nessun valore trovato, ultimi nodi testati: ~p\n", [
                %             ClosestNodes
                %         ]),
                %         %io:format("Nodi piu' vicini trovati: ~p\n", [ClosestNodes]),
                %         {error, not_found}
                % end;
                _ ->
                    %%Interroga il nodo più vicino (il primo nella lista)
                    NextNodePID = hd(NewNodes),
                    find_value_iterative(NextNodePID, Key, [NodePID | TriedNodes], NewNodes)
            end;
        _ ->
            %io:format("Risposta inattesa dal nodo ~p\n", [NodePID]),
            handle_unexpected_response(NodePID, Key, TriedNodes, ClosestNodes)
    end.

handle_node_failure(NodePID, Key, TriedNodes, ClosestNodes) ->
    %io:format("Gestione del fallimento del nodo ~p\n", [NodePID]),
    case ClosestNodes of
        [] ->
            %io:format("Ricerca fallita per la chiave ~p\n", [Key]),
            {error, not_found};
        _ ->
            % Prova con un altro nodo dai nodi più vicini
            case tl(lists:filter(fun(N) -> not lists:member(N, TriedNodes) end, ClosestNodes)) of
                [] ->
                    % Nessun altro nodo disponibile
                    % io:format(
                    %     "Nessun altro nodo disponibile, ricerca fallita per la chiave ~p\n", [Key]
                    % ),
                    {error, not_found};
                [NextNode | _] ->
                    %io:format("Riprova con il nodo ~p\n", [NextNode]),
                    NextNodePID = NextNode,
                    find_value_iterative(NextNodePID, Key, [NodePID | TriedNodes], ClosestNodes)
            end
    end.

handle_unexpected_response(NodePID, Key, TriedNodes, ClosestNodes) ->
    % Implementa la logica per gestire una risposta inattesa da un nodo.
    % Puoi registrare l'evento, riprovare o segnalare un errore.
    %io:format("Gestione di una risposta inattesa dal nodo ~p\n", [NodePID]),
    case ClosestNodes of
        [] ->
            %io:format("Ricerca fallita per la chiave ~p\n", [Key]),
            {error, not_found};
        _ ->
            % Prova con un altro nodo dai nodi più vicini
            case tl(lists:filter(fun(N) -> not lists:member(N, TriedNodes) end, ClosestNodes)) of
                [] ->
                    % Nessun altro nodo disponibile
                    % io:format(
                    %     "Nessun altro nodo disponibile, ricerca fallita per la chiave ~p\n", [Key]
                    % ),
                    {error, not_found};
                [NextNode | _] ->
                    %io:format("Riprova con il nodo ~p\n", [NextNode]),
                    NextNodePID = NextNode,
                    find_value_iterative(NextNodePID, Key, [NodePID | TriedNodes], ClosestNodes)
            end
    end.

reiterate_find_node(Key, NodePID) ->
    reiterate_find_node(Key, NodePID, [], 0).

reiterate_find_node(Key, NodePID, BestNodes, Tries) ->
    case Tries > 10 of
        true ->
            {error, max_retries_reached};
        _ ->
            RequestId = generate_requestId(),
            case gen_server:call(NodePID, {find_node, Key, RequestId}, timer:seconds(2)) of
                {found_nodes, ClosestNodes, ReceivedRequestId} when is_list(ClosestNodes) ->
                    case RequestId == ReceivedRequestId of
                        true ->
                            case ClosestNodes of
                                [] ->
                                    % io:format(
                                    %     "Nessun nodo vicino trovato, ricerca conclusa.~n", []
                                    % ),
                                    {ok, BestNodes};
                                _ ->
                                    % Calcola le distanze dai nodi trovati alla chiave
                                    NodesWithDistance = lists:map(
                                        fun({Pid, Id}) ->
                                            Distance = calcola_distanza(Key, Id),
                                            {Pid, Id, Distance}
                                        end,
                                        ClosestNodes
                                    ),
                                    % Ordina i nodi per distanza crescente
                                    NewBestNodes = lists:sort(
                                        fun({_, _, D1}, {_, _, D2}) -> D1 < D2 end,
                                        NodesWithDistance
                                    ),

                                    case BestNodes == [] of
                                        %Caso base
                                        true ->
                                            [{_, _, FirstNewBestNodesDistance} | _] = NewBestNodes,
                                            case FirstNewBestNodesDistance == 0 of
                                                true ->
                                                    % Ferma l'iterazione
                                                    % io:format(
                                                    %     "Nodo con distanza 0 trovato, ricerca conclusa.~n"
                                                    % ),
                                                    NewBestNodesNoDistance = lists:map(
                                                        fun({PIDNodo, IdNodo, _}) ->
                                                            {PIDNodo, IdNodo}
                                                        end,
                                                        NewBestNodes
                                                    ),
                                                    {ok, NewBestNodesNoDistance};
                                                _ ->
                                                    % Se ci sono nodi migliori, continua l'iterazione
                                                    % io:format(
                                                    %     "Trovati nodi più vicini, si continua l'iterazione.~n"
                                                    % ),
                                                    [{NewNodePID, _, _} | _] = NewBestNodes,
                                                    %timer:sleep(1000),
                                                    reiterate_find_node(
                                                        Key, NewNodePID, NewBestNodes, Tries + 1
                                                    )
                                            end;
                                        _ ->
                                            [{_, _, FirstNewBestNodesDistance} | _] = NewBestNodes,
                                            [{_, _, FirstBestNodesDistance} | _] = BestNodes,
                                            % Confronta i nuovi nodi con i migliori precedenti
                                            case
                                                FirstNewBestNodesDistance < FirstBestNodesDistance
                                            of
                                                true ->
                                                    case FirstNewBestNodesDistance == 0 of
                                                        true ->
                                                            % Altrimenti, ferma l'iterazione
                                                            % io:format(
                                                            %     "Nodo con distanza 0 trovato, ricerca conclusa.~n"
                                                            % ),
                                                            NewBestNodesNoDistance = lists:map(
                                                                fun({PIDNodo, IdNodo, _}) ->
                                                                    {PIDNodo, IdNodo}
                                                                end,
                                                                NewBestNodes
                                                            ),
                                                            {ok, NewBestNodesNoDistance};
                                                        _ ->
                                                            % Se ci sono nodi migliori, continua l'iterazione
                                                            % io:format(
                                                            %     "Trovati nodi più vicini, si continua l'iterazione.~n"
                                                            % ),
                                                            [{NewNodePID, _, _} | _] = NewBestNodes,
                                                            %timer:sleep(1000),
                                                            reiterate_find_node(
                                                                Key,
                                                                NewNodePID,
                                                                NewBestNodes,
                                                                Tries + 1
                                                            )
                                                    end;
                                                _ ->
                                                    % Altrimenti, ferma l'iterazione
                                                    % io:format(
                                                    %     "Nessun nodo più vicino trovato, ricerca conclusa.~n"
                                                    % ),
                                                    %Elimino la distanza
                                                    BestNodesNoDistance = lists:map(
                                                        fun({PIDNodo, IdNodo, _}) ->
                                                            {PIDNodo, IdNodo}
                                                        end,
                                                        BestNodes
                                                    ),
                                                    {ok, BestNodesNoDistance}
                                            end
                                    end
                            end;
                        _ ->
                            %io:format("RequestId ricevuto non corretto~n"),
                            {ok, BestNodes}
                    end;
                {error, Reason} ->
                    %io:format("Errore durante find_node: ~p~n", [Reason]),
                    timer:sleep(1000),
                    reiterate_find_node(Key, NodePID, BestNodes, Tries + 1);
                Other ->
                    %io:format("Risposta non gestita da find_node: ~p~n", [Other]),
                    timer:sleep(1000),
                    reiterate_find_node(Key, NodePID, BestNodes, Tries + 1)
            end
    end.

%Funzione da testare per avere meno righe di codice
% reiterate_find_node2(Key, NodePID, BestNodes, Tries) ->
%     case Tries > 10 of
%         true ->
%             {error, max_retries_reached};
%         _ ->
%             RequestId = generate_requestId(),
%             case gen_server:call(NodePID, {find_node, Key, RequestId}, timer:seconds(2)) of
%                 {found_nodes, ClosestNodes, ReceivedRequestId} when
%                     RequestId == ReceivedRequestId
%                 ->
%                     NodesWithDistance = lists:map(
%                         fun({Pid, Id}) -> {Pid, Id, calcola_distanza(Key, Id)} end, ClosestNodes
%                     ),
%                     NewBestNodes = lists:sort(
%                         fun({_, _, D1}, {_, _, D2}) -> D1 < D2 end, NodesWithDistance
%                     ),
%                     case hd(NewBestNodes) of
%                         {_, _, 0} ->
%                             io:format("Nodo con distanza 0 trovato, ricerca conclusa.~n"),
%                             {ok, lists:map(fun({Pid, Id, _}) -> {Pid, Id} end, NewBestNodes)};
%                         _ ->
%                             case BestNodes == [] orelse hd(NewBestNodes) < hd(BestNodes) of
%                                 true ->
%                                     io:format(
%                                         "Trovati nodi più vicini, si continua l'iterazione.~n"
%                                     ),
%                                     [{NewNodePID, _, _} | _] = NewBestNodes,
%                                     reiterate_find_node(Key, NewNodePID, NewBestNodes, Tries + 1);
%                                 _ ->
%                                     io:format(
%                                         "Nessun nodo più vicino trovato, ricerca conclusa.~n"
%                                     ),
%                                     {ok, lists:map(fun({Pid, Id, _}) -> {Pid, Id} end, BestNodes)}
%                             end
%                     end;
%                 {error, Reason} ->
%                     io:format("Errore durante find_node: ~p~n", [Reason]),
%                     timer:sleep(1000),
%                     reiterate_find_node(Key, NodePID, BestNodes, Tries + 1);
%                 Other ->
%                     io:format("Risposta non gestita da find_node: ~p~n", [Other]),
%                     timer:sleep(1000),
%                     reiterate_find_node(Key, NodePID, BestNodes, Tries + 1)
%             end
%     end.

%% Avvia più nodi Kademlia
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

%% Funzione per misurare il tempo di lookup
% measure_lookup_time(NumLookups) ->
%     measure_lookup_time(NumLookups, 0, 0).

% measure_lookup_time(0, TotalTime, NumSuccess) ->
%     case NumSuccess of
%         0 ->
%             {error, "Nessun lookup completato con successo"};
%         _ ->
%             AverageTime = TotalTime / NumSuccess,
%             {ok, AverageTime}
%     end;
% measure_lookup_time(NumLookups, TotalTime, NumSuccess) ->
%     Key = generate_random_key(), % Genera una chiave casuale
%     {Time, Result} = timer:tc(fun() -> knode:lookup(Key) end), %Misura il tempo di esecuzione della lookup
%     case Result of
%         {ok, _Value} -> %Lookup avuto successo
%             measure_lookup_time(NumLookups - 1, TotalTime + Time, NumSuccess + 1);
%         {error, _Reason} -> %Gestisci l'errore, ad esempio nodo non trovato
%             io:format("Lookup fallito per la chiave ~p~n", [Key]),
%             measure_lookup_time(NumLookups - 1, TotalTime, NumSuccess) %Riprova senza sommare il tempo
%     end.

% measure_lookup_average_time(ValoreDaTrovare,TotalTime) ->
%     NumNodes = 500,
%     ListOfNodes = [list_to_atom("knode_" ++ integer_to_list(N)) || N <- lists:seq(1, NumNodes)],
%     lists:foreach(
%         fun(NomeNodo) ->
%             {Time, Result} = timer:tc(fun() ->
%                 knode:find_value_iterative(whereis(NomeNodo), ValoreDaTrovare)end),
%                 case Result of
%                     %Lookup avuto successo
%                     {ok, _Value} ->

%                     %Gestisci l'errore, ad esempio nodo non trovato
%                     {error, _Reason} ->
%                         io:format("Lookup fallito per la chiave ~p~n", [Key]),
%                         %Riprova senza sommare il tempo
%                 end

%         end,
%         ListOfNodes
%     ).

% calcola_tempo_totale(Key) ->
%     NumNodes = 500,
%     ListaNodi = [list_to_atom("knode_" ++ integer_to_list(N)) || N <- lists:seq(1, NumNodes)],
%     [ {Node, Tempo} ||
%       Node <- ListaNodi,
%       Inizio = erlang:monotonic_time(microsecond),
%       Risultato = find_value_iterative(Node, Key),
%       case Risultato of
%         {ok, _} ->
%           Tempo = erlang:monotonic_time(microsecond) - Inizio,
%           true;
%         _ ->
%           false
%       end ].

calcola_tempo_totale(Key) ->
    NumNodes = 500,
    ListaNodi = [list_to_atom("knode_" ++ integer_to_list(N)) || N <- lists:seq(1, NumNodes)],
    NodiETempi = [
        {Node, Tempo}
     || Node <- ListaNodi,
        {ok, _, Tempo} <- [calcola_tempo(Node, Key)]
    ],
    Tempi = [Tempo || {_, Tempo} <- NodiETempi],
    Media = lists:sum(Tempi) / length(Tempi),
    io:format("Media dei tempi: ~p~n", [Media]),
    lists:foreach(
        fun({Node, Tempo}) -> io:format("Nodo: ~p, Tempo: ~p~n", [Node, Tempo]) end, NodiETempi
    ).

calcola_tempo(Node, Key) ->
    Inizio = erlang:monotonic_time(microsecond),
    Risultato = find_value_iterative(Node, Key),
    Fine = erlang:monotonic_time(microsecond),
    Tempo = Fine - Inizio,
    case Risultato of
        {ok, Value} -> {ok, Value, Tempo};
        _ -> false
    end.
