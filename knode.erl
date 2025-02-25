-module(knode).
-behaviour(gen_server).
-include_lib("stdlib/include/ms_transform.hrl").
-define(K, 5).
-define(T, 3600).
-export([start_link/2, store/2, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).
-export([
    stop/0,
    get_id/1,
    read_store/0,
    find_node/1,
    find_value/1,
    generate_requestId/0
]).

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
    BootstrapNodePID = maps:get(bootstrap, State, undefined),
    start_periodic_republish(self()),
    NewState = {Id, 0, State, StoreTable, KBuckets},
    case BootstrapNodePID of
        % Sono il primo nodo
        undefined ->
            io:format("Nodo ~p diventato bootstrap~n", [Id]),
            {ok, NewState};
        % Connetto al nodo bootstrap
        _ ->
            io:format("Nodo ~p prova a connettersi a bootstrap ~p~n", [Id, BootstrapNodePID]),
            % Verifico se il nodo bootstrap è vivo
            %Potrei fare un genservercall con join_request direttamente, se mi risponde in modo
            %positivo, allora mi collego a lui
            %case catch gen_server:call(BootstrapNodePID, get_id, 2000) of
            RequestId = generate_requestId(),
            case
                catch gen_server:call(
                    BootstrapNodePID, {join_request, Id, RequestId}
                )
            of
                {'EXIT', Reason} ->
                    % Nodo bootstrap non raggiungibile, divento bootstrap
                    io:format("Nodo bootstrap ~p non raggiungibile (~p), divento bootstrap~n", [
                        BootstrapNodePID, Reason
                    ]),
                    {ok, NewState};
                {k_buckets, BucketsReceived, BootstrapID, RequestIdReceived} ->
                    % Nodo bootstrap raggiungibile, procedo normalmente
                    io:format("Nodo bootstrap ~p risponde, ID: ~p~n", [
                        BootstrapNodePID, BootstrapID
                    ]),
                    %Controllare se il requestId è corretto
                    %...
                    case RequestId == RequestIdReceived of
                        true ->
                            %aggiungo il nodo bootstrap ai miei kbucket e poi aggiungo anche i suoi nodi
                            %mandati in risposta
                            add_bootstrap_node_in_kbuckets(
                                BootstrapNodePID, KBuckets, Id, BootstrapID
                            ),
                            add_nodes_to_kbuckets(Id, BucketsReceived, KBuckets),
                            {ok, NewState};
                        false ->
                            io:format("Sono il nodo con pid: ~p Request Id non corretto~n", [
                                BootstrapNodePID
                            ]),
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

handle_call({ping, RequestId}, _From, {Id, Counter, State, StoreTable, KBuckets}) ->
    {PID, _} = _From,
    io:format("Node ~p (~p) received ping from ~p~n", [self(), Id, PID]),
    {reply, {pong, self(), RequestId}, {Id, Counter, State, StoreTable, KBuckets}};
handle_call(stop, _From, {Id, Counter, State, StoreTable, KBuckets}) ->
    {stop, normal, {Id, Counter, State, StoreTable, KBuckets}};
handle_call(get_id, _From, {Id, Counter, State, StoreTable, KBuckets}) ->
    {reply, Id, {Id, Counter, State, StoreTable, KBuckets}};
handle_call({read_store, RequestId}, _From, {Id, Counter, State, StoreTable, KBuckets}) ->
    io:format("Received read_store request from ~p,~p~n", [_From, Id]),
    % Leggi la tabella ETS
    Tuples = ets:tab2list(StoreTable),
    io:format("Table content: ~p~n", [Tuples]),
    {reply, {Tuples, RequestId}, {Id, Counter, State, StoreTable, KBuckets}};
handle_call(
    {find_node, ToFindNodeId, RequestId}, _From, {Id, Counter, State, StoreTable, KBuckets}
) ->
    {PID, _} = _From,
    io:format("Node ~p (~p) received FIND_NODE request for ID ~p from ~p~n", [
        self(), Id, ToFindNodeId, PID
    ]),
    % 1. Recupera i k-bucket dalla tabella ETS
    KBucketsList = ets:tab2list(KBuckets),
    % 2. Implementa la logica per trovare i nodi più vicini
    ClosestNodes = find_closest_nodes(ToFindNodeId, KBucketsList),
    % 3. Rispondi al nodo richiedente con la lista dei nodi più vicini
    {reply, {found_nodes, ClosestNodes, RequestId}, {Id, Counter, State, StoreTable, KBuckets}};
handle_call({find_value, Key, RequestId}, _From, {Id, Counter, State, StoreTable, KBuckets}) ->
    io:format("Node ~p (~p) received FIND_VALUE request for Key ~p from ~p~n", [
        self(), Id, Key, _From
    ]),
    {PID, _} = _From,
    [{Key, Value}] = ets:lookup(StoreTable, Key),
    % case ets:lookup(StoreTable, Key) of
    %     [{Key, Value}] ->
    %         % Il nodo ha il valore, lo restituisce
    %         PID ! {found_value, Value};
    %     [] ->
    %         % Il nodo non ha il valore, restituisce i nodi più vicini
    %         KBucketsList = ets:tab2list(KBuckets),
    %         ClosestNodes = find_closest_nodes(Key, KBucketsList),
    %         PID ! {found_nodes, ClosestNodes}
    % end,
    {reply, {found_value, Value, RequestId}, {Id, Counter, State, StoreTable, KBuckets}};
handle_call(
    {join_request, IdNewNode, RequestId}, _From, {Id, Counter, State, StoreTable, KBuckets}
) ->
    {PID, _} = _From,
    io:format("Sono il Nodo ~p, ho ricevuto join_request da ~p~n", [Id, PID]),

    % Calcolo la distanza tra il mio ID e l'ID del NewNode
    Distanza = calcola_distanza(Id, IdNewNode),

    % Ottengo il giusto intervallo del k-bucket
    RightKbucket = get_right_bucket_interval(Distanza, KBuckets),
    io:format("Il kbucket giusto è: ~p~n", [RightKbucket]),

    % Recupera il contenuto corrente del k-bucket
    [{Key, CurrentNodes}] = ets:lookup(KBuckets, RightKbucket),
    io:format("L'output di lookup è: ~p~n", [[{Key, CurrentNodes}]]),

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
    io:format("Received unknown request: ~p~n", [_Request]),
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    io:format("Received cast message: ~p~n", [_Msg]),
    {noreply, State}.
%Guardare se deve essere call/cast
% handle_info(
%     {join_request, PIDNewNode, IdNewNode}, {Id, Counter, State, StoreTable, KBuckets}
% ) ->
%     io:format("Sono il Nodo ~p, ho ricevuto join_request da ~p~n", [Id, PIDNewNode]),

%     % Calcolo la distanza tra il mio ID e l'ID del NewNode
%     Distanza = calcola_distanza(Id, IdNewNode),

%     % Ottengo il giusto intervallo del k-bucket
%     RightKbucket = get_right_bucket_interval(Distanza, KBuckets),
%     io:format("Il kbucket giusto è: ~p~n", [RightKbucket]),

%     % Recupera il contenuto corrente del k-bucket
%     [{Key, CurrentNodes}] = ets:lookup(KBuckets, RightKbucket),
%     io:format("L'output di lookup è: ~p~n", [[{Key, CurrentNodes}]]),

%     %aggiungo in coda
%     UpdatedNodes = CurrentNodes ++ [{PIDNewNode, IdNewNode}],

%     %Gestisco la dimensione massima del k-bucket
%     NewNodes =
%         if
%             length(UpdatedNodes) > ?K ->
%                 % Rimuovi il nodo meno recentemente visto (alla testa della lista)
%                 %Ricordarsi di aggiungere il ping, quindi di rimuoverlo se pingando non
%                 %risponde, andando avanti per tutti i nodi seguenti, se tutti rispondono,
%                 %non aggiungere il nodo
%                 tl(UpdatedNodes);
%             true ->
%                 UpdatedNodes
%         end,

%     %Reinserisco la tupla aggiornata nel k-bucket
%     ets:insert(KBuckets, {Key, NewNodes}),

%     % invia i miei k-buckets
%     PIDNewNode ! {k_buckets, KBuckets},
%     {noreply, {Id, Counter, State, StoreTable, KBuckets}};
% handle_info({k_buckets, BucketsReceived}, {Id, Counter, State, StoreTable, KBuckets}) ->
%     {noreply, {Id, Counter, State, StoreTable, KBuckets}};
handle_info(republish, {Id, Counter, State, StoreTable, KBuckets}) ->
    io:format("Received republish message, sono il nodo con il pid:~p~n", [self()]),
    % 1. Ripubblica i dati
    republish_data(StoreTable, KBuckets),
    % 2. Reimposta il timer per la prossima ripubblicazione
    start_periodic_republish(self()),
    {noreply, {Id, Counter, State, StoreTable, KBuckets}};
handle_info({store, Key, Value}, {Id, Counter, State, StoreTable, KBuckets}) ->
    io:format("Received store request: Id=~p Key=~p, Value=~p, From=?~n", [Id, Key, Value]),
    % Incrementa il contatore
    NewCounter = Counter + 1,
    % Inserisci la tupla nella tabella ETS
    ets:insert(StoreTable, {Key, Value}),
    io:format("Inserted in ETS: Key=~p, Value=~p~n", [Key, Value]),
    % Rispondi al client
    {noreply, {Id, NewCounter, State, StoreTable, KBuckets}};
handle_info({store, Value}, {Id, Counter, State, StoreTable, KBuckets}) ->
    io:format("Received store request: Id=~p, Value=~p, From=?~n", [Id, Value]),
    % Incrementa il contatore
    NewCounter = Counter + 1,
    %Calcola la key
    HashValue = crypto:hash(sha, integer_to_binary(Value)),
    Key = binary_to_integer_representation(HashValue),
    % Inserisci la tupla nella tabella ETS
    ets:insert(StoreTable, {Key, Value}),
    io:format("Inserted in ETS: Key=~p, Value=~p~n", [Key, Value]),
    % Rispondi al client
    {noreply, {Id, NewCounter, State, StoreTable, KBuckets}};
handle_info(_Info, State) ->
    io:format("Received info message: ~p~n", [_Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    io:format("Kademlia node terminating~n"),
    ok.

% ping() ->
%     gen_server:call(?MODULE, {ping, self()}).

stop() ->
    gen_server:cast(?MODULE, stop).

get_id(PID) ->
    gen_server:call(PID, get_id, 2000).

store(Key, Value) ->
    gen_server:call(?MODULE, {store, Key, Value}).

read_store() ->
    gen_server:call(?MODULE, read_store).

find_node(ToFindNodeId) ->
    gen_server:call(?MODULE, {find_node, ToFindNodeId, generate_requestId()}).

find_value(Key) ->
    gen_server:call(?MODULE, {find_value, Key, generate_requestId()}).

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
    io:format("La distanza calcolata è: ~p ~n", [Distanza]),
    Distanza.

find_closest_nodes(Key, KBuckets) ->
    % 1. Ottieni tutti i nodi dai k-buckets.
    Nodi = get_all_nodes_from_kbuckets(KBuckets),
    io:format("Nodi estratti: ~p~n", [Nodi]),

    % 2. Calcola la distanza di ogni nodo rispetto a ToFindNodeId
    NodiConDistanza = aggiungi_distanza(Nodi, Key),
    io:format("Nodi con distanza: ~p~n", [NodiConDistanza]),

    %Rimuovo nodo con distanza 0
    NodiConDistanzaNo0 = lists:filter(fun({_, _, X}) -> X =/= 0 end, NodiConDistanza),
    io:format("Nodi con distanza senza 0: ~p~n", [NodiConDistanzaNo0]),

    % 3. Ordina i nodi per distanza crescente.
    NodiOrdinati = lists:sort(
        fun({_, _, Dist1}, {_, _, Dist2}) -> Dist1 < Dist2 end, NodiConDistanzaNo0
    ),
    io:format("Nodi ordinati: ~p~n", [NodiOrdinati]),

    % 4. Restituisce i primi K nodi (o tutti se ce ne sono meno di K) senza la distanza.
    KClosestNodesWithDistance = lists:sublist(NodiOrdinati, ?K),
    KClosestNodes = lists:map(
        fun({IdNodo, AltroDato, _}) -> {IdNodo, AltroDato} end, KClosestNodesWithDistance
    ),

    io:format("Nodi più vicini (limitati a K e senza distanza): ~p~n", [KClosestNodes]),
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

start_periodic_republish(NodePID) ->
    erlang:send_after(timer:seconds(?T), NodePID, republish).

republish_data(StoreTable, KBuckets) ->
    io:format("Sono il nodo con pid: ~p, sto ripubblicando i dati...~n", [self()]),

    % 1. Ottieni tutti i dati (coppie chiave-valore) dalla tabella ETS (StoreTable).
    Data = ets:tab2list(StoreTable),
    io:format("Dati da ripubblicare: ~p\n", [Data]),

    % 2. Ottieni la lista dei k-buckets
    KBucketsList = ets:tab2list(KBuckets),

    % 3. Per ogni dato (coppia chiave-valore):
    lists:foreach(
        fun({Key, Value}) ->
            io:format("Ripubblicazione della chiave ~p\n", [Key]),

            % 4. Trova i k nodi più vicini alla chiave.
            ClosestNodes = find_closest_nodes(Key, KBucketsList),

            % 5. Invia una richiesta STORE a ciascuno dei k nodi più vicini.
            lists:foreach(
                fun({NodePID, NodeId}) ->
                    io:format("Invio richiesta STORE a nodo ~p (ID: ~p)\n", [NodePID, NodeId]),
                    % Invia la richiesta STORE in modo asincrono (cast) per non bloccare il processo di ripubblicazione
                    NodePID ! {store, Key, Value}
                %gen_server:call(NodePID, {store, Key, Value}, 2000)
                end,
                ClosestNodes
            ),
            io:format("Richieste di store mandate per la chiave ~p\n", [Key])
        end,
        Data
    ),
    io:format("Ripubblicazione dati completata per il nodo con pid: ~p\n", [self()]).

add_nodes_to_kbuckets(Id, BucketsReceived, MyKBuckets) ->
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
                            RightKbucket = get_right_bucket_interval(Distanza, MyKBuckets),

                            % Recupera i nodi attualmente presenti nel bucket corretto
                            case RightKbucket of
                                false ->
                                    io:format(
                                        "+++++ ERRORE: nessun bucket trovato per la distanza ~p (nodo ~p)~n",
                                        [Distanza, NodeId]
                                    );
                                {_, _} ->
                                    [{Key, CurrentNodes}] = ets:lookup(MyKBuckets, RightKbucket),

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
                                    ets:insert(MyKBuckets, {Key, NewNodesLimited})
                            end
                    end
                end,
                Nodes
            )
        end,
        BucketsReceivedList
    ).
