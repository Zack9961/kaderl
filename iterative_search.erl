-module(iterative_search).
-include("parameters.hrl").
-import(knode, [
    aggiungi_distanza/2,
    generate_requestId/0,
    find_node/3,
    find_value/3,
    add_nodes_to_kbuckets_cast/2
]).
-export([
    find_node_iterative/3,
    find_value_iterative/3,
    start_find_node_iterative/2,
    start_find_value_iterative/2
]).

find_node_iterative(AlphaClosestNodes, Key, ParentNode) ->
    find_node_iterative(AlphaClosestNodes, Key, ParentNode, []).

find_node_iterative(AlphaClosestNodes, Key, ParentNode, BestNodes) ->
    ParentPID = self(),
    case BestNodes == [] of
        true ->
            Responses = find_node_spawn(AlphaClosestNodes, Key, ParentNode),

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
            add_nodes_to_kbuckets_cast(ParentPID, NewBestNodes),

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
            Responses = find_node_spawn(AlphaClosestNodes, Key, ParentNode),

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
                    add_nodes_to_kbuckets_cast(ParentPID, AllReceivedNodesNoDistanceNoSelf),

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

find_node_spawn(AlphaClosestNodes, Key, ParentNode) ->
    %Creo la lista di soli pid dalla lista di tuple
    AlphaClosestNodesPID = lists:map(fun({PIDNodo, _}) -> PIDNodo end, AlphaClosestNodes),
    % Eseguo la spawn di tanti processi quanti sono gli elementi della lista AlphaClosestNodesPID
    lists:foreach(
        fun(PIDNodo) ->
            spawn(fun() -> find_node(PIDNodo, Key, ParentNode) end)
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
            Responses = find_value_spawn(AlphaClosestNodes, Key, ParentNode),

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
                    add_nodes_to_kbuckets_cast(ParentPID, NewBestNodes),

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
            Responses = find_value_spawn(AlphaClosestNodes, Key, ParentNode),

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
                            add_nodes_to_kbuckets_cast(ParentPID, AllReceivedNodesNoDistanceNoSelf),

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

find_value_spawn(AlphaClosestNodes, Key, ParentNode) ->
    %Creo la lista di soli pid dalla lista di tuple
    AlphaClosestNodesPID = lists:map(fun({PIDNodo, _}) -> PIDNodo end, AlphaClosestNodes),

    % Eseguo la spawn di tanti processi quanti sono gli elementi della lista AlphaClosestNodesPID
    lists:foreach(
        fun(PIDNodo) ->
            spawn(fun() -> find_value(PIDNodo, Key, ParentNode) end)
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
