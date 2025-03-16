-module(tests).
-include("parameters.hrl").
-import(knode, [start_link/1, start_link/2, store_value/2, stop/1]).
-import(iterative_search, [start_find_value_iterative/2, start_find_node_iterative/2]).
-export([calcola_tempo_find_value/3, calcola_tempo_find_node/3, start_nodes/1]).

%% Avvia più nodi Kademlia per test
start_nodes(NumNodes) ->
    start_nodes(NumNodes, {}, []).

start_nodes(0, _, JoinTimeList) ->
    case length(JoinTimeList) > 0 of
        true ->
            Media = round(lists:sum(JoinTimeList) / length(JoinTimeList)),
            io:format("Media dei tempi per la join: ~p, su ~p nodi totali~n~n", [
                Media, length(JoinTimeList)
            ]);
        _ ->
            ok
    end;
start_nodes(NumNodes, BootstrapNode, JoinTimeList) ->
    Name = list_to_atom("knode_" ++ integer_to_list(NumNodes)),
    case BootstrapNode of
        % Primo nodo, diventa bootstrap
        {} ->
            start_link(Name),
            start_nodes(NumNodes - 1, {Name, whereis(Name)}, JoinTimeList);
        % Nodi successivi, si connettono al bootstrap
        {NameBootstrap, BootstrapNodePID} ->
            Inizio = erlang:monotonic_time(microsecond),
            start_link(Name, BootstrapNodePID),
            Fine = erlang:monotonic_time(microsecond),
            Tempo = Fine - Inizio,

            start_nodes(NumNodes - 1, {NameBootstrap, BootstrapNodePID}, JoinTimeList ++ [Tempo])
    end.

calcola_tempo_find_value(NumNodesToKillPerIter, Key, NumNodes) ->
    calcola_tempo_find_value(NumNodesToKillPerIter, Key, NumNodes, []).

calcola_tempo_find_value(NumNodesToKillPerIter, Key, NumNodes, NodesKilled) ->
    %creo la lista con i nomi dei nodi
    ListaNodi = [
        {N, list_to_atom("knode_" ++ integer_to_list(N))}
     || N <- lists:seq(1, NumNodes)
    ],
    case NodesKilled of
        [] ->
            start_nodes(NumNodes),
            %aggiungo un valore ad un nodo a caso
            store_value(list_to_atom("knode_" ++ integer_to_list(rand:uniform(NumNodes))), 123),

            %aspetto che faccia almeno 5 republish
            timer:sleep((?T * 1000) * 5),

            %faccio eseguire la find_value_iterative a tutti i nodi
            start_routine_find_value_iterative(ListaNodi, Key),
            %Prendo dei nodi a caso e li uccido, il numero di questo nodi a caso viene scelto da NodesToKillPerIteration
            NodesToKill = select_random_tuples(ListaNodi, NumNodesToKillPerIter),
            %io:format("NodesToKill:~p~n", [NodesToKill]),
            lists:foreach(
                fun({Num, _}) ->
                    stop(list_to_atom("knode_" ++ integer_to_list(Num)))
                end,
                NodesToKill
            ),

            calcola_tempo_find_value(
                NumNodesToKillPerIter, Key, NumNodes, NodesKilled ++ NodesToKill
            );
        _ ->
            %aspetto che rieseguono la republish
            %timer:sleep((?T * 1000) + 500),

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
                                    stop(list_to_atom("knode_" ++ integer_to_list(Num)))
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
                            stop(list_to_atom("knode_" ++ integer_to_list(Num)))
                        end,
                        NodesToKill
                    ),

                    calcola_tempo_find_value(
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
                "Media dei tempi di lookup dei nodi che hanno trovato il valore: ~p (~p su ~p nodi totali)~n~n",
                [
                    MediaFound, length(ListaNodiFound), length(ListaNodi)
                ]
            )
    end.

calcola_tempo_find_node(NumNodesToKillPerIter, Key, NumNodes) ->
    calcola_tempo_find_node(NumNodesToKillPerIter, Key, NumNodes, []).

calcola_tempo_find_node(NumNodesToKillPerIter, Key, NumNodes, NodesKilled) ->
    %creo la lista con i nomi dei nodi
    ListaNodi = [
        {N, list_to_atom("knode_" ++ integer_to_list(N))}
     || N <- lists:seq(1, NumNodes)
    ],
    case NodesKilled of
        [] ->
            start_nodes(NumNodes),

            %faccio eseguire la find_value_iterative a tutti i nodi
            start_routine_find_node_iterative(ListaNodi, Key),
            %Prendo dei nodi a caso e li uccido, il numero di questo nodi a caso viene scelto da NodesToKillPerIteration
            NodesToKill = select_random_tuples(ListaNodi, NumNodesToKillPerIter),
            %io:format("NodesToKill:~p~n", [NodesToKill]),
            lists:foreach(
                fun({Num, _}) ->
                    stop(list_to_atom("knode_" ++ integer_to_list(Num)))
                end,
                NodesToKill
            ),

            calcola_tempo_find_node(
                NumNodesToKillPerIter, Key, NumNodes, NodesKilled ++ NodesToKill
            );
        _ ->
            %aspetto che rieseguono la republish
            %timer:sleep((?T * 1000) + 500),

            %io:format("NodesKilled: ~p~n", [NodesKilled]),
            %prendo i nodi che non sono nella lista dei NodesKilled
            SurvidedNodes = ListaNodi -- NodesKilled,
            %io:format("SurvidedNodes: ~p~n", [SurvidedNodes]),

            %faccio la routine della find_value_iterative
            start_routine_find_node_iterative(SurvidedNodes, Key),

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
                                    stop(list_to_atom("knode_" ++ integer_to_list(Num)))
                                end,
                                NodesToKill
                            ),
                            %eseguo per l'ultima volta la routine per i tempi dato che la eseguo ad un nodo solo
                            %io:format("Primo nodo sopravvissuto: ~p~n", [[hd(SurvidedNodes)]]),
                            start_routine_find_node_iterative([hd(SurvidedNodes)], Key)
                    end;
                _ ->
                    %riprendo dei nodi a caso e li uccido
                    NodesToKill = select_random_tuples(SurvidedNodes, NumNodesToKillPerIter),
                    lists:foreach(
                        fun({Num, _}) ->
                            stop(list_to_atom("knode_" ++ integer_to_list(Num)))
                        end,
                        NodesToKill
                    ),

                    calcola_tempo_find_node(
                        NumNodesToKillPerIter, Key, NumNodes, NodesKilled ++ NodesToKill
                    )
            end
    end.

start_routine_find_node_iterative(ListaNodi, Key) ->
    NodeListWithTimeResult = lists:foldl(
        fun({Index, NameNode}, Acc) ->
            Inizio = erlang:monotonic_time(microsecond),
            Risultato = start_find_node_iterative(NameNode, Key),
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
    ]).

select_random_tuples(List, Num) ->
    select_random_tuples(List, Num, []).

select_random_tuples([], _, Acc) ->
    Acc;
select_random_tuples(List, Num, Acc) when Num > 0 ->
    Index = rand:uniform(length(List)),
    {Selected, _} = lists:split(Index, List),
    select_random_tuples(List -- [lists:last(Selected)], Num - 1, [lists:last(Selected)] ++ Acc);
select_random_tuples(_, 0, Acc) ->
    Acc.
