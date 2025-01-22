-module(kademlia_node).
-export([start_kademlia_node/0, ping/1, loop/0]).

start_kademlia_node() ->
    spawn(kademlia_node, loop, []).

loop() ->
    receive
        {ping, Sender} ->
            io:format("Node ~p received ping from ~p~n", [self(), Sender]),
            Sender ! {pong, self()},
            loop();
        Other ->
            io:format("Node ~p received unknown message: ~p~n", [self(), Other]),
            loop()
    end.

ping(Node) ->
    Node ! {ping, self()},
    receive
        {pong, Node} ->
            io:format("Node ~p received pong from ~p~n", [self(), Node]),
            ok
    after 1000 ->
        io:format("Node ~p no pong received~n", [self()]),
        timeout
    end.
