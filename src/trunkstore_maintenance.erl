%%%-----------------------------------------------------------------------------
%%% @copyright (C) 2012-2020, 2600Hz
%%% @doc
%%% @author James Aimonetti
%%%
%%% This Source Code Form is subject to the terms of the Mozilla Public
%%% License, v. 2.0. If a copy of the MPL was not distributed with this
%%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(trunkstore_maintenance).

-export([clear_old_calls/0
        ,classifier_inherit/2
        ,classifier_deny/2
        ,flush/0, flush/1
        ]).

-export([migrate/1]).

-include("ts.hrl").

-spec flush() -> 'ok'.
flush() ->
    kz_cache:flush_local(?CACHE_NAME).

-spec flush(kz_term:ne_binary()) -> 'ok'.
flush(Account) ->
    AccountId = kzs_util:format_account_id(Account),
    Flush = kz_cache:filter_local(?CACHE_NAME
                                 ,fun(Key, _Value) -> is_ts_cache_object(Key, AccountId) end
                                 ),
    _ = [kz_cache:erase_local(?CACHE_NAME, Key) || {Key, _Value} <- Flush],
    'ok'.

%%------------------------------------------------------------------------------
%% @doc Some calls get stuck if they miss the CDR. This clears them out.
%% @end
%%------------------------------------------------------------------------------
-spec clear_old_calls() -> 'ok'.
clear_old_calls() ->
    _ = clear_old_calls('ts_offnet_sup'),
    _ = clear_old_calls('ts_onnet_sup'),
    'ok'.

clear_old_calls(Super) ->
    Ps = [P || {_,P,_,_} <- supervisor:which_children(Super)],
    [begin
         {'dictionary', D} = erlang:process_info(P, 'dictionary'),
         C = props:get_value('callid', D),
         case kapps_call_command:channel_status(C) of
             {'error', _} -> {P, C, exit(P, 'kill')};
             _ -> {P, C, 'ok'}
         end
     end || P <- Ps
    ].

%%------------------------------------------------------------------------------
%% @doc classifier_inherit.
%%
%% Usage example:
%%
%% ```
%% sup trunkstore_maintenance classifier_inherit international pbx_username@realm.domain.tld
%% '''
%% @end
%%------------------------------------------------------------------------------
-spec classifier_inherit(kz_json:object(), kz_term:ne_binary()) -> 'ok'.
classifier_inherit(Classifier, UserR) ->
    set_classifier_action(<<"inherit">>, Classifier, UserR).

%%------------------------------------------------------------------------------
%% @doc classifier_deny.
%%
%% Usage example:
%% ```
%% sup trunkstore_maintenance classifier_deny international trunkserver_id@account_id
%% '''
%% @end
%%------------------------------------------------------------------------------
-spec classifier_deny(kz_json:object(), kz_term:ne_binary()) -> 'ok'.
classifier_deny(Classifier, UserR) ->
    set_classifier_action(<<"deny">>, Classifier, UserR).

set_classifier_action(Action, Classifier, UserR) ->
    io:format("Classifier: ~p", [Classifier]),
    Classifiers = knm_converters:available_classifiers(),
    case lists:member(Classifier, kz_json:get_keys(Classifiers)) of
        'false' ->
            io:format("\nNo ~p classifier among configured classifiers ~p\n", [Classifier, kz_json:get_keys(Classifiers)]),
            exit('no_such_classifier');
        _ ->
            io:format("  ... found\n")
    end,
    [TrunkServerId, AccountId] = re:split(UserR, <<"@">>, [{'return','binary'}, {'parts',2}]),
    case ts_util:onnet_options(AccountId, TrunkServerId) of
        'undefined' ->
            io:format("Failed: trunk server ~s @ ~s does not exist\n", [TrunkServerId, AccountId]);
        Options ->
            TSDocId = kz_doc:id(Options),
            Updates = [{[<<"call_restriction">>, Classifier, <<"action">>], Action}],
            UpdateOptions = [{'update', Updates}],
            {'ok', _} = kz_datamgr:update_doc(AccountId, TSDocId, UpdateOptions),
            io:format("Success\n")
    end.

-spec is_ts_cache_object(tuple(), kz_term:ne_binary()) -> boolean().
is_ts_cache_object({'lookup_user_flags', _Realm, _User, AccountId}, AccountId) ->
    'true';
is_ts_cache_object({'lookup_did', _DID, AccountId}, AccountId) ->
    'true';
is_ts_cache_object(_Key, _AccountId) ->
    'false'.

-spec migrate(kz_term:ne_binaries()) -> any().
migrate(Accounts) ->
    lists:foreach(fun migrate_trunkstore/1, Accounts).

migrate_trunkstore(AccountId) ->
    DesignDoc = <<"crossbar_listings/by_type_id">>,
    Options = [{'key', <<"sys_info">>}],
    Ids = kz_datamgr:get_result_ids(AccountId, DesignDoc, Options),
    migrate_trunkstore(AccountId, Ids).

migrate_trunkstore(_AccountId, []) -> ok;
migrate_trunkstore(AccountId, [Id | Ids]) ->
    case kz_datamgr:open_doc(AccountId, Id) of
        {ok, Doc} -> maybe_migrate_trunkstore_doc(Doc);
        {error, Error} -> io:format("error reading document from trunkstore ~s / ~s => ~p", [AccountId, Id, Error])
    end,
    migrate_trunkstore(AccountId, Ids).

needs_migration(Doc) ->
    lists:any(fun kzd_trunkstore:needs_migration/1, Doc).

maybe_migrate_trunkstore_doc(Doc) ->
    maybe_migrate_trunkstore_doc(Doc, needs_migration(Doc)).

maybe_migrate_trunkstore_doc(_Doc, false) -> ok;
maybe_migrate_trunkstore_doc(Doc, true) -> kzd_trunkstore:save(Doc).
