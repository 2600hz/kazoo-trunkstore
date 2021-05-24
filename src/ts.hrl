-ifndef(TS_HRL).
-include_lib("kazoo_stdlib/include/kz_types.hrl").
-include_lib("kazoo_stdlib/include/kz_log.hrl").
-include_lib("kazoo_stdlib/include/kz_databases.hrl").
-include_lib("kazoo_numbers/include/knm_phone_number.hrl").
-include_lib("kazoo_sip/include/kzsip_uri.hrl").

-define(APP_NAME, <<"trunkstore">>).
-define(APP_VERSION, <<"4.0.0">>).
-define(CONFIG_CAT, ?APP_NAME).

%% couch params for the trunk store and its views
-define(CACHE_NAME, 'trunkstore_cache').

%% just want to deal with binary K/V pairs
-type active_calls() :: [{binary(), 'flat_rate' | 'per_min'}].

-record(ts_callflow_state, {aleg_callid :: kz_term:api_ne_binary()
                           ,bleg_callid :: kz_term:api_ne_binary()
                           ,acctid = <<>> :: binary()
                           ,acctdb = <<>> :: binary()
                           ,route_req_jobj = kz_json:new() :: kapi_route:req()
                           ,ep_data = kz_json:new() :: kz_json:object() %% data for the endpoint, either an actual endpoint or an offnet request
                           ,amqp_worker :: kz_term:api_pid()
                           ,amqp_queue :: kz_term:api_ne_binary()
                           ,callctl_q :: kz_term:api_ne_binary()
                           ,call_cost = 0.0 :: float()
                           ,failover :: kz_term:api_object()
                           ,kapps_call :: kapps_call:call()
                           }).

-define(TS_HRL, 'true').
-endif.
