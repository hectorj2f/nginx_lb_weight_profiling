/*
 * ngx_http_upstream_conpaas.c
 *
 * Load balancing module for nginx, customized for ConPaaS.
 * The module can perform profiling of the load balanced servers on request.
 * The profiling is done by directing to each server
 * a number of specified workload intensities. The request for profiling is received
 * via a signal sent by the ConPaaS agent. The requirements for the profiling
 * (list of servers to be profiled, list of workload intensities, the duration
 *  of profiling) are read from a configuration file.
 *
 *  Author: Corina Stratan, based on the fair upstream module written by G. Nosek
 */

#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>

/**
 * Module configuration structure.
 */
typedef struct {
    char *profiling_file; /* path of the config file which contains the profiling parameters */
} ngx_http_upstream_conpaas_conf_t;

typedef struct ngx_http_upstream_conpaas_peers_s  ngx_http_upstream_conpaas_peers_t;

/**
 * Structure that holds information about the hosts (peers) which are being
 * load balanced.
 */
struct ngx_http_upstream_conpaas_peers_s {
    ngx_uint_t                      single;        /* unsigned  single:1; */
    ngx_uint_t                      number;
    ngx_uint_t                      last_cached;

    ngx_int_t						idx_peer_to_test;
    ngx_int_t						peer_to_test;
    ngx_time_t						test_start_time;
    ngx_time_t						last_test_req_time;
    ngx_int_t						n_tested_requests;
    ngx_int_t						wl_index;

 /* ngx_mutex_t                    *mutex; */
    ngx_connection_t              **cached;

    ngx_str_t                      *name;

    ngx_http_upstream_conpaas_peers_t   *next;

    ngx_http_upstream_rr_peer_t     peer[1]; /* array that will actually contain all the peers */
};


typedef struct {
    ngx_http_upstream_conpaas_peers_t   *peers;
    ngx_uint_t                      current;
    uintptr_t                      *tried;
    uintptr_t                       data;
    ngx_http_upstream_conpaas_conf_t *module_conf;
} ngx_http_upstream_conpaas_peer_data_t;


static char *ngx_http_upstream_conpaas(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);
static void *ngx_http_upstream_conpaas_create_conf(ngx_conf_t *cf);

static ngx_int_t ngx_http_upstream_init_conpaas_peer(ngx_http_request_t *r, ngx_http_upstream_srv_conf_t *us);
static void ngx_http_upstream_free_conpaas_peer(ngx_peer_connection_t *pc, void *data, ngx_uint_t state);
static ngx_int_t ngx_http_upstream_get_conpaas_peer(ngx_peer_connection_t *pc, void *data);

static ngx_int_t ngx_http_upstream_cmp_servers(const void *one, const void *two);



static ngx_uint_t ngx_http_upstream_conpaas_select_peer(ngx_http_upstream_conpaas_peers_t *peers, ngx_http_upstream_conpaas_conf_t *conf, ngx_peer_connection_t *pc);
/*
static ngx_int_t ngx_http_upstream_create_conpaas_peer(ngx_http_request_t *r, ngx_http_upstream_resolved_t *ur);
*/

void sigusr2_handler(int signum);
static ngx_int_t get_profiling_config(char *filename, int n_total_peers);
static ngx_int_t ngx_difftime_msec(ngx_time_t t1, ngx_time_t t0);

static volatile sig_atomic_t sig_profiling = 0;
static ngx_uint_t signal_initialized = 0;

/* Time (in seconds) for which each workload intensity is directed to the profiled server. */
static ngx_int_t profile_time = 5;
/* Number of peers (back-end servers) to profile. */
static ngx_int_t n_profile_peers = 0;
/* The list of back-end servers to profile. */
static ngx_int_t profile_peers[50];
/* Number of workload intensity values used for profiling. */
static ngx_int_t n_workloads = 3;
/* The workload intensities used for profiling. */
static ngx_int_t workloads[20];

/**
 * Module directives. This module has only one directive, taking one configuration
 * parameter (the location of the file that specifies the profiling workloads).
 */
static ngx_command_t  ngx_http_upstream_conpaas_commands[] = {

    { ngx_string("conpaas"),
      NGX_HTTP_UPS_CONF|NGX_CONF_TAKE1,
      ngx_http_upstream_conpaas,
      NGX_HTTP_LOC_CONF_OFFSET,
      0,
      NULL },

      ngx_null_command
};

/**
 * Module context. Contains pointers to functions that are used to generate the module configuration.
 */
static ngx_http_module_t  ngx_http_upstream_conpaas_module_ctx = {
    NULL,                                  /* preconfiguration */
    NULL, /* ngx_http_copy_filter_init, */             /* postconfiguration */

    NULL,                                  /* create main configuration */
    NULL,                                  /* init main configuration */

    NULL,                                  /* create server configuration */
    NULL,                                  /* merge server configuration */

    ngx_http_upstream_conpaas_create_conf,   /* create location configuration */
    NULL							        /* merge location configuration */
};

/**
 * Module definition.
 */
ngx_module_t  ngx_http_upstream_conpaas_module = {
    NGX_MODULE_V1,
    &ngx_http_upstream_conpaas_module_ctx,    /* module context */
    ngx_http_upstream_conpaas_commands,       /* module directives */
    NGX_HTTP_MODULE,                       /* module type */
    NULL,                                  /* init master */
    NULL,                                  /* init module */
    NULL,                                  /* init process */
    NULL,                                  /* init thread */
    NULL,                                  /* exit thread */
    NULL,                                  /* exit process */
    NULL,                                  /* exit master */
    NGX_MODULE_V1_PADDING
};



/**
 * Initializes the module's configuration structure.
 */
static void *
ngx_http_upstream_conpaas_create_conf(ngx_conf_t *cf)
{
    ngx_http_upstream_conpaas_conf_t *conf;

    //printf("log level: %x\n", cf -> log -> log_level);
    ngx_log_debug0(NGX_LOG_NOTICE, cf -> log, 0, "In ngx_http_upstream_conpaas_create_conf()...");

    conf = ngx_palloc(cf-> pool, sizeof(ngx_http_upstream_conpaas_conf_t));
    if (conf == NULL) {
        return NULL;
    }

    /* default location for the configuration file */
    conf -> profiling_file = "/tmp/conpaas_profile";

    return conf;
}

/**
 * Upstream initialization function. Allocates sockets for the load-balanced servers.
 */
ngx_int_t
ngx_http_upstream_init_conpaas(ngx_conf_t *cf,
    ngx_http_upstream_srv_conf_t *us)
{
    ngx_url_t                      u;
    ngx_uint_t                     i, j, n;
    ngx_http_upstream_server_t    *server;
    ngx_http_upstream_conpaas_peers_t  *peers, *backup;


    ngx_log_debug0(NGX_LOG_NOTICE, cf -> log, 0, "In ngx_http_upstream_init_conpaas()...");

    /* set the peer initialization function */
    us->peer.init = ngx_http_upstream_init_conpaas_peer;

    if (us->servers) {
        server = us->servers->elts;

        n = 0;

        for (i = 0; i < us->servers->nelts; i++) {
            if (server[i].backup) {
                continue;
            }

            n += server[i].naddrs;
        }

        peers = ngx_pcalloc(cf->pool, sizeof(ngx_http_upstream_conpaas_peers_t)
                              + sizeof(ngx_http_upstream_rr_peer_t) * (n - 1));
        if (peers == NULL) {
            return NGX_ERROR;
        }

        peers->single = (n == 1);
        peers->number = n;
        peers->name = &us->host;

        peers -> idx_peer_to_test = -1;
        peers -> peer_to_test = -1;
        (peers -> test_start_time).sec = (peers -> test_start_time).msec = 0;
        (peers -> last_test_req_time).sec = (peers -> last_test_req_time).msec = 0;
        peers -> n_tested_requests = 0;
        peers -> wl_index = 0;

        n = 0;

        for (i = 0; i < us->servers->nelts; i++) {
            for (j = 0; j < server[i].naddrs; j++) {
                if (server[i].backup) {
                    continue;
                }

                peers->peer[n].sockaddr = server[i].addrs[j].sockaddr;
                peers->peer[n].socklen = server[i].addrs[j].socklen;
                peers->peer[n].name = server[i].addrs[j].name;
                peers->peer[n].max_fails = server[i].max_fails;
                peers->peer[n].fail_timeout = server[i].fail_timeout;
                peers->peer[n].down = server[i].down;
                peers->peer[n].weight = server[i].down ? 0 : server[i].weight;
                peers->peer[n].current_weight = peers->peer[n].weight;
                n++;
            }
        }

        us->peer.data = peers;

        ngx_sort(&peers->peer[0], (size_t) n,
                 sizeof(ngx_http_upstream_rr_peer_t),
                 ngx_http_upstream_cmp_servers);

        /* backup servers */

        n = 0;

        for (i = 0; i < us->servers->nelts; i++) {
            if (!server[i].backup) {
                continue;
            }

            n += server[i].naddrs;
        }

        if (n == 0) {
            return NGX_OK;
        }

        backup = ngx_pcalloc(cf->pool, sizeof(ngx_http_upstream_conpaas_peers_t)
                              + sizeof(ngx_http_upstream_rr_peer_t) * (n - 1));
        if (backup == NULL) {
            return NGX_ERROR;
        }

        peers->single = 0;
        backup->single = 0;
        backup->number = n;
        backup->name = &us->host;

        n = 0;

        for (i = 0; i < us->servers->nelts; i++) {
            for (j = 0; j < server[i].naddrs; j++) {
                if (!server[i].backup) {
                    continue;
                }

                backup->peer[n].sockaddr = server[i].addrs[j].sockaddr;
                backup->peer[n].socklen = server[i].addrs[j].socklen;
                backup->peer[n].name = server[i].addrs[j].name;
                backup->peer[n].weight = server[i].weight;
                backup->peer[n].current_weight = server[i].weight;
                backup->peer[n].max_fails = server[i].max_fails;
                backup->peer[n].fail_timeout = server[i].fail_timeout;
                backup->peer[n].down = server[i].down;
                n++;
            }
        }

        peers->next = backup;

        ngx_sort(&backup->peer[0], (size_t) n,
                 sizeof(ngx_http_upstream_rr_peer_t),
                 ngx_http_upstream_cmp_servers);



        return NGX_OK;
    }


    /* an upstream implicitly defined by proxy_pass, etc. */

    ngx_log_debug0(NGX_LOG_NOTICE, cf -> log, 0, "ngx_http_upstream_init_conpaas(): upstream implicitly defined");

    if (us->port == 0 && us->default_port == 0) {
        ngx_log_error(NGX_LOG_EMERG, cf->log, 0,
                      "no port in upstream \"%V\" in %s:%ui",
                      &us->host, us->file_name, us->line);
        return NGX_ERROR;
    }

    ngx_memzero(&u, sizeof(ngx_url_t));

    u.host = us->host;
    u.port = (in_port_t) (us->port ? us->port : us->default_port);

    if (ngx_inet_resolve_host(cf->pool, &u) != NGX_OK) {
        if (u.err) {
            ngx_log_error(NGX_LOG_EMERG, cf->log, 0,
                          "%s in upstream \"%V\" in %s:%ui",
                          u.err, &us->host, us->file_name, us->line);
        }

        return NGX_ERROR;
    }

    n = u.naddrs;

    peers = ngx_pcalloc(cf->pool, sizeof(ngx_http_upstream_conpaas_peers_t)
                              + sizeof(ngx_http_upstream_rr_peer_t) * (n - 1));
    if (peers == NULL) {
        return NGX_ERROR;
    }

    peers->single = (n == 1);
    peers->number = n;
    peers->name = &us->host;

    for (i = 0; i < u.naddrs; i++) {
        peers->peer[i].sockaddr = u.addrs[i].sockaddr;
        peers->peer[i].socklen = u.addrs[i].socklen;
        peers->peer[i].name = u.addrs[i].name;
        peers->peer[i].weight = 1;
        peers->peer[i].current_weight = 1;
        peers->peer[i].max_fails = 1;
        peers->peer[i].fail_timeout = 10;
    }

    us->peer.data = peers;

    /* implicitly defined upstream has no backup servers */

    return NGX_OK;
}


/**
 * Sets the module's configuration based on the arguments given to the "conpaas" directive.
 */
static char *
ngx_http_upstream_conpaas(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_http_upstream_srv_conf_t  *uscf;
    ngx_http_script_compile_t      sc;
    ngx_str_t                     *value;
    ngx_array_t                   *vars_lengths, *vars_values;
    char							*p;
    ngx_http_upstream_conpaas_conf_t *conpaas_conf;

    value = cf->args->elts;

    ngx_log_debug(NGX_LOG_NOTICE, cf ->log, 0, "In ngx_http_upstream_conpaas()...");

    ngx_memzero(&sc, sizeof(ngx_http_script_compile_t));

    vars_lengths = NULL;
    vars_values = NULL;

    sc.cf = cf;
    sc.source = &value[1];
    sc.lengths = &vars_lengths;
    sc.values = &vars_values;
    sc.complete_lengths = 1;
    sc.complete_values = 1;

    if (ngx_http_script_compile(&sc) != NGX_OK) {
        return NGX_CONF_ERROR;
    }

    /* set the values in the configuration structure */
    p = conf;
    conpaas_conf = (ngx_http_upstream_conpaas_conf_t *) conf;
    /* interval = ngx_atoi(value[1].data, value[1].len); */
    conpaas_conf -> profiling_file = ngx_palloc(cf-> pool, sizeof(char) * (value[1].len + 1));
    if (conpaas_conf -> profiling_file == NULL) {
    	return NGX_CONF_ERROR;
    }
    strcpy(conpaas_conf -> profiling_file, (char *)value[1].data);
    /*
    conpaas_conf -> profile_interval = ngx_atoi(value[1].data, value[1].len);
    conpaas_conf -> profile_duration = ngx_atoi(value[2].data, value[2].len);
    */
    /*
    if (value[3].len > 30) {
    	ngx_log_error(NGX_LOG_EMERG, cf->log, 0, "Profiling configuration file name too large");
    	return NGX_CONF_ERROR;
    }
    */
    /* strcpy(conpaas_conf -> conf_file, (char *)(value[3].data)); */
    ngx_log_debug1(NGX_LOG_NOTICE, cf ->log, 0,
                      "Using profiling config file: %s", conpaas_conf -> profiling_file);

    uscf = ngx_http_conf_get_module_srv_conf(cf, ngx_http_upstream_module);

    /* set the upstream initialization function */
    uscf->peer.init_upstream = ngx_http_upstream_init_conpaas;

    uscf->flags = NGX_HTTP_UPSTREAM_CREATE;

    /*
    uscf->values = vars_values->elts;
    uscf->lengths = vars_lengths->elts;

    if (uscf->hash_function == NULL) {
        uscf->hash_function = ngx_hash_key;
    }
     */

    return NGX_CONF_OK;
}



static ngx_int_t
ngx_http_upstream_cmp_servers(const void *one, const void *two)
{
    ngx_http_upstream_rr_peer_t  *first, *second;

    first = (ngx_http_upstream_rr_peer_t *) one;
    second = (ngx_http_upstream_rr_peer_t *) two;

    return (first->weight < second->weight);
}

/**
 * Peer initialization function, called once for each HTTP request.
 * Sets up the ngx_http_upstream_conpaas_peer_data_t structure associated with the request.
 */
ngx_int_t
ngx_http_upstream_init_conpaas_peer(ngx_http_request_t *r,
    ngx_http_upstream_srv_conf_t *us)
{
    ngx_uint_t                         n;
    ngx_http_upstream_conpaas_peer_data_t  *rrp;
    struct sigaction sa;
    int ret;

    /* set signal handler */
    if (!signal_initialized) {
    	memset(&sa, 0, sizeof(sa));
    	sa.sa_handler = sigusr2_handler;
    	/* act.sa_flags = SA_SIGINFO; */
    	ret = sigaction(SIGUSR2, &sa, NULL);
    	ngx_log_error(NGX_LOG_NOTICE, r -> connection -> log, 0, "signal handler set: %d", ret);
    	signal_initialized = 1;
    }

    /* ngx_log_debug0(NGX_LOG_NOTICE, r -> connection ->log, 0, "In ngx_http_upstream_init_conpaas_peer()..."); */
    ngx_log_error(NGX_LOG_NOTICE, r -> connection ->log, 0, "init_conpaas_peer");

    rrp = r->upstream->peer.data;

    if (rrp == NULL) {
        rrp = ngx_palloc(r->pool, sizeof(ngx_http_upstream_conpaas_peer_data_t));
        if (rrp == NULL) {
            return NGX_ERROR;
        }

        r->upstream->peer.data = rrp;
    }

    rrp->peers = us->peer.data;
    rrp->current = 0;

    if (rrp->peers->number <= 8 * sizeof(uintptr_t)) {
        rrp->tried = &rrp->data;
        rrp->data = 0;

    } else {
        n = (rrp->peers->number + (8 * sizeof(uintptr_t) - 1))
                / (8 * sizeof(uintptr_t));

        rrp->tried = ngx_pcalloc(r->pool, n * sizeof(uintptr_t));
        if (rrp->tried == NULL) {
            return NGX_ERROR;
        }
    }

    /* store the module configuration parameters */
    rrp -> module_conf =  ngx_http_get_module_loc_conf(r, ngx_http_upstream_conpaas_module);

    r->upstream->peer.get = ngx_http_upstream_get_conpaas_peer;
    r->upstream->peer.free = ngx_http_upstream_free_conpaas_peer;
    r->upstream->peer.tries = rrp->peers->number;

    return NGX_OK;
}

#ifdef gigel
ngx_int_t
ngx_http_upstream_create_conpaas_peer(ngx_http_request_t *r,
    ngx_http_upstream_resolved_t *ur)
{
    u_char                            *p;
    size_t                             len;
    ngx_uint_t                         i, n;
    struct sockaddr_in                *sin;
    ngx_http_upstream_rr_peers_t      *peers;
    ngx_http_upstream_conpaas_peer_data_t  *rrp;

    rrp = r->upstream->peer.data;

    if (rrp == NULL) {
        rrp = ngx_palloc(r->pool, sizeof(ngx_http_upstream_conpaas_peer_data_t));
        if (rrp == NULL) {
            return NGX_ERROR;
        }

        r->upstream->peer.data = rrp;
    }

    peers = ngx_pcalloc(r->pool, sizeof(ngx_http_upstream_rr_peers_t)
                     + sizeof(ngx_http_upstream_rr_peer_t) * (ur->naddrs - 1));
    if (peers == NULL) {
        return NGX_ERROR;
    }

    peers->single = (ur->naddrs == 1);
    peers->number = ur->naddrs;
    peers->name = &ur->host;

    if (ur->sockaddr) {
        peers->peer[0].sockaddr = ur->sockaddr;
        peers->peer[0].socklen = ur->socklen;
        peers->peer[0].name = ur->host;
        peers->peer[0].weight = 1;
        peers->peer[0].current_weight = 1;
        peers->peer[0].max_fails = 1;
        peers->peer[0].fail_timeout = 10;

    } else {

        for (i = 0; i < ur->naddrs; i++) {

            len = NGX_INET_ADDRSTRLEN + sizeof(":65536") - 1;

            p = ngx_pnalloc(r->pool, len);
            if (p == NULL) {
                return NGX_ERROR;
            }

            len = ngx_inet_ntop(AF_INET, &ur->addrs[i], p, NGX_INET_ADDRSTRLEN);
            len = ngx_sprintf(&p[len], ":%d", ur->port) - p;

            sin = ngx_pcalloc(r->pool, sizeof(struct sockaddr_in));
            if (sin == NULL) {
                return NGX_ERROR;
            }

            sin->sin_family = AF_INET;
            sin->sin_port = htons(ur->port);
            sin->sin_addr.s_addr = ur->addrs[i];

            peers->peer[i].sockaddr = (struct sockaddr *) sin;
            peers->peer[i].socklen = sizeof(struct sockaddr_in);
            peers->peer[i].name.len = len;
            peers->peer[i].name.data = p;
            peers->peer[i].weight = 1;
            peers->peer[i].current_weight = 1;
            peers->peer[i].max_fails = 1;
            peers->peer[i].fail_timeout = 10;
        }
    }

    rrp->peers = peers;
    rrp->current = 0;

    if (rrp->peers->number <= 8 * sizeof(uintptr_t)) {
        rrp->tried = &rrp->data;
        rrp->data = 0;

    } else {
        n = (rrp->peers->number + (8 * sizeof(uintptr_t) - 1))
                / (8 * sizeof(uintptr_t));

        rrp->tried = ngx_pcalloc(r->pool, n * sizeof(uintptr_t));
        if (rrp->tried == NULL) {
            return NGX_ERROR;
        }
    }

    r->upstream->peer.get = ngx_http_upstream_get_conpaas_peer;
    r->upstream->peer.free = ngx_http_upstream_free_conpaas_peer;
    r->upstream->peer.tries = rrp->peers->number;

    return NGX_OK;
}
#endif


ngx_int_t
ngx_http_upstream_get_conpaas_peer(ngx_peer_connection_t *pc, void *data)
{
    ngx_http_upstream_conpaas_peer_data_t  *rrp = data;

    time_t                     		now;
    uintptr_t                      m;
    ngx_int_t                      rc;
    ngx_uint_t                     i, n;
    ngx_connection_t              *c;
    ngx_http_upstream_rr_peer_t   *peer;
    ngx_http_upstream_conpaas_peers_t  *peers;

    /*
    ngx_log_debug1(NGX_LOG_DEBUG_HTTP, pc->log, 0,
                   "get rr peer, try: %ui", pc->tries);
    */
    now = ngx_time();

    ngx_log_debug3(NGX_LOG_DEBUG_HTTP, pc->log, 0,
                       "ngx_http_upstream_get_conpaas_peer(), try: %ui, time: %l, profiling: %d",
                       pc->tries, now, sig_profiling);

    /* ngx_lock_mutex(rrp->peers->mutex); */

    if (rrp->peers->last_cached) {

        /* cached connection */
    	ngx_log_debug1(NGX_LOG_DEBUG_HTTP, pc -> log, 0, "Cached connection: %ui", rrp -> peers -> last_cached);

        c = rrp->peers->cached[rrp->peers->last_cached];
        rrp->peers->last_cached--;

        /* ngx_unlock_mutex(ppr->peers->mutex); */

#if (NGX_THREADS)
        c->read->lock = c->read->own_lock;
        c->write->lock = c->write->own_lock;
#endif

        pc->connection = c;
        pc->cached = 1;

        return NGX_OK;
    }

    pc->cached = 0;
    pc->connection = NULL;

    if (rrp->peers->single) {
        peer = &rrp->peers->peer[0];

    } else {

        /* there are several peers */

        if (pc->tries == rrp->peers->number) {

            /* it's a first try - get a current peer */

            i = pc->tries;

            for ( ;; ) {
                rrp->current = ngx_http_upstream_conpaas_select_peer(rrp->peers, rrp->module_conf, pc);

                ngx_log_debug2(NGX_LOG_DEBUG_HTTP, pc->log, 0,
                               "ngx_http_upstream_get_conpaas_peer(), current: %ui, weight: %i",
                               rrp->current,
                               rrp->peers->peer[rrp->current].current_weight);

                n = rrp->current / (8 * sizeof(uintptr_t));
                m = (uintptr_t) 1 << rrp->current % (8 * sizeof(uintptr_t));

                if (!(rrp->tried[n] & m)) {
                    peer = &rrp->peers->peer[rrp->current];

                    if (!peer->down) {

                        if (peer->max_fails == 0
                            || peer->fails < peer->max_fails)
                        {
                            break;
                        }

                        if (now - peer->accessed > peer->fail_timeout) {
                            peer->fails = 0;
                            break;
                        }

                        peer->current_weight = 0;

                    } else {
                        rrp->tried[n] |= m;
                    }

                    pc->tries--;
                }

                if (pc->tries == 0) {
                    goto failed;
                }

                if (--i == 0) {
                    ngx_log_error(NGX_LOG_ALERT, pc->log, 0,
                                  "round robin upstream stuck on %ui tries",
                                  pc->tries);
                    goto failed;
                }
            }

            peer->current_weight--;

        } else { /* this is not the first try */

            i = pc->tries;

            for ( ;; ) {
                n = rrp->current / (8 * sizeof(uintptr_t));
                m = (uintptr_t) 1 << rrp->current % (8 * sizeof(uintptr_t));

                if (!(rrp->tried[n] & m)) {

                    peer = &rrp->peers->peer[rrp->current];

                    if (!peer->down) {

                        if (peer->max_fails == 0
                            || peer->fails < peer->max_fails)
                        {
                            break;
                        }

                        if (now - peer->accessed > peer->fail_timeout) {
                            peer->fails = 0;
                            break;
                        }

                        peer->current_weight = 0;

                    } else {
                        rrp->tried[n] |= m;
                    }

                    pc->tries--;
                }

                rrp->current++;

                if (rrp->current >= rrp->peers->number) {
                    rrp->current = 0;
                }

                if (pc->tries == 0) {
                    goto failed;
                }

                if (--i == 0) {
                    ngx_log_error(NGX_LOG_ALERT, pc->log, 0,
                                  "round robin upstream stuck on %ui tries",
                                  pc->tries);
                    goto failed;
                }
            }

            peer->current_weight--;
        }

        rrp->tried[n] |= m;
    }

    /* set the data for the peer we have chosen */
    pc->sockaddr = peer->sockaddr;
    pc->socklen = peer->socklen;
    pc->name = &peer->name;

    /* ngx_unlock_mutex(rrp->peers->mutex); */

    if (pc->tries == 1 && rrp->peers->next) {
        pc->tries += rrp->peers->next->number;

        n = rrp->peers->next->number / (8 * sizeof(uintptr_t)) + 1;
        for (i = 0; i < n; i++) {
             rrp->tried[i] = 0;
        }
    }

    return NGX_OK;

failed:

    peers = rrp->peers;

    if (peers->next) {

        /* ngx_unlock_mutex(peers->mutex); */

        ngx_log_debug0(NGX_LOG_DEBUG_HTTP, pc->log, 0, "backup servers");

        rrp->peers = peers->next;
        pc->tries = rrp->peers->number;

        n = rrp->peers->number / (8 * sizeof(uintptr_t)) + 1;
        for (i = 0; i < n; i++) {
             rrp->tried[i] = 0;
        }

        rc = ngx_http_upstream_get_conpaas_peer(pc, rrp);

        if (rc != NGX_BUSY) {
            return rc;
        }

        /* ngx_lock_mutex(peers->mutex); */
    }

    /* all peers failed, mark them as live for quick recovery */

    for (i = 0; i < peers->number; i++) {
        peers->peer[i].fails = 0;
    }

    /* ngx_unlock_mutex(peers->mutex); */

    pc->name = peers->name;

    return NGX_BUSY;
}

/**
 * Selects a peer to serve a request. This function implements the load
 * balancing policy.
 */
static ngx_uint_t
ngx_http_upstream_conpaas_select_peer(ngx_http_upstream_conpaas_peers_t *peers,
		ngx_http_upstream_conpaas_conf_t *conf, ngx_peer_connection_t *pc)
{
    ngx_uint_t                    i, n;
    ngx_int_t						elapsed_msecs;
    ngx_http_upstream_rr_peer_t  *peer;
    ngx_time_t now;
    float							current_req_rate;

    peer = &peers->peer[0];

    if (peers -> peer_to_test < 0 && sig_profiling == 1) { /* start profiling */
    	sig_profiling = 0;

    	/* read the profiling configuration from the file */
    	get_profiling_config(conf -> profiling_file, peers -> number);

    	if (n_profile_peers > 0) {
    		/* start with the first peer */
    		peers -> idx_peer_to_test = 0;
    		peers -> peer_to_test = profile_peers[peers -> idx_peer_to_test];
    		ngx_log_debug1(NGX_LOG_DEBUG_HTTP, pc->log, 0,
    		    		                    "conpaas_load_balancing: profiling peer %d...", peers -> peer_to_test);
    		/* start with the first workload value from the list */
    		peers -> wl_index = 0;

    		ngx_log_debug1(NGX_LOG_DEBUG_HTTP, pc->log, 0,
    				"conpaas_load_balancing: start profiling, profile time: %d", profile_time);
    	}
    }

    now = *ngx_timeofday();
    if (peers -> peer_to_test >= 0) { /* we are in profiling mode */
    	if ((peers -> test_start_time.sec != 0 ) &&
    			(ngx_difftime_msec(now, peers -> test_start_time) > 1000 * profile_time)) { /* we finished testing the current workload */
    			/* (now.sec > (peers -> test_start_time.sec + profile_time))) { */
    		/* move to the next workload value, reset timers */
    		(peers -> wl_index)++;
    		if (peers -> wl_index < n_workloads) {
    			ngx_log_debug2(NGX_LOG_DEBUG_HTTP, pc->log, 0,
                    "conpaas_load_balancing: switching to workload no. %d, req/sec: %d", peers -> wl_index, workloads[peers -> wl_index]);
    		}
    		(peers -> last_test_req_time).sec = (peers -> last_test_req_time).msec = 0;
    		(peers -> test_start_time).sec = (peers -> test_start_time).msec = 0;
    		peers -> n_tested_requests = 0;
    	}

    	if (peers -> wl_index < n_workloads) {
    		if ((peers -> last_test_req_time).sec == 0) { /* first test request for this workload */
    			peers -> last_test_req_time = now;
    			peers -> test_start_time = now;
    			peers -> n_tested_requests = 1;
    			ngx_log_debug1(NGX_LOG_DEBUG_HTTP, pc->log, 0,
    			    	"conpaas_load_balancing: first time choosing tested peer: %d", peers -> peer_to_test);
    			return peers -> peer_to_test;
    		}

    		//elapsed_msecs = ngx_difftime_msec(now, peers -> last_test_req_time);
    		elapsed_msecs = ngx_difftime_msec(now, peers -> test_start_time);
    		current_req_rate = (float)(peers -> n_tested_requests * 1000) / elapsed_msecs;
    		ngx_log_debug2(NGX_LOG_DEBUG_HTTP, pc->log, 0,
    		    "conpaas_load_balancing: elapsed_msecs %d, req_rate %f", elapsed_msecs, current_req_rate);
    		//if (elapsed_msecs * workloads[peers -> wl_index] >= 1000) {
    		if (current_req_rate < workloads[peers -> wl_index]) {
    			/* we should send the request to the tested peer */
    			peers -> last_test_req_time = now;
    			(peers -> n_tested_requests)++;
    			ngx_log_debug1(NGX_LOG_DEBUG_HTTP, pc->log, 0,
    			    "conpaas_load_balancing: choosing tested peer: %d", peers -> peer_to_test);
    			return peers -> peer_to_test;
    		}
    	} else { /* we are done testing this peer */
    		(peers -> idx_peer_to_test)++; /* move to the next peer */
    		/* if (peers -> peer_to_test >= (ngx_int_t)(peers -> number)) { */
    		if (peers -> idx_peer_to_test >= n_profile_peers) {
    		    /* we are done testing all the peers, reset the peer counter */
    		    peers -> peer_to_test = -1;
    		    ngx_log_debug0(NGX_LOG_DEBUG_HTTP, pc->log, 0,
    		                        "conpaas_load_balancing: done profiling...");
    		} else {
    			peers -> peer_to_test = profile_peers[peers -> idx_peer_to_test];
    			peers -> wl_index = 0;
    			ngx_log_debug1(NGX_LOG_DEBUG_HTTP, pc->log, 0,
    		                    "conpaas_load_balancing: profiling peer %d...", peers -> peer_to_test);
    		}
    	}
    }

    for ( ;; ) {

        for (i = 0; i < peers->number; i++) {

        	ngx_log_debug3(NGX_LOG_DEBUG_HTTP, pc->log, 0,
        	    	"conpaas_load_balancing: peer_to_test %d peer %d weight %d", peers -> peer_to_test, i, peer[i].current_weight);

            if (peer[i].current_weight <= 0 || ((ngx_int_t)i == peers -> peer_to_test)) {
                continue;
            }

            n = i;

            while (i < peers->number - 1) {

                i++;

                if (peer[i].current_weight <= 0 || ((ngx_int_t)i == peers -> peer_to_test)) {
                    continue;
                }

                if (peer[n].current_weight * 1000 / peer[i].current_weight
                    > peer[n].weight * 1000 / peer[i].weight)
                {
                	ngx_log_debug1(NGX_LOG_DEBUG_HTTP, pc->log, 0,
                	 "conpaas_load_balancing: normal return 1 peer %d...", n);
                    return n;
                }

                n = i;
            }

            if (peer[i].current_weight > 0 && ((ngx_int_t)i != peers -> peer_to_test)) {
                n = i;
            }

            ngx_log_debug1(NGX_LOG_DEBUG_HTTP, pc->log, 0,
              "conpaas_load_balancing: normal return 2 peer %d...", n);
            return n;
        }

        for (i = 0; i < peers->number; i++) {
            peer[i].current_weight = peer[i].weight;
        }
    }
}


void
ngx_http_upstream_free_conpaas_peer(ngx_peer_connection_t *pc, void *data,
    ngx_uint_t state)
{
    ngx_http_upstream_conpaas_peer_data_t  *rrp = data;

    time_t                       now;
    ngx_http_upstream_rr_peer_t  *peer;

    ngx_log_debug2(NGX_LOG_DEBUG_HTTP, pc->log, 0,
                   "free rr peer %ui %ui", pc->tries, state);

    if (state == 0 && pc->tries == 0) {
        return;
    }

    /* TODO: NGX_PEER_KEEPALIVE */

    if (rrp->peers->single) {
        pc->tries = 0;
        return;
    }

    if (state & NGX_PEER_FAILED) {
        now = ngx_time();

        peer = &rrp->peers->peer[rrp->current];

        /* ngx_lock_mutex(rrp->peers->mutex); */

        peer->fails++;
        peer->accessed = now;

        if (peer->max_fails) {
            peer->current_weight -= peer->weight / peer->max_fails;
        }

        ngx_log_debug2(NGX_LOG_DEBUG_HTTP, pc->log, 0,
                       "free rr peer failed: %ui %i",
                       rrp->current, peer->current_weight);

        if (peer->current_weight < 0) {
            peer->current_weight = 0;
        }

        /* ngx_unlock_mutex(rrp->peers->mutex); */
    }

    rrp->current++;

    if (rrp->current >= rrp->peers->number) {
        rrp->current = 0;
    }

    if (pc->tries) {
        pc->tries--;
    }

    /* ngx_unlock_mutex(rrp->peers->mutex); */
}

void sigusr2_handler(int signum) {
	/* ngx_log_debug1(NGX_LOG_NOTICE, "Signal originates from process %lu\n",
	        (unsigned long)info->si_pid); */
	sig_profiling = 1;
}

/** Reads the profiling parameters from the profiling configuration file. */
static ngx_int_t get_profiling_config(char *filename, int n_total_peers) {
	char line[128];
	char *token, *res;
	int i;

	FILE *file = fopen(filename, "r");

	if (file == NULL) {
		return -1;
	}

	/* read the first line which specifies the profile time for a given workload value */
	res = fgets(line, sizeof(line), file);
	if (res == NULL) {
		return -1;
	}
	char *s_profile_time = strtok(line, " \n\t");
	profile_time = atoi(s_profile_time);

	/* read the second line which specifies the list of peers to profile */
	res = fgets(line, sizeof(line), file);
	if (res == NULL) {
		return -1;
	}
	/* if the line contains "all" then all peers will be profiled */
	if (strstr(res, "all") != NULL) {
		n_profile_peers = n_total_peers;
		for (i = 0; i < n_total_peers; i++) {
			profile_peers[i] = i;
		}
	} else { /* the line contains a set of peers separated by spaces */
		token = strtok(line, " \n\t");
		n_profile_peers = 0;
		do {
			profile_peers[n_profile_peers] = atoi(token);
			token = strtok(NULL, " \n\t");
			n_profile_peers++;
		} while (token != NULL);
	}

	/* read the third line which specifies the list of workload values */
	res = fgets(line, sizeof(line), file);
	if (res == NULL) {
		return -1;
	}
	token = strtok(line, " \n\t");
	n_workloads = 0;
	do {
		workloads[n_workloads] = atoi(token);
		token = strtok(NULL, " \n\t");
		n_workloads++;
	} while (token != NULL);

	fclose ( file );

	return 0;
}

static ngx_int_t ngx_difftime_msec(ngx_time_t t1, ngx_time_t t0) {
	ngx_int_t d_sec, d_msec;

	d_sec = t1.sec - t0.sec;
	if (t1.msec > t0.msec) {
		d_msec = t1.msec - t0.msec;
	} else {
		d_sec--;
		d_msec = (1000 + t1.msec) - t0.msec;
	}

	return 1000 * d_sec + d_msec;
}
