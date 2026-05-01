// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <stdatomic.h>
#include <sys/stat.h>
#include <unistd.h>
#include <uv.h>
#include "a-raft-library/araft.h"
#include "h2o-c-library/h2o_c.h"
#include "the-macro-library/macro_map.h"

char* g_argv0 = NULL;
char* g_argv1 = NULL;

// --- PENDING REQUEST REGISTRY (HTTP <-> Raft Bridge) ---

#define MAX_PENDING_REQUESTS 1024

typedef struct {
    _Atomic uint64_t request_id;
    _Atomic int status;
} pending_request_t;

pending_request_t g_pending_reqs[MAX_PENDING_REQUESTS];
_Atomic uint64_t g_request_counter = 1;

uint64_t register_pending_request() {
    uint64_t req_id = atomic_fetch_add(&g_request_counter, 1);
    uint32_t slot = req_id % MAX_PENDING_REQUESTS;
    atomic_store(&g_pending_reqs[slot].request_id, req_id);
    atomic_store(&g_pending_reqs[slot].status, 1);
    return req_id;
}

void araft_on_forward_response(uint64_t request_id, uint8_t success) {
    (void)success;
    uint32_t slot = request_id % MAX_PENDING_REQUESTS;
    if (atomic_load(&g_pending_reqs[slot].request_id) == request_id) {
        atomic_store(&g_pending_reqs[slot].status, 2);
    }
}

// --- THE STATE MACHINE (Intrusive Red-Black Tree) ---

typedef struct {
    macro_map_t link;
    char name[64];
    char phone[32];
} contact_t;

static int cmp_contact(const contact_t* a, const contact_t* b) { return strcmp(a->name, b->name); }
static int cmp_name(const char** key, const contact_t* node) { return strcmp(*key, node->name); }

macro_map_insert(contact_insert, contact_t, cmp_contact)
macro_map_find_kv(contact_find, char*, contact_t, cmp_name)

macro_map_t* g_contacts = NULL;
pthread_rwlock_t g_sm_lock = PTHREAD_RWLOCK_INITIALIZER;

int get_http_port_for_node(uint64_t node_id) {
    if (node_id == 100) return 8081;
    if (node_id == 200) return 8082;
    if (node_id == 300) return 8083;
    return 8080;
}

bool wants_redirect(h2o_c_header_t* headers) {
    h2o_c_header_t* h = headers;
    while (h) {
        if (strcasecmp(h->key, "X-Raft-Redirect") == 0 && strcasecmp(h->value, "true") == 0) return true;
        h = h->next;
    }
    return false;
}

void apply_to_state_machine(const char* payload, uint32_t len) {
    const char* colon = memchr(payload, ':', len);
    if (!colon) return;

    size_t name_len = colon - payload;
    size_t phone_len = len - name_len - 1;
    if (name_len > 63) name_len = 63;
    if (phone_len > 31) phone_len = 31;

    char search_name[64] = {0};
    memcpy(search_name, payload, name_len);

    pthread_rwlock_wrlock(&g_sm_lock);
    const char* k = search_name;
    contact_t* existing = contact_find(g_contacts, &k);

    if (existing) {
        memset(existing->phone, 0, sizeof(existing->phone));
        memcpy(existing->phone, colon + 1, phone_len);
    } else {
        contact_t* c = malloc(sizeof(contact_t));
        memset(c, 0, sizeof(contact_t));
        memcpy(c->name, search_name, name_len);
        memcpy(c->phone, colon + 1, phone_len);
        contact_insert(&g_contacts, c);
    }
    pthread_rwlock_unlock(&g_sm_lock);
}

void* state_machine_worker(void* arg) {
    araft_node_t* node = (araft_node_t*)arg;
    uint64_t term, index;
    uint8_t* payload;
    uint32_t len;

    printf("[Worker] Starting WAL Demux loop...\n");
    while (true) {
        // awal_demux_step is now mathematically safe. It only returns entries <= commit_index
        if (awal_demux_step(&node->wal, &term, &index, &payload, &len) == 1) {
            if (payload && len > 0) {
                apply_to_state_machine((const char*)payload, len);
                free(payload);
            }
        } else {
            usleep(2000);
        }
    }
    return NULL;
}

// Helper to poll state machine for confirmation
bool wait_for_state_machine_apply(const char* target_name, int timeout_ms) {
    int cycles = 0;
    while (cycles < (timeout_ms / 2)) {
        pthread_rwlock_rdlock(&g_sm_lock);
        contact_t* c = contact_find(g_contacts, &target_name);
        pthread_rwlock_unlock(&g_sm_lock);

        if (c) return true;
        usleep(2000);
        cycles++;
    }
    return false;
}

h2o_c_response_t* handle_set(void* arg, const char* method, const char* path, h2o_c_header_t* headers, const char* body, size_t body_len) {
    (void)method;
    araft_node_t* raft_node = (araft_node_t*)arg;

    // Extract name to verify commit
    const char* colon = memchr(body, ':', body_len);
    if (!colon) return h2o_c_make_response(400, "Bad Request", "Invalid Format", 14, "text/plain");

    char search_name[64] = {0};
    size_t name_len = colon - body;
    if (name_len > 63) name_len = 63;
    memcpy(search_name, body, name_len);
    const char* k = search_name;

    if (raft_node->state == ARAFT_STATE_LEADER) {
        araft_propose(raft_node, (const uint8_t*)body, body_len);

        // FIXED: Fictional Success. Do not return OK until a Raft majority commits it and it is applied.
        if (wait_for_state_machine_apply(k, 5000)) {
            return h2o_c_make_response(200, "OK", "Contact safely committed by Leader", 34, "text/plain");
        }
        return h2o_c_make_response(504, "Gateway Timeout", "Timed out waiting for majority commit", 37, "text/plain");
    }

    if (raft_node->current_leader_id == 0) {
        return h2o_c_make_response(503, "Service Unavailable", "Election pending", 16, "text/plain");
    }

    if (wants_redirect(headers)) {
        int leader_port = get_http_port_for_node(raft_node->current_leader_id);
        char redirect_url[256];
        snprintf(redirect_url, sizeof(redirect_url), "http://127.0.0.1:%d%s", leader_port, path);

        h2o_c_response_t* resp = h2o_c_make_response(307, "Temporary Redirect", NULL, 0, NULL);
        h2o_c_header_t* loc = calloc(1, sizeof(h2o_c_header_t));
        loc->key = strdup("Location");
        loc->value = strdup(redirect_url);
        resp->headers = loc;
        return resp;
    }

    uint64_t req_id = register_pending_request();
    uint32_t slot = req_id % MAX_PENDING_REQUESTS;

    araft_forward_request(raft_node, req_id, (const uint8_t*)body, body_len);

    int wait_cycles = 0;
    while (atomic_load(&g_pending_reqs[slot].status) == 1) {
        usleep(2000);
        if (++wait_cycles > 2500) {
            return h2o_c_make_response(504, "Gateway Timeout", "Leader did not respond in time", 30, "text/plain");
        }
    }

    // FIXED: Wait for follower to actually catch up to the leader's append before returning success
    if (wait_for_state_machine_apply(k, 5000)) {
        return h2o_c_make_response(200, "OK", "Data proxied and successfully committed!", 40, "text/plain");
    }
    return h2o_c_make_response(504, "Gateway Timeout", "Follower timed out applying commit", 34, "text/plain");
}

h2o_c_response_t* handle_get(void* arg, const char* method, const char* path, h2o_c_header_t* headers, const char* body, size_t body_len) {
    (void)arg; (void)method; (void)path; (void)headers;
    araft_node_t* raft_node = (araft_node_t*)arg;

    if (raft_node->current_leader_id == 0) {
        return h2o_c_make_response(503, "Service Unavailable", "Election pending", 16, "text/plain");
    }

    bool is_caught_up = (raft_node->commit_index >= raft_node->known_leader_commit);

    if (!is_caught_up && raft_node->state != ARAFT_STATE_LEADER) {
        printf("[HTTP] Node is lagging (Local Commit: %llu, Leader Commit: %llu). Redirecting GET to Leader...\n",
               raft_node->commit_index, raft_node->known_leader_commit);

        int leader_port = get_http_port_for_node(raft_node->current_leader_id);
        char redirect_url[256];
        snprintf(redirect_url, sizeof(redirect_url), "http://127.0.0.1:%d%s", leader_port, path);

        h2o_c_response_t* resp = h2o_c_make_response(307, "Temporary Redirect", NULL, 0, NULL);
        h2o_c_header_t* loc = calloc(1, sizeof(h2o_c_header_t));
        loc->key = strdup("Location");
        loc->value = strdup(redirect_url);
        resp->headers = loc;
        return resp;
    }

    char search_name[64] = {0};
    size_t n_len = body_len < 63 ? body_len : 63;
    memcpy(search_name, body, n_len);

    pthread_rwlock_rdlock(&g_sm_lock);
    const char* k = search_name;
    contact_t* c = contact_find(g_contacts, &k);

    h2o_c_response_t* resp;
    if (c) {
        resp = h2o_c_make_response(200, "OK", c->phone, strlen(c->phone), "text/plain");
    } else {
        resp = h2o_c_make_response(404, "Not Found", "Contact not found", 17, "text/plain");
    }
    pthread_rwlock_unlock(&g_sm_lock);
    return resp;
}

// --- CHAOS API HANDLERS ---
h2o_c_response_t* handle_isolate(void* arg, const char* method, const char* path, h2o_c_header_t* headers, const char* body, size_t body_len) {
    (void)method; (void)path; (void)headers; (void)body; (void)body_len;
    araft_node_t* node = (araft_node_t*)arg;
    printf("\n[CHAOS] Severing all network connections!\n");
    // FIXED: This is now safely queued to the LibUV loop asynchronously
    araft_server_isolate(node->server);
    return h2o_c_make_response(200, "OK", "Network isolated.", 17, "text/plain");
}

h2o_c_response_t* handle_reconnect(void* arg, const char* method, const char* path, h2o_c_header_t* headers, const char* body, size_t body_len) {
    (void)method; (void)path; (void)headers; (void)body; (void)body_len;
    araft_node_t* node = (araft_node_t*)arg;
    printf("\n[CHAOS] Restoring network connections.\n");
    araft_server_reconnect(node->server);
    return h2o_c_make_response(200, "OK", "Network restored.", 17, "text/plain");
}

h2o_c_response_t* handle_crash(void* arg, const char* method, const char* path, h2o_c_header_t* headers, const char* body, size_t body_len) {
    (void)method; (void)path; (void)headers; (void)body; (void)body_len;
    araft_node_t* node = (araft_node_t*)arg;

    printf("\n[CHAOS] CATASTROPHIC HARDWARE FAILURE!\n");
    printf("[CHAOS] Wiping SSD (rm -rf %s/*)...\n", node->server->data_dir);

    char cmd[256];
    snprintf(cmd, sizeof(cmd), "rm -rf %s/*", node->server->data_dir);
    system(cmd);

    printf("[CHAOS] Restarting node from scratch natively via execvp...\n\n");

    char* args[] = {g_argv0, g_argv1, NULL};
    execvp(g_argv0, args);

    exit(1);
    return NULL;
}

void* start_h2o_server(void* arg) {
    (void)arg;
    h2o_c_run();
    return NULL;
}

// --- MULTI-NODE DISCOVERY MESH ---
void on_mesh_timer(uv_timer_t* handle) {
    physical_server_t* server = (physical_server_t*)handle->data;

    if (server->network_isolated) return;

    int target_nodes[] = {100, 200, 300};
    int target_ports[] = {9001, 9002, 9003};

    for (int i = 0; i < 3; i++) {
        if (target_nodes[i] == server->physical_node_id) continue;

        bool connected = false;
        for (uint32_t p = 0; p < server->active_peer_count; p++) {
            if (server->active_peers[p]->remote_node_id == target_nodes[i]) {
                connected = true;
                break;
            }
        }

        if (!connected) {
            araft_server_connect(server, "127.0.0.1", target_ports[i], target_nodes[i]);
        }
    }
}

// --- ENTRY POINT ---
int main(int argc, char** argv) {
    if (argc < 2) {
        printf("Usage: ./server <node_id: 100, 200, or 300>\n");
        return 1;
    }

    g_argv0 = argv[0];
    g_argv1 = argv[1];

    uint64_t node_id = atoi(argv[1]);
    int raft_port = (node_id == 100) ? 9001 : (node_id == 200) ? 9002 : 9003;
    int http_port = (node_id == 100) ? 8081 : (node_id == 200) ? 8082 : 8083;

    char data_dir[128];
    snprintf(data_dir, sizeof(data_dir), "data/node_%llu", node_id);

    mkdir("data", 0755);
    mkdir(data_dir, 0755);

    uv_loop_t* loop = uv_default_loop();

    physical_server_t server;
    araft_server_init(&server, loop, node_id, 10, data_dir);
    araft_server_set_forward_cb(&server, araft_on_forward_response);
    araft_server_listen(&server, "127.0.0.1", raft_port);

    araft_node_t shard_0;
    araft_node_init(&shard_0, &server, 0, 3);

    uv_timer_t mesh_timer;
    uv_timer_init(loop, &mesh_timer);
    mesh_timer.data = &server;
    uv_timer_start(&mesh_timer, (uv_timer_cb)on_mesh_timer, 1000, 1000);

    pthread_t sm_thread;
    pthread_create(&sm_thread, NULL, state_machine_worker, &shard_0);

    // Bootstrap Election Timer
    uv_timer_start(&shard_0.election_timer, araft_on_election_timeout, 150 + (rand() % 150), 0);

    h2o_c_options_t h2o_opts = {0};
    h2o_opts.port = http_port;
    h2o_opts.address = "127.0.0.1";
    h2o_opts.thread_pool_size = 1;

    h2o_c_init(&h2o_opts);

    h2o_c_use("POST", "/set", handle_set, &shard_0);
    h2o_c_use("POST", "/get", handle_get, &shard_0);

    h2o_c_use("POST", "/isolate", handle_isolate, &shard_0);
    h2o_c_use("POST", "/reconnect", handle_reconnect, &shard_0);
    h2o_c_use("POST", "/crash", handle_crash, &shard_0);

    pthread_t h2o_thread;
    pthread_create(&h2o_thread, NULL, start_h2o_server, NULL);

    printf("--- Node %llu Started (HTTP: %d, Raft: %d) ---\n", node_id, http_port, raft_port);
    printf("Data stored in: %s\n", data_dir);

    return uv_run(loop, UV_RUN_DEFAULT);
}
