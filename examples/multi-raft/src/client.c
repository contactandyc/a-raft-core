// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <unistd.h>

void print_usage(const char* prog_name) {
    printf("--- Raft 3-Node Interactive CLI Client ---\n\n");
    printf("Usage: %s <port> <op> [payload] [redirect]\n", prog_name);
    printf("   OR: %s suite\n\n", prog_name);
}

void run_curl(const char* description, const char* command) {
    printf("\n======================================================\n");
    printf("TEST: %s\n", description);
    printf("EXEC: %s\n", command);
    printf("------------------------------------------------------\n");
    system(command);
    printf("\n");
}

void run_automated_suite() {
    printf("\n######################################################\n");
    printf("         INITIATING AUTOMATED CHAOS SUITE\n");
    printf("######################################################\n");

    run_curl("Establish Baseline (Insert Alice via Leader)",
             "curl -s -X POST http://127.0.0.1:8081/set -d 'Alice:555-0001'");
    sleep(1);

    run_curl("Verify Replication (Read Alice from Node 200)",
             "curl -s -L -X POST http://127.0.0.1:8082/get -d 'Alice'");

    run_curl("HARDWARE FAILURE (Crash and Wipe Node 200)",
             "curl -s -X POST http://127.0.0.1:8082/crash");

    printf("\n[SLEEP] Waiting 3 seconds for Node 200 to execvp() and boot an empty drive...\n");
    sleep(3);

    run_curl("System Degraded Write (Insert Bob via Leader)",
             "curl -s -X POST http://127.0.0.1:8081/set -d 'Bob:555-0002'");

    usleep(100000);

    run_curl("Stale Read Check (Node 200 boots empty. Should gracefully forward to Leader)",
             "curl -s -L -v -X POST http://127.0.0.1:8082/get -d 'Bob' 2>&1 | grep -E '< HTTP|< Location|> POST'");

    printf("\n[SLEEP] Waiting 4 seconds for the Catch-Up loop to backfill Node 200's SSD...\n");
    sleep(4);

    // FIXED: Removed the '-L' redirect flag. We must assert that Node 200 actually
    // restored its WAL and populated its local State Machine locally.
    run_curl("Verify True Local Self-Healing (Read Bob DIRECTLY from recovered Node 200)",
             "curl -s -X POST http://127.0.0.1:8082/get -d 'Bob'");

    run_curl("NETWORK PARTITION (Isolate Node 300)",
             "curl -s -X POST http://127.0.0.1:8083/isolate");

    printf("\n[SLEEP] Waiting 2 seconds for cluster to realize 300 is dead and elect a new Leader...\n");
    sleep(2);

    run_curl("Quorum Write (Insert Charlie while 300 is isolated)",
             "curl -s -X POST http://127.0.0.1:8081/set -d 'Charlie:555-0003'");

    run_curl("Restore Network (Reconnect Node 300)",
             "curl -s -X POST http://127.0.0.1:8083/reconnect");

    printf("\n[SLEEP] Waiting 3 seconds for Node 300 to ingest the missed heartbeats and resolve log conflicts...\n");
    sleep(3);

    run_curl("Verify Reconnection Healing (Read Charlie from Node 300)",
             "curl -s -L -X POST http://127.0.0.1:8083/get -d 'Charlie'");

    printf("\n######################################################\n");
    printf("                 CHAOS SUITE COMPLETE\n");
    printf("######################################################\n\n");
}

int main(int argc, char** argv) {
    if (argc == 2 && strcmp(argv[1], "suite") == 0) {
        run_automated_suite();
        return 0;
    }

    if (argc < 3) {
        print_usage(argv[0]);
        return 1;
    }

    int port = atoi(argv[1]);
    const char* op = argv[2];
    const char* payload = (argc >= 4) ? argv[3] : "";
    bool use_redirect = (argc >= 5 && strcmp(argv[4], "redirect") == 0);

    char command[1024];

    if (strcmp(op, "set") == 0) {
        if (use_redirect) {
            snprintf(command, sizeof(command),
                     "curl -s -L -v -X POST http://127.0.0.1:%d/set -H 'X-Raft-Redirect: true' -d '%s' 2>&1 | grep -E '< HTTP|< Location|> POST'",
                     port, payload);
        } else {
            snprintf(command, sizeof(command), "curl -s -X POST http://127.0.0.1:%d/set -d '%s'", port, payload);
        }
    }
    else if (strcmp(op, "get") == 0) {
        snprintf(command, sizeof(command), "curl -s -L -X POST http://127.0.0.1:%d/get -d '%s'", port, payload);
    }
    else if (strcmp(op, "isolate") == 0 || strcmp(op, "reconnect") == 0 || strcmp(op, "crash") == 0) {
        snprintf(command, sizeof(command), "curl -s -X POST http://127.0.0.1:%d/%s", port, op);
    }
    else {
        printf("Error: Unknown operation '%s'.\n", op);
        return 1;
    }

    system(command);
    printf("\n");
    return 0;
}
