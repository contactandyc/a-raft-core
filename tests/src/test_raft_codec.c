// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "a-raft-library/raft_codec.h"
#include "the-macro-library/macro_test.h"

MACRO_TEST(codec_rejects_truncated_base_frame) {
    uint8_t bad_buf[40] = {0}; // Way too small (minimum 74)
    raft_msg_t m;
    int res = raft_codec_deserialize_msg(bad_buf, 40, &m);
    MACRO_ASSERT_EQ_INT(res, -1);
}

MACRO_TEST(codec_rejects_billion_entry_attack) {
    uint8_t buf[128] = {0};
    uint64_t massive_num = 5000000000ULL;

    memset(buf, 0, 74);
    memcpy(buf + 66, &massive_num, 8); // Inject into num_entries position

    raft_msg_t m;
    int res = raft_codec_deserialize_msg(buf, 76, &m);

    MACRO_ASSERT_EQ_INT(res, -1);
}

MACRO_TEST(codec_rejects_missing_entry_payload_bytes) {
    uint8_t buf[256] = {0};
    uint64_t num_entries = 1;
    memcpy(buf + 66, &num_entries, 8);

    uint32_t fake_payload_len = 10000;
    memcpy(buf + 74 + 8 + 8 + 1, &fake_payload_len, 4);

    raft_msg_t m;
    int res = raft_codec_deserialize_msg(buf, 100, &m);

    MACRO_ASSERT_EQ_INT(res, -1);
}

MACRO_TEST(codec_roundtrips_successfully) {
    raft_entry_t e1 = { .term = 4, .index = 5, .type = ENTRY_NORMAL, .data = (uint8_t*)"HELLO", .data_len = 5 };
    raft_msg_t original = {
        .type = MSG_APPEND_ENTRIES,
        .to = 2, .from = 1, .term = 4, .log_term = 3, .index = 4, .commit = 2,
        .conflict_term = 99, .conflict_index = 88, // Testing Phase 3 fields
        .reject = false, .entries = &e1, .num_entries = 1
    };

    uint8_t* buf = NULL;
    uint32_t len = 0;
    MACRO_ASSERT_EQ_INT(raft_codec_serialize_msg(&original, &buf, &len), 0);

    raft_msg_t restored;
    MACRO_ASSERT_EQ_INT(raft_codec_deserialize_msg(buf, len, &restored), 0);

    MACRO_ASSERT_EQ_INT(restored.to, 2);
    MACRO_ASSERT_EQ_INT(restored.term, 4);
    MACRO_ASSERT_EQ_INT(restored.conflict_term, 99);
    MACRO_ASSERT_EQ_INT(restored.conflict_index, 88);
    MACRO_ASSERT_EQ_INT(restored.num_entries, 1);
    MACRO_ASSERT_EQ_INT(restored.entries[0].data_len, 5);
    MACRO_ASSERT_TRUE(memcmp(restored.entries[0].data, "HELLO", 5) == 0);

    raft_codec_free_msg_entries(&restored);
    free(buf);
}

int main(void) {
    macro_test_case tests[256];
    size_t test_count = 0;
    MACRO_ADD(tests, codec_rejects_truncated_base_frame);
    MACRO_ADD(tests, codec_rejects_billion_entry_attack);
    MACRO_ADD(tests, codec_rejects_missing_entry_payload_bytes);
    MACRO_ADD(tests, codec_roundtrips_successfully);
    macro_run_all("raft_codec", tests, test_count);
    return 0;
}
