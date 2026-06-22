// SPDX-FileCopyrightText: 2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-License-Identifier: Apache-2.0
//
// Maintainer: Andy Curtis <contactandyc@gmail.com>

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "raft_internal.h"
#include "the-macro-library/macro_test.h"

MACRO_TEST(codec_roundtrips_basic_message) {
    raft_msg_t msg = {
        .type = MSG_APPEND_ENTRIES, .to = 1, .from = 2, .term = 3,
        .log_term = 2, .index = 100, .commit = 50, .conflict_term = 0,
        .conflict_index = 0, .read_seq = 12345, .reject = false,
        .num_entries = 0, .entries = NULL, .snapshot_len = 0
    };

    size_t sz = raft_msg_encoded_size(&msg);
    uint8_t* buf = malloc(sz);

    MACRO_ASSERT_TRUE(raft_msg_encode(&msg, buf, sz));

    raft_msg_t out;
    MACRO_ASSERT_TRUE(raft_msg_decode(buf, sz, &out));

    MACRO_ASSERT_EQ_INT(out.type, MSG_APPEND_ENTRIES);
    MACRO_ASSERT_EQ_INT(out.to, 1);
    MACRO_ASSERT_EQ_INT(out.from, 2);
    MACRO_ASSERT_EQ_INT(out.term, 3);
    MACRO_ASSERT_EQ_INT(out.read_seq, 12345);
    MACRO_ASSERT_FALSE(out.reject);

    raft_msg_free_payloads(&out);
    free(buf);
}

MACRO_TEST(codec_roundtrips_with_entries) {
    raft_entry_t e = {
        .term = 3, .index = 101, .type = ENTRY_NORMAL,
        .client_id = 99, .client_seq = 88,
        .data = (uint8_t*)"PAYLOAD", .data_len = 7
    };

    raft_msg_t msg = {
        .type = MSG_APPEND_ENTRIES, .to = 1, .from = 2,
        .entries = &e, .num_entries = 1
    };

    size_t sz = raft_msg_encoded_size(&msg);
    uint8_t* buf = malloc(sz);
    MACRO_ASSERT_TRUE(raft_msg_encode(&msg, buf, sz));

    raft_msg_t out;
    MACRO_ASSERT_TRUE(raft_msg_decode(buf, sz, &out));

    MACRO_ASSERT_EQ_INT(out.num_entries, 1);
    MACRO_ASSERT_EQ_INT(out.entries[0].index, 101);
    MACRO_ASSERT_EQ_INT(out.entries[0].data_len, 7);
    MACRO_ASSERT_TRUE(memcmp(out.entries[0].data, "PAYLOAD", 7) == 0);

    raft_msg_free_payloads(&out);
    free(buf);
}

MACRO_TEST(codec_roundtrips_with_snapshot_topology) {
    uint64_t peers[] = {2, 3};
    bool lrn[] = {false, true};

    raft_msg_t msg = {
        .type = MSG_INSTALL_SNAPSHOT, .to = 1, .from = 2,
        .snapshot_data = (uint8_t*)"STATE", .snapshot_len = 5,
        .snapshot_peers = peers, .snapshot_is_learner = lrn, .snapshot_num_peers = 2
    };

    size_t sz = raft_msg_encoded_size(&msg);
    uint8_t* buf = malloc(sz);
    MACRO_ASSERT_TRUE(raft_msg_encode(&msg, buf, sz));

    raft_msg_t out;
    MACRO_ASSERT_TRUE(raft_msg_decode(buf, sz, &out));

    MACRO_ASSERT_EQ_INT(out.snapshot_num_peers, 2);
    MACRO_ASSERT_EQ_INT(out.snapshot_peers[1], 3);
    MACRO_ASSERT_TRUE(out.snapshot_is_learner[1]);
    MACRO_ASSERT_TRUE(memcmp(out.snapshot_data, "STATE", 5) == 0);

    raft_msg_free_payloads(&out);
    free(buf);
}

MACRO_TEST(codec_rejects_malicious_num_entries_oom_dos) {
    // 86 byte valid base frame
    uint8_t buf[86] = {0};

    // Correct offset for num_entries is 74. We inject a massive number.
    buf[74] = 0x1D;
    buf[75] = 0xCD;
    buf[76] = 0x65;
    buf[77] = 0x00;

    raft_msg_t out;
    // Protocol strictly rejects exceeding RAFT_MAX_MSG_ENTRIES limit
    MACRO_ASSERT_FALSE(raft_msg_decode(buf, 86, &out));
}

MACRO_TEST(codec_safely_aborts_on_truncated_packet) {
    raft_msg_t msg = { .type = MSG_HUP, .to = 1, .from = 2 };

    size_t sz = raft_msg_encoded_size(&msg);
    uint8_t* buf = malloc(sz);
    raft_msg_encode(&msg, buf, sz);

    raft_msg_t out;
    // Chop exactly 1 byte off the required size
    MACRO_ASSERT_FALSE(raft_msg_decode(buf, sz - 1, &out));

    free(buf);
}

MACRO_TEST(codec_rejects_trailing_garbage) {
    raft_msg_t msg = { .type = MSG_HUP, .to = 1, .from = 2 };

    size_t sz = raft_msg_encoded_size(&msg);
    uint8_t* buf = malloc(sz + 1); // 1 byte of malicious trailing data
    raft_msg_encode(&msg, buf, sz);
    buf[sz] = 0xFF;

    raft_msg_t out;
    MACRO_ASSERT_FALSE(raft_msg_decode(buf, sz + 1, &out)); // Must fail-closed

    free(buf);
}

MACRO_TEST(codec_rejects_null_buffer) {
    raft_msg_t out;
    MACRO_ASSERT_FALSE(raft_msg_decode(NULL, 100, &out));
}

MACRO_TEST(codec_rejects_invalid_message_type) {
    uint8_t buf[86] = {0};
    buf[0] = 99; // Invalid type

    raft_msg_t out;
    MACRO_ASSERT_FALSE(raft_msg_decode(buf, 86, &out));
}

MACRO_TEST(codec_rejects_invalid_entry_type) {
    raft_entry_t e = { .term = 1, .type = 99 /* invalid */, .data_len = 0 };
    raft_msg_t msg = { .type = MSG_APPEND_ENTRIES, .entries = &e, .num_entries = 1 };

    size_t sz = raft_msg_encoded_size(&msg);
    uint8_t* buf = malloc(sz);

    // We hack the encode to force the bad data through, since the new encoder blocks it
    raft_msg_t hack = msg;
    hack.entries[0].type = ENTRY_NORMAL;
    raft_msg_encode(&hack, buf, sz);
    buf[86 + 16] = 99; // Manually overwrite the serialized entry type

    raft_msg_t out;
    MACRO_ASSERT_FALSE(raft_msg_decode(buf, sz, &out));

    free(buf);
}

MACRO_TEST(codec_rejects_reject_byte_not_bool) {
    uint8_t buf[86] = {0};
    buf[73] = 2; // Invalid boolean byte

    raft_msg_t out;
    MACRO_ASSERT_FALSE(raft_msg_decode(buf, 86, &out));
}

MACRO_TEST(codec_rejects_learner_flag_not_bool) {
    uint64_t peers[] = {2};
    bool lrn[] = {false};
    raft_msg_t msg = { .type = MSG_INSTALL_SNAPSHOT, .snapshot_peers = peers, .snapshot_is_learner = lrn, .snapshot_num_peers = 1 };

    size_t sz = raft_msg_encoded_size(&msg);
    uint8_t* buf = malloc(sz);
    raft_msg_encode(&msg, buf, sz);

    buf[sz - 1] = 5; // Manually corrupt the learner boolean byte

    raft_msg_t out;
    MACRO_ASSERT_FALSE(raft_msg_decode(buf, sz, &out));

    free(buf);
}

MACRO_TEST(codec_encode_rejects_data_len_with_null_data) {
    raft_entry_t e = { .data_len = 100, .data = NULL };
    raft_msg_t msg = { .type = MSG_APPEND_ENTRIES, .entries = &e, .num_entries = 1 };

    size_t sz;
    // Must refuse to calculate a size or encode if pointers are mathematically unsafe
    MACRO_ASSERT_FALSE(raft_msg_encoded_size_checked(&msg, &sz));
}

MACRO_TEST(codec_encode_rejects_snapshot_len_with_null_snapshot_data) {
    raft_msg_t msg = { .type = MSG_INSTALL_SNAPSHOT, .snapshot_len = 100, .snapshot_data = NULL };

    size_t sz;
    MACRO_ASSERT_FALSE(raft_msg_encoded_size_checked(&msg, &sz));
}

MACRO_TEST(codec_encode_rejects_snapshot_topology_count_with_null_arrays) {
    raft_msg_t msg = { .type = MSG_INSTALL_SNAPSHOT, .snapshot_num_peers = 1, .snapshot_peers = NULL, .snapshot_is_learner = NULL };

    size_t sz;
    MACRO_ASSERT_FALSE(raft_msg_encoded_size_checked(&msg, &sz));
}

MACRO_TEST(codec_decode_rejects_frame_larger_than_max) {
    raft_msg_t out;
    uint8_t dummy[10] = {0};
    // If a frame claims it exceeds our 1MB system limit, immediately drop it
    MACRO_ASSERT_FALSE(raft_msg_decode(dummy, 2000000, &out));
}

MACRO_TEST(codec_roundtrips_big_endian_known_bytes) {
    raft_msg_t msg = { .type = MSG_APPEND_ENTRIES, .to = 0x0102030405060708ULL };
    uint8_t buf[86] = {0};

    MACRO_ASSERT_TRUE(raft_msg_encode(&msg, buf, sizeof(buf)));

    // Prove Big-Endian Network Byte Order mathematically
    MACRO_ASSERT_EQ_INT(buf[1], 0x01);
    MACRO_ASSERT_EQ_INT(buf[2], 0x02);
    MACRO_ASSERT_EQ_INT(buf[3], 0x03);
    MACRO_ASSERT_EQ_INT(buf[4], 0x04);
    MACRO_ASSERT_EQ_INT(buf[5], 0x05);
    MACRO_ASSERT_EQ_INT(buf[6], 0x06);
    MACRO_ASSERT_EQ_INT(buf[7], 0x07);
    MACRO_ASSERT_EQ_INT(buf[8], 0x08);
}

int main(void) {
    macro_test_case tests[256];
    size_t test_count = 0;

    MACRO_ADD(tests, codec_roundtrips_basic_message);
    MACRO_ADD(tests, codec_roundtrips_with_entries);
    MACRO_ADD(tests, codec_roundtrips_with_snapshot_topology);
    MACRO_ADD(tests, codec_rejects_malicious_num_entries_oom_dos);
    MACRO_ADD(tests, codec_safely_aborts_on_truncated_packet);

    MACRO_ADD(tests, codec_rejects_trailing_garbage);
    MACRO_ADD(tests, codec_rejects_null_buffer);
    MACRO_ADD(tests, codec_rejects_invalid_message_type);
    MACRO_ADD(tests, codec_rejects_invalid_entry_type);
    MACRO_ADD(tests, codec_rejects_reject_byte_not_bool);
    MACRO_ADD(tests, codec_rejects_learner_flag_not_bool);
    MACRO_ADD(tests, codec_encode_rejects_data_len_with_null_data);
    MACRO_ADD(tests, codec_encode_rejects_snapshot_len_with_null_snapshot_data);
    MACRO_ADD(tests, codec_encode_rejects_snapshot_topology_count_with_null_arrays);
    MACRO_ADD(tests, codec_decode_rejects_frame_larger_than_max);
    MACRO_ADD(tests, codec_roundtrips_big_endian_known_bytes);

    macro_run_all("raft_codec", tests, test_count);
    return 0;
}
