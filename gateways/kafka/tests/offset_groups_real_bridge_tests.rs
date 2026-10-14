// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! `ListGroups` and `DescribeGroups` against a real `iggy-server` process, for groups with no
//! members on the coordinator whose offsets are in Iggy. Kafka keeps such a group and reports it
//! as `Empty`. Responses are decoded by hand for the reason that
//! `create_topics_real_bridge_tests.rs` gives.

#[path = "common/codec.rs"]
mod codec;
#[path = "common/iggy_server.rs"]
mod iggy_server;
#[path = "common/wire.rs"]
mod wire;

use std::sync::Arc;
use std::time::Duration;

use iggy::prelude::{ConsumerGroupClient, Identifier};
use serial_test::serial;
use tokio_util::sync::CancellationToken;

use iggy_gateway_kafka::bridge::IggyBridge;
use iggy_gateway_kafka::group::{GroupCoordinator, GroupCoordinatorConfig};
use iggy_gateway_kafka::protocol::api::{
    API_KEY_DESCRIBE_GROUPS, API_KEY_JOIN_GROUP, API_KEY_LEAVE_GROUP, API_KEY_LIST_GROUPS,
    API_KEY_OFFSET_COMMIT, BrokerAdvertise, ERROR_COORDINATOR_LOAD_IN_PROGRESS,
    ERROR_GROUP_ID_NOT_FOUND, ERROR_NONE, GatewayState, handle_request_bounded,
};

use codec::Decoder;
use iggy_server::TestServer;
use wire::{
    JoinGroupParams, build_describe_groups_request, build_join_group_request,
    build_leave_group_request, build_list_groups_request, build_offset_commit_request,
};

const TEST_MAX_FRAME_SIZE: usize = 8 * 1024 * 1024;
const COMMIT_VERSION: i16 = 8;
/// The first version with both filters.
const LIST_VERSION: i16 = 5;
/// The last version without `error_message`, so a missing group is `Dead` with error 0.
const DESCRIBE_VERSION: i16 = 5;
/// v3 admits a member without the `MEMBER_ID_REQUIRED` round trip.
const JOIN_VERSION: i16 = 3;
const LEAVE_VERSION: i16 = 1;
const GROUP: &str = "orders-app";

/// One `ListGroups` entry: `(group_id, protocol_type, state, group_type)`.
type Listed = (String, String, String, String);

/// One `DescribeGroups` entry with the fields an offsets-only group sets.
#[derive(Debug, PartialEq, Eq)]
struct Described {
    error: i16,
    error_message: Option<String>,
    group_id: String,
    state: String,
    protocol_type: String,
    protocol_data: String,
    members: usize,
}

async fn connected_state(server: &TestServer) -> GatewayState {
    let bridge = IggyBridge::connect(server.test_config())
        .await
        .expect("bridge should connect to a ready server");
    // A new group's first join completes at once, so a test can hold a live member.
    let groups = GroupCoordinatorConfig {
        initial_rebalance_delay: Duration::ZERO,
        ..GroupCoordinatorConfig::default()
    };
    GatewayState::new(
        BrokerAdvertise::default(),
        Some(Arc::new(bridge)),
        TEST_MAX_FRAME_SIZE,
        false,
        0,
        GroupCoordinator::new(groups, CancellationToken::new()),
    )
}

async fn seed_topic(server: &TestServer, topic: &str) {
    IggyBridge::connect(server.test_config())
        .await
        .expect("seed bridge should connect")
        .ensure_stream_and_topic(topic, 1)
        .await
        .expect("seed the topic");
}

/// Commits `offset` for partition 0 of `topic` outside the group protocol, as admin tools do.
async fn commit(state: &GatewayState, group: &str, topic: &str, offset: i64) {
    let body = build_offset_commit_request(COMMIT_VERSION, group, topic, offset);
    let response = handle_request_bounded(state, API_KEY_OFFSET_COMMIT, COMMIT_VERSION, body)
        .await
        .expect_response("OffsetCommit answers");
    let mut d = Decoder::new(response);
    let _throttle_time_ms = d.read_i32().expect("throttle_time_ms");
    assert_eq!(d.read_varint().expect("topics"), 2, "one topic");
    let _name = d.read_compact_nullable_string().expect("topic name");
    assert_eq!(d.read_varint().expect("partitions"), 2, "one partition");
    let _index = d.read_i32().expect("partition_index");
    assert_eq!(d.read_i16().expect("error_code"), ERROR_NONE, "the commit");
}

/// The top-level error and every group of a v5 `ListGroups` answer.
async fn list(state: &GatewayState, states: &[&str], group_types: &[&str]) -> (i16, Vec<Listed>) {
    let body = build_list_groups_request(LIST_VERSION, states, group_types);
    let response = handle_request_bounded(state, API_KEY_LIST_GROUPS, LIST_VERSION, body)
        .await
        .expect_response("ListGroups answers");
    let mut d = Decoder::new(response);
    let _throttle_time_ms = d.read_i32().expect("throttle_time_ms");
    let error = d.read_i16().expect("error_code");
    let mut groups = Vec::new();
    for _ in 1..d.read_varint().expect("groups") {
        let mut string = || {
            d.read_compact_nullable_string()
                .expect("a string")
                .expect("not null")
        };
        groups.push((string(), string(), string(), string()));
        d.read_tagged_fields().expect("group tagged fields");
    }
    d.read_tagged_fields().expect("tagged fields");
    assert_eq!(d.remaining(), 0, "ListGroups trailing bytes");
    (error, groups)
}

async fn describe(state: &GatewayState, version: i16, groups: &[&str]) -> Vec<Described> {
    let body = build_describe_groups_request(version, groups, false);
    let response = handle_request_bounded(state, API_KEY_DESCRIBE_GROUPS, version, body)
        .await
        .expect_response("DescribeGroups answers");
    let mut d = Decoder::new(response);
    let _throttle_time_ms = d.read_i32().expect("throttle_time_ms");
    let mut described = Vec::new();
    for _ in 1..d.read_varint().expect("groups") {
        let error = d.read_i16().expect("error_code");
        let error_message = if version >= 6 {
            d.read_compact_nullable_string().expect("error_message")
        } else {
            None
        };
        let mut string = || {
            d.read_compact_nullable_string()
                .expect("a string")
                .expect("not null")
        };
        let (group_id, state, protocol_type, protocol_data) =
            (string(), string(), string(), string());
        let members = usize::try_from(d.read_varint().expect("members") - 1).expect("a count");
        assert_eq!(members, 0, "only groups with no members are described here");
        let _authorized_operations = d.read_i32().expect("authorized_operations");
        d.read_tagged_fields().expect("group tagged fields");
        described.push(Described {
            error,
            error_message,
            group_id,
            state,
            protocol_type,
            protocol_data,
            members,
        });
    }
    d.read_tagged_fields().expect("tagged fields");
    assert_eq!(d.remaining(), 0, "DescribeGroups trailing bytes");
    described
}

/// Joins a member to `group` and returns its id. The group is then `CompletingRebalance`.
async fn join(state: &GatewayState, group: &str) -> String {
    let params = JoinGroupParams {
        group_id: group,
        ..JoinGroupParams::default()
    };
    let body = build_join_group_request(JOIN_VERSION, &params);
    let response = handle_request_bounded(state, API_KEY_JOIN_GROUP, JOIN_VERSION, body)
        .await
        .expect_response("JoinGroup answers");
    let mut d = Decoder::new(response);
    let _throttle_time_ms = d.read_i32().expect("throttle_time_ms");
    assert_eq!(d.read_i16().expect("error_code"), ERROR_NONE, "the join");
    let _generation_id = d.read_i32().expect("generation_id");
    let _protocol_name = d.read_nullable_string().expect("protocol_name");
    let _leader = d.read_nullable_string().expect("leader");
    d.read_nullable_string()
        .expect("member_id")
        .expect("a member id")
}

async fn leave(state: &GatewayState, group: &str, member_id: &str) {
    let body = build_leave_group_request(LEAVE_VERSION, group, &[(member_id, None, None)]);
    let response = handle_request_bounded(state, API_KEY_LEAVE_GROUP, LEAVE_VERSION, body)
        .await
        .expect_response("LeaveGroup answers");
    let mut d = Decoder::new(response);
    let _throttle_time_ms = d.read_i32().expect("throttle_time_ms");
    assert_eq!(d.read_i16().expect("error_code"), ERROR_NONE, "the leave");
}

fn listed(group: &str, state: &str) -> Listed {
    let group_type = "classic".to_string();
    (
        group.to_string(),
        "consumer".to_string(),
        state.to_string(),
        group_type,
    )
}

fn offsets_only(group: &str) -> Described {
    Described {
        error: ERROR_NONE,
        error_message: None,
        group_id: group.to_string(),
        state: "Empty".to_string(),
        protocol_type: "consumer".to_string(),
        protocol_data: String::new(),
        members: 0,
    }
}

/// A group whose consumers committed and left has no members here. Kafka still lists and
/// describes it, as `Empty`, while it holds offsets.
#[tokio::test]
#[serial]
async fn given_offsets_and_no_members_list_and_describe_should_report_an_empty_group() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    seed_topic(&server, "orders").await;
    let state = connected_state(&server).await;
    commit(&state, GROUP, "orders", 7).await;

    assert_eq!(
        list(&state, &[], &[]).await,
        (ERROR_NONE, vec![listed(GROUP, "Empty")])
    );
    assert_eq!(
        describe(&state, DESCRIBE_VERSION, &[GROUP, "never-committed"]).await,
        vec![
            offsets_only(GROUP),
            Described {
                state: "Dead".to_string(),
                protocol_type: String::new(),
                ..offsets_only("never-committed")
            },
        ]
    );
    assert_eq!(
        describe(&state, 6, &[GROUP, "never-committed"]).await,
        vec![
            offsets_only(GROUP),
            Described {
                error: ERROR_GROUP_ID_NOT_FOUND,
                error_message: Some("Group never-committed not found.".to_string()),
                state: "Dead".to_string(),
                protocol_type: String::new(),
                ..offsets_only("never-committed")
            },
        ]
    );
}

/// A live group is listed once, with its live state. After its last member leaves, the group is
/// `Empty`, because its offsets are still in Iggy.
#[tokio::test]
#[serial]
async fn given_a_live_group_with_offsets_should_list_it_once_and_empty_after_it_leaves() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    seed_topic(&server, "orders").await;
    let state = connected_state(&server).await;
    commit(&state, GROUP, "orders", 7).await;

    let member_id = join(&state, GROUP).await;
    assert_eq!(
        list(&state, &[], &[]).await,
        (ERROR_NONE, vec![listed(GROUP, "CompletingRebalance")])
    );

    leave(&state, GROUP, &member_id).await;
    assert_eq!(
        list(&state, &[], &[]).await,
        (ERROR_NONE, vec![listed(GROUP, "Empty")])
    );
    assert_eq!(
        describe(&state, DESCRIBE_VERSION, &[GROUP]).await,
        vec![offsets_only(GROUP)]
    );
}

/// The filters apply to a group that only holds offsets as to any other: its state is `Empty` and
/// its type is `classic`.
#[tokio::test]
#[serial]
async fn given_filters_should_keep_an_offsets_only_group_only_when_they_match() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    seed_topic(&server, "orders").await;
    let state = connected_state(&server).await;
    commit(&state, GROUP, "orders", 7).await;
    let empty = (ERROR_NONE, vec![listed(GROUP, "Empty")]);

    assert_eq!(list(&state, &[" empty "], &[]).await, empty);
    assert_eq!(list(&state, &[], &["Classic"]).await, empty);
    assert_eq!(list(&state, &["Stable"], &[]).await, (ERROR_NONE, vec![]));
    assert_eq!(list(&state, &[], &["consumer"]).await, (ERROR_NONE, vec![]));
}

/// Groups come from a scan that answers for a while. A commit through this gateway adds its group
/// at once, so a group does not wait for the next scan.
#[tokio::test]
#[serial]
async fn given_a_commit_after_the_last_scan_should_list_its_group_at_once() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    seed_topic(&server, "orders").await;
    let state = connected_state(&server).await;
    commit(&state, "first-app", "orders", 1).await;
    assert_eq!(
        list(&state, &[], &[]).await,
        (ERROR_NONE, vec![listed("first-app", "Empty")])
    );

    commit(&state, "second-app", "orders", 2).await;

    assert_eq!(
        list(&state, &[], &[]).await,
        (
            ERROR_NONE,
            vec![listed("first-app", "Empty"), listed("second-app", "Empty")]
        )
    );
}

/// A gateway that just started has no memory of the groups. Its scan finds the groups that another
/// gateway, or an earlier run, committed for, on every topic.
#[tokio::test]
#[serial]
async fn given_a_new_gateway_should_find_the_groups_of_every_topic() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    for topic in ["orders", "payments"] {
        seed_topic(&server, topic).await;
    }
    let first = connected_state(&server).await;
    commit(&first, "first-app", "orders", 1).await;
    commit(&first, "second-app", "payments", 2).await;
    commit(&first, "first-app", "payments", 3).await;
    drop(first);

    let restarted = connected_state(&server).await;

    assert_eq!(
        list(&restarted, &[], &[]).await,
        (
            ERROR_NONE,
            vec![listed("first-app", "Empty"), listed("second-app", "Empty")]
        )
    );
}

/// Only an Iggy group named `kafka.cg.<group>` holds Kafka offsets. A native Iggy group on the
/// same topic is not a Kafka group.
#[tokio::test]
#[serial]
async fn given_a_native_iggy_group_should_not_list_it() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    seed_topic(&server, "orders").await;
    let raw = iggy_server::raw_client(&server).await;
    let iggy_id = |name: &str| Identifier::named(name).expect("valid Iggy name");
    raw.create_consumer_group(&iggy_id("kafka"), &iggy_id("orders"), "native-app")
        .await
        .expect("create a native Iggy group");
    let state = connected_state(&server).await;

    assert_eq!(list(&state, &[], &[]).await, (ERROR_NONE, vec![]));
}

/// Without a scan that found the groups, the gateway cannot tell `Empty` from `Dead`. It answers
/// `COORDINATOR_LOAD_IN_PROGRESS`, which clients retry, rather than a list that leaves groups out.
#[tokio::test]
#[serial]
async fn given_iggy_unreachable_before_any_scan_should_answer_coordinator_load_in_progress() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let server = TestServer::spawn(data_dir.path()).await;
    let state = connected_state(&server).await;
    drop(server);

    assert_eq!(
        list(&state, &[], &[]).await,
        (ERROR_COORDINATOR_LOAD_IN_PROGRESS, vec![])
    );
    assert_eq!(
        list(&state, &["Stable"], &[]).await,
        (ERROR_NONE, vec![]),
        "a filter that leaves out Empty needs no scan"
    );
    let unknown = describe(&state, DESCRIBE_VERSION, &[GROUP]).await;
    assert_eq!(
        unknown,
        vec![Described {
            error: ERROR_COORDINATOR_LOAD_IN_PROGRESS,
            state: String::new(),
            protocol_type: String::new(),
            ..offsets_only(GROUP)
        }]
    );
}
