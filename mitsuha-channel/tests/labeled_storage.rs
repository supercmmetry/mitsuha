use std::{collections::HashMap, sync::Arc, time::Duration};

use mitsuha_channel::{
    context::ChannelContext,
    labeled_storage::{self, LabeledStorageChannel},
    system::SystemChannel,
};
use mitsuha_core::{
    channel::{ComputeChannel, ComputeInput, ComputeOutput},
    config,
    kernel::StorageSpec,
    selector::Label,
    storage::{Storage, StorageClass, StorageLocality},
};
use mitsuha_storage::UnifiedStorage;

mod setup;
use setup::*;

#[tokio::test]
async fn store_and_load() {
    let chan = make_channel().await;

    let spec = StorageSpec {
        handle: "spec1".to_string(),
        data: "Hello world!".bytes().collect(),
        ttl: 100,
        extensions: HashMap::new(),
    };

    chan.compute(ChannelContext::default(), ComputeInput::Store { spec })
        .await
        .unwrap();

    let output = chan
        .compute(
            ChannelContext::default(),
            ComputeInput::Load {
                handle: "spec1".to_string(),
            },
        )
        .await
        .unwrap();

    match output {
        ComputeOutput::Loaded { data } => {
            assert_eq!(data, "Hello world!".bytes().collect::<Vec<u8>>());
        }
        _ => panic!("expected ComputeOutput of type: Loaded"),
    }
}

#[tokio::test]
async fn store_and_expire() {
    let chan = make_channel().await;

    let spec = StorageSpec {
        handle: "spec1".to_string(),
        data: "Hello world!".bytes().collect(),
        ttl: 1,
        extensions: HashMap::new(),
    };

    chan.compute(ChannelContext::default(), ComputeInput::Store { spec })
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_secs(2)).await;

    let result = chan
        .compute(
            ChannelContext::default(),
            ComputeInput::Load {
                handle: "spec1".to_string(),
            },
        )
        .await;

    assert!(result.is_err());
}

#[tokio::test]
async fn store_and_persist() {
    let chan = make_channel().await;

    let spec = StorageSpec {
        handle: "spec1".to_string(),
        data: "Hello world!".bytes().collect(),
        ttl: 2,
        extensions: HashMap::new(),
    };

    chan.compute(ChannelContext::default(), ComputeInput::Store { spec })
        .await
        .unwrap();
    chan.compute(
        ChannelContext::default(),
        ComputeInput::Persist {
            handle: "spec1".to_string(),
            ttl: 1,
        },
    )
    .await
    .unwrap();

    tokio::time::sleep(Duration::from_secs(2)).await;

    let output = chan
        .compute(
            ChannelContext::default(),
            ComputeInput::Load {
                handle: "spec1".to_string(),
            },
        )
        .await
        .unwrap();

    match output {
        ComputeOutput::Loaded { data } => {
            assert_eq!(data, "Hello world!".bytes().collect::<Vec<u8>>());
        }
        _ => panic!("expected ComputeOutput of type: Loaded"),
    }
}
