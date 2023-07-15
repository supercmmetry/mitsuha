use std::sync::Arc;

use mitsuha_qflow::qmux::QueueMuxer;

#[tokio::test]
async fn test_qmux_single_queue_producer() {
    let client = tikv_client::TransactionClient::new(vec!["127.0.0.1:2379"])
        .await
        .unwrap();
    let client = Arc::new(client);

    let muxer = QueueMuxer::new(client.clone(), 1).await.unwrap();

    for i in 0..10 {
        let mut tx = client.begin_pessimistic().await.unwrap();

        assert_eq!(0, muxer.get_producer_queue_index(&mut tx).await.unwrap());

        tx.commit().await.unwrap();
    }
}

#[tokio::test]
async fn test_qmux_multi_queue_producer() {
    let client = tikv_client::TransactionClient::new(vec!["127.0.0.1:2379"])
        .await
        .unwrap();
    let client = Arc::new(client);

    let muxer = QueueMuxer::new(client.clone(), 10).await.unwrap();
    let mut visited = vec![false; 10];

    for i in 0..10 {
        let mut tx = client.begin_pessimistic().await.unwrap();

        let index = muxer.get_producer_queue_index(&mut tx).await.unwrap();
        visited[index as usize] = true;

        tx.commit().await.unwrap();
    }

    for v in visited {
        assert!(v);
    }
}
