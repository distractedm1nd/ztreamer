//! Compare the production wire server with the ordinary protobuf projection.

use prost::Message;
use tokio_stream::StreamExt;
use ztreamer_protocol::proto;

#[path = "../benches/support/mod.rs"]
mod support;

#[tokio::test]
#[allow(deprecated)] // Verify compatibility with the legacy nullifier RPCs too.
async fn wire_server_preserves_blocks_filters_and_boundaries() {
    let server = support::LocalServer::start().await;
    let reference = support::Fixture::new().await;
    let mut client = support::Client::connect(server.endpoint.clone())
        .await
        .unwrap();
    for (start, end) in [
        (100, 199),
        (199, 100),
        (950, 1049),
        (1950, 2049),
        (2150, 2209),
        (2209, 2150),
    ] {
        for pools in [
            vec![],
            vec![proto::PoolType::Orchard as i32],
            vec![
                proto::PoolType::Sapling as i32,
                proto::PoolType::Ironwood as i32,
            ],
            vec![
                proto::PoolType::Orchard as i32,
                proto::PoolType::Sapling as i32,
                proto::PoolType::Ironwood as i32,
                proto::PoolType::Orchard as i32,
            ],
        ] {
            for nullifiers in [false, true] {
                let mut request = support::range(start, end);
                request.pool_types = pools.clone();
                let mut expected = reference
                    .service
                    .range(request.clone(), nullifiers)
                    .await
                    .unwrap();
                let mut actual = if nullifiers {
                    client.get_block_range_nullifiers(request).await
                } else {
                    client.get_block_range(request).await
                }
                .unwrap()
                .into_inner();
                while let Some(expected) = expected.next().await {
                    let expected = expected.unwrap();
                    let actual = actual.message().await.unwrap().expect("complete stream");
                    assert_eq!(actual.encode_to_vec(), expected.encode_to_vec());
                }
                assert!(actual.message().await.unwrap().is_none());
            }
        }
    }
    for height in [199, 2099, 2209] {
        for by_hash in [false, true] {
            let request = proto::BlockId {
                height: height.into(),
                hash: if by_hash {
                    support::fixtures::hash(height).to_vec()
                } else {
                    vec![]
                },
            };
            let expected = reference
                .service
                .block(request.clone(), false)
                .await
                .unwrap();
            assert_eq!(
                client
                    .get_block(request.clone())
                    .await
                    .unwrap()
                    .into_inner(),
                expected
            );
            let expected = reference
                .service
                .block(request.clone(), true)
                .await
                .unwrap();
            assert_eq!(
                client
                    .get_block_nullifiers(request)
                    .await
                    .unwrap()
                    .into_inner(),
                expected
            );
        }
    }
    for invalid in [0, 99] {
        let mut request = support::range(0, 10);
        request.pool_types = vec![invalid];
        assert_eq!(
            client.get_block_range(request).await.unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
    }
    server.stop().await;
}

#[tokio::test]
async fn cancelled_streams_leave_the_server_usable() {
    let server = support::LocalServer::start().await;
    let client = support::Client::connect(server.endpoint.clone())
        .await
        .unwrap();
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..64 {
        let mut client = client.clone();
        tasks.spawn(async move {
            for _ in 0..8 {
                let mut stream = client
                    .get_block_range(support::range(0, 1999))
                    .await
                    .unwrap()
                    .into_inner();
                assert_eq!(stream.message().await.unwrap().unwrap().height, 0);
                // Cancel while the server still has buffered range output.
                drop(stream);
            }
        });
    }
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }
        let mut client = client;
        let observed = support::consume(
            &mut client,
            support::range(0, 1999),
            std::time::Duration::ZERO,
        )
        .await
        .unwrap();
        assert_eq!(observed.blocks, 2000);
    })
    .await
    .expect("cancellation must release stream resources");
    server.stop().await;
}
