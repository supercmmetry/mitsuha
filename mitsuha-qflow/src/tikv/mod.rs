use std::{collections::HashMap, sync::Arc};

use anyhow::anyhow;

use crate::{
    tikv::{muxer::TikvQueueMuxer, writer::TikvWriter},
    Reader, Writer,
};

use self::reader::TikvReader;

mod muxer;
mod reader;
mod writer;

pub async fn make_tikv_writer(
    extensions: &HashMap<String, String>,
) -> anyhow::Result<Arc<Box<dyn Writer>>> {
    let desired_queue_count = extensions
        .get("desired_queue_count")
        .ok_or(anyhow!("cannot find desired_queue_count"))?
        .parse::<u64>()?;
    let pd_endpoints = extensions
        .get("pd_endpoints")
        .ok_or(anyhow!("cannot find pd_endpoints"))?;

    let client = tikv_client::TransactionClient::new(pd_endpoints.split(",").collect())
        .await
        .unwrap();

    let client = Arc::new(client);


    let muxer = Arc::new(
        TikvQueueMuxer::new(client.clone(), desired_queue_count)
            .await
            .unwrap(),
    );

    let writer = TikvWriter::new(client, muxer).await.unwrap();

    Ok(Arc::new(Box::new(writer)))
}

pub async fn make_tikv_reader(
    extensions: &HashMap<String, String>,
) -> anyhow::Result<Arc<Box<dyn Reader>>> {
    let desired_queue_count = extensions
        .get("desired_queue_count")
        .ok_or(anyhow!("cannot find desired_queue_count"))?
        .parse::<u64>()?;
    let pd_endpoints = extensions
        .get("pd_endpoints")
        .ok_or(anyhow!("cannot find pd_endpoints"))?;

    let client = tikv_client::TransactionClient::new(pd_endpoints.split(",").collect())
        .await
        .unwrap();

    let client = Arc::new(client);

    let muxer = Arc::new(
        TikvQueueMuxer::new(client.clone(), desired_queue_count)
            .await
            .unwrap(),
    );

    let reader = TikvReader::new(client, muxer).await.unwrap();

    Ok(Arc::new(Box::new(reader)))
}
