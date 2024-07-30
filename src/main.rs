// use tokio::fs::File;
// use tokio::io::AsyncWriteExt;

use std::sync::Arc;

use arrow::array::Int32Array;
use arrow::record_batch::RecordBatch;
use arrow_schema::DataType;
use arrow_schema::Field;
use arrow_schema::Schema;

#[tokio::main]
async fn main() {
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(tracing::Level::INFO)
        //.with_span_events(FmtSpan::CLOSE | FmtSpan::ENTER) // uncomment for detailed span stats
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    // let filename = "output.txt";
    // let mut file = File::options()
    //     .create(true)
    //     .append(true)
    //     .open(filename)
    //     .await
    //     .unwrap();

    // for _ in 1..=100 {
    //     file.write(b"test\n").await.unwrap();
    // }

    let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("id_2", DataType::Int32, false),
    ]);

    let batch1 = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(id_array.clone()), Arc::new(id_array.clone())],
    )
    .unwrap();
    let batch2 = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(id_array.clone()), Arc::new(id_array.clone())],
    )
    .unwrap();

    let batch_wrong = [batch1, batch2];

    let mut reader = arrow::record_batch::RecordBatchIterator::new(
        batch_wrong.into_iter().map(Ok),
        Arc::new(schema.clone()),
    );

    let buf = Vec::new();
    let mut writer = arrow_json::LineDelimitedWriter::new(buf);

    while let Some(batch) = reader.next() {
        writer
        // .write(&[reader.next().unwrap().as_ref().unwrap()])
        .write(batch.as_ref().unwrap())
        .unwrap();
    }

    writer.finish().unwrap();

    // let pretty_results = arrow::util::pretty::pretty_format_batches(batch)
    //     .unwrap()
    //     .to_string();

    // tracing::info!("Hello, world!");
    tracing::info!("\n{}", String::from_utf8(writer.into_inner()).unwrap());
}
