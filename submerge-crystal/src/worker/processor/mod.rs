use std::cmp::min;
use std::sync::{Arc, LazyLock};

use crate::api::legacy::LegacyDecodeAPIClient;
use crate::persistence::CrystalPostgreSQLStorage;
use crate::types::metadata::util::{
    get_metadata_type_by_id, get_metadata_version, get_pallet_storage_item_type_by_name,
};
use crate::types::{BlockStatus, Event, Extrinsic};
use crate::worker::metadata_cache::{get_metadata, get_parsed_metadata};
use crate::worker::WorkerError;
use anyhow::Context as _;
use frame_metadata::RuntimeMetadata;
use serde_json::Value as JSONValue;
use sqlx::{Postgres, Transaction};
use submerge_base::types::substrate::account_id::AccountId;
use submerge_base::types::substrate::block::BlockHeader;
use submerge_base::types::substrate::multi_address::MultiAddress;
use submerge_persistence::postgres::PostgreSQLStorage;
use submerge_substrate_client::{RPCConfig, SubstrateClient};
use submerge_util::string::truncate_hash;
use tokio::sync::RwLock;
use uuid::Uuid as UUID;

mod event;
mod extrinsic;
mod weight;

pub mod concurrent;

/// Holds all decoded block data ready for DB persistence.
/// Produced by `decode_block()`, consumed by `persist_decoded_block()`.
pub(crate) struct DecodedBlock {
    pub block_hash: Vec<u8>,
    pub block_hash_hex: String,
    pub block_number: u64,
    pub block_header: BlockHeader,
    pub spec_version: u32,
    pub metadata: Arc<RuntimeMetadata>,
    pub author_multi_address: Option<MultiAddress>,
    pub block_timestamp: Option<u64>,
    pub events: Vec<Event>,
    pub extrinsics: Vec<Extrinsic>,
    pub weight: Option<JSONValue>,
}

const TRANSACTION_LEVEL_KEY: &[u8] = b":transaction_level:";
const METADATA_VERSION_LEGACY_THRESHOLD: u32 = 14;
const ACCOUNT_ID_32_TYPE_PATH: &str = "sp_core::crypto::AccountId32";
const ACCOUNT_ID_20_TYPE_PATH: &str = "account::AccountId20";
const AUTHOR_INHERENT_PALLET_NAME: &str = "AuthorInherent";
const AUTHOR_STORAGE_ITEM_NAME: &str = "Author";
const SESSION_PALLET_NAME: &str = "Session";
const VALIDATORS_STORAGE_ITEM_NAME: &str = "Validators";

static SESSION_VALIDATORS_CACHE: LazyLock<RwLock<(u32, Vec<MultiAddress>)>> =
    LazyLock::new(|| RwLock::new((0, Vec::new())));

fn validate_block_range(
    maybe_start_block_number: Option<u64>,
    maybe_end_block_number: Option<u64>,
) -> Result<(), WorkerError> {
    if let Some((start, end)) = maybe_start_block_number.zip(maybe_end_block_number) {
        if start > end {
            return Err(WorkerError::InvalidFinalizedRange(start, end));
        }
    }
    Ok(())
}

pub(crate) struct BlockProcessor {
    chain_name: String,
    worker_id: UUID,
    postgres: Arc<PostgreSQLStorage>,
    substrate_client: Arc<SubstrateClient>,
    legacy_decode_api_client: Option<Arc<LegacyDecodeAPIClient>>,
}

impl BlockProcessor {
    pub(crate) async fn new(
        chain_name: &str,
        worker_id: UUID,
        postgres: Arc<PostgreSQLStorage>,
        rpc_config: &RPCConfig,
        legacy_decode_api_url: &Option<String>,
    ) -> anyhow::Result<Self> {
        let substrate_client = Arc::new(SubstrateClient::new(rpc_config).await?);
        let legacy_decode_api_client = if let Some(url) = legacy_decode_api_url {
            Some(Arc::new(LegacyDecodeAPIClient::new(url)?))
        } else {
            None
        };
        Ok(Self {
            chain_name: chain_name.to_string(),
            worker_id,
            postgres,
            substrate_client,
            legacy_decode_api_client,
        })
    }

    pub(crate) async fn save_block_error(
        &self,
        block_hash: &[u8],
        block_number: u64,
        status: BlockStatus,
        description: &str,
    ) -> anyhow::Result<()> {
        self.postgres
            .save_error(block_hash, block_number, status, description)
            .await
    }

    async fn get_actual_finalized_block_range(
        &self,
        maybe_start_block_number: Option<u64>,
        maybe_end_block_number: Option<u64>,
        scan: bool,
    ) -> anyhow::Result<(u64, u64)> {
        let start_block_number = maybe_start_block_number.unwrap_or(0);
        let finalized_block_hash = self.substrate_client.get_finalized_block_hash().await?;
        let finalized_block_number = self
            .substrate_client
            .get_block_header(&finalized_block_hash)
            .await?
            .get_number()?;
        let end_block_number = min(
            maybe_end_block_number.unwrap_or(finalized_block_number),
            finalized_block_number,
        );
        let start_block_number = if scan {
            start_block_number
        } else {
            self.postgres
                .get_next_block_number(start_block_number, end_block_number, BlockStatus::Finalized)
                .await?
        };
        Ok((start_block_number, end_block_number))
    }

    /// Decode a block without any DB interaction.
    /// Fetches all RPC data and decodes events/extrinsics/weight.
    /// This is pure (no DB writes) and safe to run in parallel.
    /// Only supports skip_traces=true (no trace processing).
    pub(crate) async fn decode_block(
        &self,
        block_hash_hex: &str,
        block_number: u64,
    ) -> anyhow::Result<DecodedBlock> {
        let block_hash = hex::decode(block_hash_hex)?;
        let block_header = self
            .substrate_client
            .get_block_header(block_hash_hex)
            .await?;
        if block_number == 0 {
            let spec_version = self
                .substrate_client
                .get_last_runtime_upgrade_info(block_hash_hex)
                .await?
                .spec_version;
            let metadata = get_metadata(
                &block_hash,
                spec_version,
                &self.postgres,
                &self.substrate_client,
                &self.legacy_decode_api_client,
            )
            .await?;
            return Ok(DecodedBlock {
                block_hash,
                block_hash_hex: block_hash_hex.to_string(),
                block_number,
                block_header,
                spec_version,
                metadata,
                author_multi_address: None,
                block_timestamp: None,
                events: Vec::new(),
                extrinsics: Vec::new(),
                weight: None,
            });
        }
        let spec_version = self
            .substrate_client
            .get_last_runtime_upgrade_info(&block_header.parent_hash)
            .await?
            .spec_version;
        let parent_hash = hex::decode(block_header.parent_hash.trim_start_matches("0x"))?;
        let metadata = get_metadata(
            &parent_hash,
            spec_version,
            &self.postgres,
            &self.substrate_client,
            &self.legacy_decode_api_client,
        )
        .await?;
        let author_multi_address = self
            .get_block_author(block_hash_hex, spec_version, &block_header)
            .await?;
        let block_timestamp = self
            .substrate_client
            .get_block_timestamp(block_hash_hex)
            .await?;
        let event_bytes = self
            .substrate_client
            .get_block_event_bytes(block_hash_hex)
            .await?;
        let events = self
            .get_events_from_event_bytes(&block_hash, spec_version, &metadata, event_bytes)
            .await?;
        let extrinsics = self
            .get_extrinsics(&block_hash, spec_version, &metadata, &events)
            .await?;
        let weight = self
            .get_block_weight_from_rpc(&block_hash, spec_version, &metadata)
            .await?;
        Ok(DecodedBlock {
            block_hash,
            block_hash_hex: block_hash_hex.to_string(),
            block_number,
            block_header,
            spec_version,
            metadata,
            author_multi_address,
            block_timestamp,
            events,
            extrinsics,
            weight,
        })
    }

    /// Persist a previously decoded block into the given transaction.
    /// This handles block, logs, events, extrinsics, calls, pruning, and error cleanup.
    pub(crate) async fn persist_decoded_block(
        &self,
        decoded: &DecodedBlock,
        status: BlockStatus,
        tx: &mut Transaction<'_, Postgres>,
    ) -> anyhow::Result<()> {
        if status == BlockStatus::Finalized {
            self.prune_other_blocks_with_number(decoded.block_number, &decoded.block_hash, tx)
                .await?;
        }
        self.postgres
            .ingest_block(
                &decoded.block_hash,
                &decoded.block_header,
                decoded.block_timestamp,
                status,
                &decoded.weight,
                decoded.spec_version,
                decoded.extrinsics.len() as u32,
                decoded.events.len() as u32,
                &decoded.author_multi_address,
                tx,
            )
            .await?;
        self.postgres
            .ingest_block_logs(&decoded.block_hash, &decoded.block_header, tx)
            .await?;
        self.process_events(
            &decoded.block_hash,
            &decoded.block_header,
            decoded.block_timestamp,
            decoded.spec_version,
            status,
            &decoded.events,
            &decoded.extrinsics,
            tx,
        )
        .await?;
        self.process_extrinsics(
            &decoded.block_hash,
            &decoded.block_header,
            decoded.block_timestamp,
            decoded.spec_version,
            status,
            &decoded.extrinsics,
            tx,
        )
        .await?;
        self.postgres.delete_error(&decoded.block_hash, tx).await?;
        Ok(())
    }

    /// Persist a batch of decoded blocks in a single DB transaction.
    pub(crate) async fn batch_persist_decoded_blocks(
        &self,
        decoded_blocks: &[DecodedBlock],
        status: BlockStatus,
    ) -> anyhow::Result<()> {
        let mut tx = self.postgres.connection_pool.begin().await?;
        for decoded in decoded_blocks {
            // Check if already processed (skip duplicates)
            if let Some(block_row) = self.postgres.get_block_by_hash(&decoded.block_hash).await? {
                if block_row.status != status && status == BlockStatus::Finalized {
                    self.postgres
                        .update_block_status(&decoded.block_hash, status, &mut tx)
                        .await?;
                    self.prune_other_blocks_with_number(
                        decoded.block_number,
                        &decoded.block_hash,
                        &mut tx,
                    )
                    .await?;
                }
                continue;
            }
            self.persist_decoded_block(decoded, status, &mut tx).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// Concurrent block fetching with parallel decoding and batch DB inserts.
    /// Fetches hashes concurrently, decodes blocks in parallel via tokio::spawn,
    /// collects decoded blocks into batches, and inserts each batch in a single transaction.
    /// Processes the range in chunks of 10,000 blocks to avoid OOM on large ranges.
    pub(crate) async fn process_finalized_blocks_in_range_concurrent(
        &self,
        stop_on_error: bool,
        skip_traces: bool,
        scan: bool,
        reindex: bool,
        maybe_start_block_number: Option<u64>,
        maybe_end_block_number: Option<u64>,
        max_concurrent_fetches: Option<usize>,
    ) -> anyhow::Result<()> {
        validate_block_range(maybe_start_block_number, maybe_end_block_number)?;
        let (start_block_number, end_block_number) = self
            .get_actual_finalized_block_range(
                maybe_start_block_number,
                maybe_end_block_number,
                scan,
            )
            .await?;

        let concurrency = max_concurrent_fetches.unwrap_or(100);
        let batch_size: usize = 50;
        let chunk_size: u64 = 10_000;

        // Fall back to sequential processing when traces or reindex are needed
        if !skip_traces || reindex {
            tracing::info!(
                "⚙️ Process finalized blocks {start_block_number}-{end_block_number} using concurrent fetching with sequential processing (concurrency: {concurrency})."
            );
            return self
                .process_finalized_blocks_in_range_concurrent_sequential(
                    stop_on_error,
                    skip_traces,
                    reindex,
                    start_block_number,
                    end_block_number,
                    concurrency,
                )
                .await;
        }

        let total_blocks = end_block_number - start_block_number + 1;
        let total_chunks = (total_blocks + chunk_size - 1) / chunk_size;
        tracing::info!(
            "⚙️ Process finalized blocks {start_block_number}-{end_block_number} using parallel decode + batch insert (concurrency: {concurrency}, batch: {batch_size}, chunks: {total_chunks} x {chunk_size})."
        );

        let mut total_processed: u64 = 0;
        let mut total_fetch_errors: u64 = 0;
        let mut total_decode_errors: u64 = 0;

        for chunk_start in (start_block_number..=end_block_number).step_by(chunk_size as usize) {
            let chunk_end = min(chunk_start + chunk_size - 1, end_block_number);
            let chunk_num = (chunk_start - start_block_number) / chunk_size + 1;
            tracing::info!(
                "📦 Chunk {chunk_num}/{total_chunks}: blocks {chunk_start}-{chunk_end}."
            );

            // Concurrently fetch block hashes for this chunk
            let mut rx = concurrent::fetch_hashes_range(
                &self.substrate_client,
                chunk_start,
                chunk_end,
                concurrency,
            )
            .await;

            let expected_count = chunk_end - chunk_start + 1;
            let mut fetch_error_count: u64 = 0;
            let mut decode_error_count: u64 = 0;
            let mut processed_count: u64 = 0;

            // Track contiguous checkpoint within this chunk
            let range_len = (chunk_end - chunk_start + 1) as usize;
            let mut completed: Vec<Option<Vec<u8>>> = vec![None; range_len];
            let mut last_checkpoint = chunk_start.saturating_sub(1);

            // Collect hashes and spawn decode tasks in parallel
            let decode_semaphore = Arc::new(tokio::sync::Semaphore::new(concurrency));
            let mut decode_handles: Vec<tokio::task::JoinHandle<Result<DecodedBlock, (u64, String, anyhow::Error)>>> =
                Vec::new();

            // Drain all hashes from channel, spawning decode tasks
            while let Some(result) = rx.recv().await {
                let block_hash = match result {
                    Ok(h) => h,
                    Err(e) => {
                        fetch_error_count += 1;
                        tracing::error!("❌ Fetch error: {e:?}");
                        if stop_on_error {
                            return Err(e);
                        }
                        continue;
                    }
                };

                let sem = decode_semaphore.clone();
                let substrate_client = self.substrate_client.clone();
                let postgres = self.postgres.clone();
                let legacy_decode_api_client = self.legacy_decode_api_client.clone();
                let chain_name = self.chain_name.clone();
                let worker_id = self.worker_id;
                let hash_hex = block_hash.hash_hex.clone();
                let number = block_hash.number;

                let handle = tokio::spawn(async move {
                    let _permit = sem.acquire().await.map_err(|e| {
                        (number, hash_hex.clone(), anyhow::anyhow!("Semaphore error: {e}"))
                    })?;
                    let processor = BlockProcessor {
                        chain_name,
                        worker_id,
                        postgres,
                        substrate_client,
                        legacy_decode_api_client,
                    };
                    processor
                        .decode_block(&hash_hex, number)
                        .await
                        .map_err(|e| (number, hash_hex, e))
                });
                decode_handles.push(handle);

                // When we have enough pending handles, drain a batch
                if decode_handles.len() >= batch_size {
                    let batch_handles: Vec<_> = decode_handles.drain(..batch_size).collect();
                    let mut batch_decoded = Vec::with_capacity(batch_size);
                    for handle in batch_handles {
                        match handle.await {
                            Ok(Ok(decoded)) => batch_decoded.push(decoded),
                            Ok(Err((num, hash_hex, error))) => {
                                decode_error_count += 1;
                                tracing::error!("❌ Error decoding block {num}: {error:?}");
                                let hash = hex::decode(&hash_hex).unwrap_or_default();
                                self.save_block_error(
                                    &hash,
                                    num,
                                    BlockStatus::Finalized,
                                    &error.to_string(),
                                )
                                .await?;
                                if stop_on_error {
                                    return Err(error);
                                }
                            }
                            Err(join_error) => {
                                decode_error_count += 1;
                                tracing::error!("❌ Decode task panicked: {join_error:?}");
                                if stop_on_error {
                                    return Err(anyhow::anyhow!("Decode task panicked: {join_error}"));
                                }
                            }
                        }
                    }
                    // Sort batch by block number for deterministic insertion
                    batch_decoded.sort_by_key(|d| d.block_number);
                    if !batch_decoded.is_empty() {
                        // Track which blocks are in this batch for checkpoint
                        let batch_block_info: Vec<(u64, Vec<u8>)> = batch_decoded
                            .iter()
                            .map(|d| (d.block_number, d.block_hash.clone()))
                            .collect();

                        self.batch_persist_decoded_blocks(&batch_decoded, BlockStatus::Finalized)
                            .await?;

                        for (num, hash) in &batch_block_info {
                            processed_count += 1;
                            let idx = (*num - chunk_start) as usize;
                            if idx < completed.len() {
                                completed[idx] = Some(hash.clone());
                            }
                            crate::metrics::processed_finalized_block_number()?
                                .with_label_values(
                                    [&self.worker_id.to_string(), "concurrent_range"].as_slice(),
                                )
                                .set(*num as i64);
                        }

                        // Advance contiguous checkpoint
                        while (last_checkpoint + 1 - chunk_start) < range_len as u64 {
                            let next_idx = (last_checkpoint + 1 - chunk_start) as usize;
                            if next_idx < completed.len() && completed[next_idx].is_some() {
                                last_checkpoint += 1;
                            } else {
                                break;
                            }
                        }
                        if last_checkpoint >= chunk_start {
                            let cp_idx = (last_checkpoint - chunk_start) as usize;
                            if let Some(ref hash_bytes) = completed[cp_idx] {
                                self.postgres
                                    .set_last_indexed_finalized_block_number_and_hash(
                                        last_checkpoint,
                                        hash_bytes,
                                    )
                                    .await?;
                            }
                        }
                    }
                }
            }

            // Drain remaining decode handles for this chunk
            if !decode_handles.is_empty() {
                let mut batch_decoded = Vec::with_capacity(decode_handles.len());
                for handle in decode_handles {
                    match handle.await {
                        Ok(Ok(decoded)) => batch_decoded.push(decoded),
                        Ok(Err((num, hash_hex, error))) => {
                            decode_error_count += 1;
                            tracing::error!("❌ Error decoding block {num}: {error:?}");
                            let hash = hex::decode(&hash_hex).unwrap_or_default();
                            self.save_block_error(
                                &hash,
                                num,
                                BlockStatus::Finalized,
                                &error.to_string(),
                            )
                            .await?;
                            if stop_on_error {
                                return Err(error);
                            }
                        }
                        Err(join_error) => {
                            decode_error_count += 1;
                            tracing::error!("❌ Decode task panicked: {join_error:?}");
                            if stop_on_error {
                                return Err(anyhow::anyhow!("Decode task panicked: {join_error}"));
                            }
                        }
                    }
                }
                batch_decoded.sort_by_key(|d| d.block_number);
                if !batch_decoded.is_empty() {
                    let batch_block_info: Vec<(u64, Vec<u8>)> = batch_decoded
                        .iter()
                        .map(|d| (d.block_number, d.block_hash.clone()))
                        .collect();

                    self.batch_persist_decoded_blocks(&batch_decoded, BlockStatus::Finalized)
                        .await?;

                    for (num, hash) in &batch_block_info {
                        processed_count += 1;
                        let idx = (*num - chunk_start) as usize;
                        if idx < completed.len() {
                            completed[idx] = Some(hash.clone());
                        }
                        crate::metrics::processed_finalized_block_number()?
                            .with_label_values(
                                [&self.worker_id.to_string(), "concurrent_range"].as_slice(),
                            )
                            .set(*num as i64);
                    }

                    // Final checkpoint advance for this chunk
                    while (last_checkpoint + 1 - chunk_start) < range_len as u64 {
                        let next_idx = (last_checkpoint + 1 - chunk_start) as usize;
                        if next_idx < completed.len() && completed[next_idx].is_some() {
                            last_checkpoint += 1;
                        } else {
                            break;
                        }
                    }
                    if last_checkpoint >= chunk_start {
                        let cp_idx = (last_checkpoint - chunk_start) as usize;
                        if let Some(ref hash_bytes) = completed[cp_idx] {
                            self.postgres
                                .set_last_indexed_finalized_block_number_and_hash(
                                    last_checkpoint,
                                    hash_bytes,
                                )
                                .await?;
                        }
                    }
                }
            }

            // completed and decode_handles are dropped here, freeing chunk memory

            let chunk_total = processed_count + fetch_error_count + decode_error_count;
            tracing::info!(
                "📦 Chunk {chunk_num}/{total_chunks} done: {processed_count}/{expected_count} processed, {fetch_error_count} fetch errors, {decode_error_count} decode errors."
            );

            total_processed += processed_count;
            total_fetch_errors += fetch_error_count;
            total_decode_errors += decode_error_count;

            if chunk_total != expected_count {
                let missing = expected_count - chunk_total;
                return Err(anyhow::anyhow!(
                    "Chunk block count mismatch: expected {}, received {}, missing {}",
                    expected_count,
                    chunk_total,
                    missing
                ));
            }

            if stop_on_error && (fetch_error_count > 0 || decode_error_count > 0) {
                return Err(anyhow::anyhow!(
                    "Chunk completed with errors: {} fetch errors, {} decode errors",
                    fetch_error_count,
                    decode_error_count
                ));
            }
        }

        let total_expected = end_block_number - start_block_number + 1;
        tracing::info!(
            "✅ Completed: {}/{} blocks processed, {} fetch errors, {} decode errors.",
            total_processed,
            total_expected,
            total_fetch_errors,
            total_decode_errors
        );

        if total_fetch_errors > 0 || total_decode_errors > 0 {
            return Err(anyhow::anyhow!(
                "Completed with errors: {} fetch errors, {} decode errors",
                total_fetch_errors,
                total_decode_errors
            ));
        }

        Ok(())
    }

    /// Fallback: concurrent hash fetching with sequential process_block() calls.
    /// Used when traces are enabled or reindex is requested.
    /// Processes the range in chunks of 10,000 blocks to avoid OOM on large ranges.
    async fn process_finalized_blocks_in_range_concurrent_sequential(
        &self,
        stop_on_error: bool,
        skip_traces: bool,
        reindex: bool,
        start_block_number: u64,
        end_block_number: u64,
        concurrency: usize,
    ) -> anyhow::Result<()> {
        let chunk_size: u64 = 10_000;
        let total_blocks = end_block_number - start_block_number + 1;
        let total_chunks = (total_blocks + chunk_size - 1) / chunk_size;

        let mut total_processed: u64 = 0;
        let mut total_fetch_errors: u64 = 0;
        let mut total_process_errors: u64 = 0;

        for chunk_start in (start_block_number..=end_block_number).step_by(chunk_size as usize) {
            let chunk_end = min(chunk_start + chunk_size - 1, end_block_number);
            let chunk_num = (chunk_start - start_block_number) / chunk_size + 1;
            tracing::info!(
                "📦 Chunk {chunk_num}/{total_chunks}: blocks {chunk_start}-{chunk_end}."
            );

            let mut rx = concurrent::fetch_hashes_range(
                &self.substrate_client,
                chunk_start,
                chunk_end,
                concurrency,
            )
            .await;

            let expected_count = chunk_end - chunk_start + 1;
            let mut processed_count: u64 = 0;
            let mut fetch_error_count: u64 = 0;
            let mut process_error_count: u64 = 0;

            let range_len = (chunk_end - chunk_start + 1) as usize;
            let mut completed: Vec<Option<Vec<u8>>> = vec![None; range_len];
            let mut last_checkpoint = chunk_start.saturating_sub(1);

            while let Some(result) = rx.recv().await {
                let block_hash = match result {
                    Ok(h) => h,
                    Err(e) => {
                        fetch_error_count += 1;
                        tracing::error!("❌ Fetch error: {e:?}");
                        if stop_on_error {
                            return Err(e);
                        }
                        continue;
                    }
                };

                if let Err(error) = self
                    .process_block(
                        skip_traces,
                        reindex,
                        &block_hash.hash_hex,
                        block_hash.number,
                        BlockStatus::Finalized,
                    )
                    .await
                {
                    process_error_count += 1;
                    let hash = hex::decode(&block_hash.hash_hex).unwrap_or_default();
                    tracing::error!("❌ Error processing block {}: {error:?}", block_hash.number);
                    self.save_block_error(
                        &hash,
                        block_hash.number,
                        BlockStatus::Finalized,
                        &error.to_string(),
                    )
                    .await?;
                    if stop_on_error {
                        return Err(error);
                    }
                    continue;
                }

                processed_count += 1;

                let idx = (block_hash.number - chunk_start) as usize;
                if idx < completed.len() {
                    let hash_bytes = hex::decode(&block_hash.hash_hex).unwrap_or_default();
                    completed[idx] = Some(hash_bytes);
                }

                while (last_checkpoint + 1 - chunk_start) < range_len as u64 {
                    let next_idx = (last_checkpoint + 1 - chunk_start) as usize;
                    if next_idx < completed.len() && completed[next_idx].is_some() {
                        last_checkpoint += 1;
                    } else {
                        break;
                    }
                }

                if last_checkpoint >= chunk_start {
                    let cp_idx = (last_checkpoint - chunk_start) as usize;
                    if let Some(ref hash_bytes) = completed[cp_idx] {
                        self.postgres
                            .set_last_indexed_finalized_block_number_and_hash(
                                last_checkpoint,
                                hash_bytes,
                            )
                            .await?;
                    }
                }

                crate::metrics::processed_finalized_block_number()?
                    .with_label_values([&self.worker_id.to_string(), "concurrent_range"].as_slice())
                    .set(block_hash.number as i64);
            }

            // completed is dropped here, freeing chunk memory

            let chunk_total = processed_count + fetch_error_count + process_error_count;
            tracing::info!(
                "📦 Chunk {chunk_num}/{total_chunks} done: {processed_count}/{expected_count} processed, {fetch_error_count} fetch errors, {process_error_count} process errors."
            );

            total_processed += processed_count;
            total_fetch_errors += fetch_error_count;
            total_process_errors += process_error_count;

            if chunk_total != expected_count {
                let missing = expected_count - chunk_total;
                return Err(anyhow::anyhow!(
                    "Chunk block count mismatch: expected {}, received {}, missing {}",
                    expected_count,
                    chunk_total,
                    missing
                ));
            }

            if stop_on_error && (fetch_error_count > 0 || process_error_count > 0) {
                return Err(anyhow::anyhow!(
                    "Chunk completed with errors: {} fetch errors, {} process errors",
                    fetch_error_count,
                    process_error_count
                ));
            }
        }

        tracing::info!(
            "✅ Completed: {}/{} blocks processed, {} fetch errors, {} process errors.",
            total_processed,
            total_blocks,
            total_fetch_errors,
            total_process_errors
        );

        if total_fetch_errors > 0 || total_process_errors > 0 {
            return Err(anyhow::anyhow!(
                "Completed with errors: {} fetch errors, {} process errors",
                total_fetch_errors,
                total_process_errors
            ));
        }

        Ok(())
    }

    pub(crate) async fn process_finalized_blocks_in_range(
        &self,
        stop_on_error: bool,
        skip_traces: bool,
        scan: bool,
        reindex: bool,
        maybe_start_block_number: Option<u64>,
        maybe_end_block_number: Option<u64>,
    ) -> anyhow::Result<()> {
        validate_block_range(maybe_start_block_number, maybe_end_block_number)?;
        let (start_block_number, end_block_number) = self
            .get_actual_finalized_block_range(
                maybe_start_block_number,
                maybe_end_block_number,
                scan,
            )
            .await?;
        tracing::info!("⚙️ Process finalized blocks {start_block_number}-{end_block_number}.");
        for number in start_block_number..=end_block_number {
            let Some(hash_hex) = self.substrate_client.get_block_hash(number).await? else {
                anyhow::bail!("Finalized block {number} not found on the RPC node.");
            };
            let hash = hex::decode(&hash_hex)?;
            tracing::info!(
                "🔧 Processing finalized block [{number}][0x{}]. Target {end_block_number}.",
                truncate_hash(&hash_hex),
            );
            match self
                .process_block(
                    skip_traces,
                    reindex,
                    &hash_hex,
                    number,
                    BlockStatus::Finalized,
                )
                .await
            {
                Ok(_) => crate::metrics::processed_finalized_block_number()?
                    .with_label_values([&self.worker_id.to_string(), "finalized_range"].as_slice())
                    .set(number as i64),
                Err(error) => {
                    tracing::error!(
                        "❌ Error while processing finalized block {number}: {error:?}"
                    );
                    self.save_block_error(
                        &hash,
                        number,
                        BlockStatus::Finalized,
                        &error.to_string(),
                    )
                    .await?;
                    if stop_on_error {
                        return Err(error);
                    }
                }
            }
        }
        tracing::info!(
            "✅ Completed processing finalized blocks {start_block_number}-{end_block_number}."
        );
        Ok(())
    }

    async fn process_block_0(
        &self,
        block_hash: &[u8],
        block_header: &BlockHeader,
        spec_version: u32,
        status: BlockStatus,
    ) -> anyhow::Result<()> {
        let mut tx = self.postgres.connection_pool.begin().await?;
        self.postgres
            .ingest_block(
                block_hash,
                block_header,
                None,
                status,
                &None,
                spec_version,
                0,
                0,
                &None,
                &mut tx,
            )
            .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn prune_other_blocks_with_number(
        &self,
        block_number: u64,
        block_hash: &[u8],
        tx: &mut Transaction<'_, Postgres>,
    ) -> anyhow::Result<()> {
        let blocks = self
            .postgres
            .get_blocks_by_number_with_tx(block_number, tx)
            .await?;
        for block in blocks.iter() {
            if block.hash != block_hash {
                tracing::info!(
                    "✂️ Prune block [{block_number}][0x{}].",
                    truncate_hash(&hex::encode(&block.hash)),
                );
                self.postgres
                    .update_block_status(&block.hash, BlockStatus::Pruned, tx)
                    .await?;
            }
        }
        Ok(())
    }

    pub(crate) async fn get_block_hash_hex(
        &self,
        block_number: u64,
    ) -> anyhow::Result<Option<String>> {
        self.substrate_client.get_block_hash(block_number).await
    }

    async fn get_block_author(
        &self,
        block_hash_hex: &str,
        spec_version: u32,
        block_header: &BlockHeader,
    ) -> anyhow::Result<Option<MultiAddress>> {
        let block_hash = hex::decode(block_hash_hex)?;
        let is_nimbus = get_parsed_metadata(
            &block_hash,
            spec_version,
            &self.postgres,
            &self.substrate_client,
            &self.legacy_decode_api_client,
        )
        .await?
        .has_storage_item(AUTHOR_INHERENT_PALLET_NAME, AUTHOR_STORAGE_ITEM_NAME);
        let author_multi_address = if is_nimbus {
            self.substrate_client
                .get_nimbus_block_author(block_hash_hex)
                .await?
        } else if let Some(validator_index) = block_header.get_validator_index()? {
            let session_index = self
                .substrate_client
                .get_current_session_index(block_hash_hex)
                .await?;
            let mut session_validators_cache = SESSION_VALIDATORS_CACHE.write().await;
            let validator_addresses = {
                if session_validators_cache.0 != session_index
                    || session_validators_cache.1.is_empty()
                {
                    let metadata = get_metadata(
                        &block_hash,
                        spec_version,
                        &self.postgres,
                        &self.substrate_client,
                        &self.legacy_decode_api_client,
                    )
                    .await?;
                    let sequence_type_path =
                        if get_metadata_version(&metadata) < METADATA_VERSION_LEGACY_THRESHOLD {
                            ACCOUNT_ID_32_TYPE_PATH.to_string()
                        } else {
                            let session_validators_type = get_pallet_storage_item_type_by_name(
                                &metadata,
                                SESSION_PALLET_NAME,
                                VALIDATORS_STORAGE_ITEM_NAME,
                            )?
                            .ok_or(anyhow::Error::msg(format!(
                                "Session.Validators storage item not found in {} metadata.",
                                self.chain_name
                            )))?;
                            match &session_validators_type.ty.type_def {
                                scale_info::TypeDef::Sequence(sequence_type) => {
                                    let sequence_type = get_metadata_type_by_id(
                                        &metadata,
                                        sequence_type.type_param.id,
                                    )?
                                    .ok_or(anyhow::Error::msg(format!(
                                    "Session.Validators sequence type not found in {} metadata.",
                                    self.chain_name
                                )))?;
                                    sequence_type.ty.path.segments.join("::")
                                }
                                _ => anyhow::bail!(
                                    "Unexpected non-sequence type for Session.Validators: {:?}",
                                    session_validators_type.ty.type_def
                                ),
                            }
                        };
                    let validator_multi_addresses: Vec<MultiAddress> =
                        match sequence_type_path.as_str() {
                            ACCOUNT_ID_20_TYPE_PATH => self
                                .substrate_client
                                .get_active_validator_account_ids::<[u8; 20]>(block_hash_hex)
                                .await?
                                .iter()
                                .map(|address| MultiAddress::Address20(*address))
                                .collect(),
                            ACCOUNT_ID_32_TYPE_PATH => self
                                .substrate_client
                                .get_active_validator_account_ids::<AccountId>(block_hash_hex)
                                .await?
                                .iter()
                                .map(|account_id| MultiAddress::Id(*account_id))
                                .collect(),
                            _ => anyhow::bail!(
                            "Unexpected sequence type for Session.Validators: {sequence_type_path}"
                        ),
                        };
                    session_validators_cache.0 = session_index;
                    session_validators_cache.1 = validator_multi_addresses;
                }
                &session_validators_cache.1
            };
            let validator_index = validator_index % validator_addresses.len() as u32;
            if let Some(author_multi_address) = validator_addresses.get(validator_index as usize) {
                Some(author_multi_address.clone())
            } else {
                anyhow::bail!("Author validator was not found at index {validator_index}.");
            }
        } else {
            None
        };
        Ok(author_multi_address)
    }

    pub(crate) async fn process_block(
        &self,
        skip_traces: bool,
        reindex: bool,
        block_hash_hex: &str,
        block_number: u64,
        status: BlockStatus,
    ) -> anyhow::Result<()> {
        let block_hash = hex::decode(block_hash_hex)?;
        let truncated_block_hash = truncate_hash(block_hash_hex);
        let mut tx = self.postgres.connection_pool.begin().await?;
        if let Some(block_row) = self.postgres.get_block_by_hash(&block_hash).await? {
            tracing::info!(
                "👍 Block [{block_number}][{truncated_block_hash}] had already been processed."
            );
            if reindex {
                tracing::info!(
                    "🗑️  Deleting block [{block_number}][{truncated_block_hash}] and its traces for reindexing.",
                );
                self.postgres
                    .delete_block_and_traces_by_block_hash(&block_hash, &mut tx)
                    .await?;
            } else {
                if block_row.status != status && status == BlockStatus::Finalized {
                    let start_time = std::time::Instant::now();
                    tracing::info!(
                        "🔁 Update block [{block_number}][0x{truncated_block_hash}] status: {} ➡️ {status}",
                        block_row.status,
                    );
                    self.postgres
                        .update_block_status(&block_hash, status, &mut tx)
                        .await?;
                    self.prune_other_blocks_with_number(block_number, &block_hash, &mut tx)
                        .await?;
                    crate::metrics::block_status_update_time_ms()?
                        .with_label_values(&[&self.worker_id.to_string()])
                        .observe(start_time.elapsed().as_millis() as f64);
                }
                tx.commit().await?;
                return Ok(());
            }
        }
        let start_time = std::time::Instant::now();
        let block_header = self
            .substrate_client
            .get_block_header(block_hash_hex)
            .await?;
        if block_number == 0 {
            let spec_version = self
                .substrate_client
                .get_last_runtime_upgrade_info(block_hash_hex)
                .await?
                .spec_version;
            self.process_block_0(&block_hash, &block_header, spec_version, status)
                .await?;
            return Ok(());
        }
        let spec_version = self
            .substrate_client
            .get_last_runtime_upgrade_info(&block_header.parent_hash)
            .await?
            .spec_version;
        let parent_hash = hex::decode(block_header.parent_hash.trim_start_matches("0x"))?;
        let metadata = get_metadata(
            &parent_hash,
            spec_version,
            &self.postgres,
            &self.substrate_client,
            &self.legacy_decode_api_client,
        )
        .await?;
        let parsed_metadata = get_parsed_metadata(
            &parent_hash,
            spec_version,
            &self.postgres,
            &self.substrate_client,
            &self.legacy_decode_api_client,
        )
        .await?;
        let author_multi_address = self
            .get_block_author(block_hash_hex, spec_version, &block_header)
            .await?;
        let block_timestamp = self
            .substrate_client
            .get_block_timestamp(block_hash_hex)
            .await?;
        let (events, extrinsics, weight) = if skip_traces {
            let event_bytes = self
                .substrate_client
                .get_block_event_bytes(block_hash_hex)
                .await?;
            let events = self
                .get_events_from_event_bytes(&block_hash, spec_version, &metadata, event_bytes)
                .await?;
            let extrinsics = self
                .get_extrinsics(&block_hash, spec_version, &metadata, &events)
                .await?;
            let weight = self
                .get_block_weight_from_rpc(&block_hash, spec_version, &metadata)
                .await?;
            (events, extrinsics, weight)
        } else {
            let trace = self
                .substrate_client
                .get_block_trace(block_hash_hex)
                .await?;
            for (trace_index, event) in trace.events.iter().enumerate() {
                let key = hex::decode(event.data_wrapper.data.key.trim_start_matches("0x"))
                    .context(format!(
                        "Cannot decode key for trace #{} in block #{}.",
                        trace_index, block_number,
                    ))?;
                let ext_id = hex::decode(event.data_wrapper.data.ext_id.trim_start_matches("0x"))
                    .context(format!(
                    "Cannot decode ext id for trace #{} in block #{}.",
                    trace_index, block_number,
                ))?;
                let value = if event.data_wrapper.data.value.is_empty()
                    || event.data_wrapper.data.value.eq_ignore_ascii_case("none")
                {
                    None
                } else if let Some(inner) = event
                    .data_wrapper
                    .data
                    .value
                    .to_lowercase()
                    .strip_prefix("some(")
                    .and_then(|s| s.strip_suffix(')'))
                {
                    Some(
                        hex::decode(inner)
                            .context("Cannot decode trace value hexadecimal string.")?,
                    )
                } else {
                    Some(
                        hex::decode(&event.data_wrapper.data.value)
                            .context("Cannot decode trace value hexadecimal string.")?,
                    )
                };
                // find storage item
                let storage_item = parsed_metadata
                    .pallets
                    .iter()
                    .flat_map(|pallet| &pallet.storage_items)
                    .find(|item| key.starts_with(&item.key_prefix));
                // check for known key
                let is_known_key = matches!(
                    key.as_slice(),
                    sp_storage::well_known_keys::CHILD_STORAGE_KEY_PREFIX
                        | sp_storage::well_known_keys::CODE
                        | sp_storage::well_known_keys::DEFAULT_CHILD_STORAGE_KEY_PREFIX
                        | sp_storage::well_known_keys::EXTRINSIC_INDEX
                        | sp_storage::well_known_keys::HEAP_PAGES
                        | sp_storage::well_known_keys::INTRABLOCK_ENTROPY
                        | TRANSACTION_LEVEL_KEY,
                );
                if storage_item.is_none() && !is_known_key {
                    tracing::warn!(
                        "Trace {trace_index} of block [{block_number}][0x{}] has unknown key: 0x{}",
                        truncate_hash(block_hash_hex),
                        event.data_wrapper.data.key
                    );
                }

                let (key_prefix, key_params) = if let Some(storage_item) = storage_item {
                    (
                        storage_item.key_prefix.as_slice(),
                        if key.len() > storage_item.key_prefix.len() {
                            key.get(storage_item.key_prefix.len()..)
                        } else {
                            None
                        },
                    )
                } else {
                    (key.as_slice(), None)
                };
                // ingest
                self.postgres
                    .ingest_block_trace(
                        &block_hash,
                        block_number,
                        spec_version,
                        trace_index as u32,
                        key_prefix,
                        key_params,
                        value.as_deref(),
                        &ext_id,
                        &event.data_wrapper.data.storage_method,
                        event.parent_id.as_deref(),
                        storage_item.map(|storage_item| storage_item.id),
                        is_known_key,
                        &mut tx,
                    )
                    .await?;
            }

            let event_count = trace.get_event_count()?;
            let events = self
                .get_events_from_trace(&block_hash, spec_version, &metadata, &trace)
                .await?;
            if event_count != events.len() as u32 {
                tracing::warn!(
                    "❌ Expected event count {event_count} is not equal to decoded event count {}.",
                    events.len()
                );
            }

            let extrinsic_count = trace.get_extrinsic_count()?;
            let extrinsics = self
                .get_extrinsics_from_trace(&block_hash, spec_version, &metadata, &trace, &events)
                .await?;
            if extrinsic_count != extrinsics.len() as u32 {
                anyhow::bail!(
                    "❌ Expected extrinsic count {extrinsic_count} is not equal to decoded event count {}.",
                    extrinsics.len()
                );
            }
            let weight = self
                .get_block_weight_from_trace(&block_hash, spec_version, &metadata, &trace)
                .await?;
            tracing::info!(
                block_number,
                "Processed and persisted {} traces.",
                trace.events.len()
            );
            (events, extrinsics, weight)
        };
        tracing::info!(block_number, "Decoded {} extrinsics.", extrinsics.len(),);
        tracing::info!(block_number, "Decoded {} events.", events.len(),);
        // persist block, events, and extrinsics
        if status == BlockStatus::Finalized {
            self.prune_other_blocks_with_number(block_number, &block_hash, &mut tx)
                .await?;
        }
        self.postgres
            .ingest_block(
                &block_hash,
                &block_header,
                block_timestamp,
                status,
                &weight,
                spec_version,
                extrinsics.len() as u32,
                events.len() as u32,
                &author_multi_address,
                &mut tx,
            )
            .await?;
        self.postgres
            .ingest_block_logs(&block_hash, &block_header, &mut tx)
            .await?;
        tracing::info!("Persisted block and logs.");
        self.process_events(
            &block_hash,
            &block_header,
            block_timestamp,
            spec_version,
            status,
            &events,
            &extrinsics,
            &mut tx,
        )
        .await?;
        tracing::info!("Persisted {} events.", events.len());
        self.process_extrinsics(
            &block_hash,
            &block_header,
            block_timestamp,
            spec_version,
            status,
            &extrinsics,
            &mut tx,
        )
        .await?;
        tracing::info!("Persisted {} extrinsics.", extrinsics.len());
        self.postgres.delete_error(&block_hash, &mut tx).await?;
        tx.commit().await?;

        let log_emoji = match status {
            BlockStatus::Proposed => "🟦",
            BlockStatus::Pruned => "⬜",
            BlockStatus::Finalized => "🟩",
        };
        let elapsed_time_ms = start_time.elapsed().as_millis();
        crate::metrics::block_processing_time_ms()?
            .with_label_values(&[&self.worker_id.to_string()])
            .observe(elapsed_time_ms as f64);
        tracing::info!(
            "{log_emoji} Processed {status} block [{block_number}][0x{}] in {elapsed_time_ms} ms.",
            truncate_hash(block_hash_hex),
        );
        Ok(())
    }
}
