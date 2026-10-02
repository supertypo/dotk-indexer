//! Undo after apply is the identity on all four tables, over seeded random event sequences.

use crate::common::*;
use dotk_indexer::chain::ChainBlock;
use dotk_indexer::model::HistoryOp;
use sqlx::PgPool;

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn below(&mut self, n: usize) -> usize {
        usize::try_from(self.next() % n as u64).unwrap()
    }
}

struct Driver {
    world: World,
    /// (key, name, owner). Activation needs the preimage.
    pendings: Vec<([u8; 32], String, [u8; 32])>,
    actives: Vec<String>,
    counter: u64,
}

impl Driver {
    fn new() -> (Self, sim::SimTx) {
        let (world, genesis_tx) = World::new();
        (Self { world, pendings: Vec::new(), actives: Vec::new(), counter: 0 }, genesis_tx)
    }

    fn random_op(&mut self, rng: &mut Rng) -> sim::SimTx {
        loop {
            let tx = match rng.below(6) {
                0 | 1 => Some(self.random_split(rng)),
                2 => self.random_activate(rng),
                3 => self.random_transfer(rng),
                4 => self.random_release(rng),
                _ => self.random_evict(rng),
            };
            if let Some(tx) = tx {
                return tx;
            }
        }
    }

    fn random_split(&mut self, rng: &mut Rng) -> sim::SimTx {
        self.counter += 1;
        let name = format!("n{}-{}", self.counter, rng.below(1000));
        let owner = OWNER_A;
        let tx = self.world.split(&name, &owner);
        self.pendings.push((dotk_core::key_of(&name), name, owner));
        tx
    }

    fn random_activate(&mut self, rng: &mut Rng) -> Option<sim::SimTx> {
        if self.pendings.is_empty() {
            return None;
        }
        let (_, name, owner) = self.pendings.swap_remove(rng.below(self.pendings.len()));
        let tx = self.world.activate(&name, &owner);
        self.actives.push(name);
        Some(tx)
    }

    fn random_transfer(&mut self, rng: &mut Rng) -> Option<sim::SimTx> {
        if self.actives.is_empty() {
            return None;
        }
        let name = self.actives[rng.below(self.actives.len())].clone();
        let to = curve_owner(rng.next().to_le_bytes()[0]);
        if rng.below(2) == 0 {
            let records = records_named(&name);
            return Some(self.world.transfer_with_card(&name, &to, &records, &to));
        }
        Some(self.world.transfer(&name, &to))
    }

    fn random_release(&mut self, rng: &mut Rng) -> Option<sim::SimTx> {
        if self.actives.is_empty() {
            return None;
        }
        let name = self.actives.swap_remove(rng.below(self.actives.len()));
        Some(self.world.release(&name))
    }

    fn random_evict(&mut self, rng: &mut Rng) -> Option<sim::SimTx> {
        if self.pendings.is_empty() {
            return None;
        }
        let (key, _, _) = self.pendings.swap_remove(rng.below(self.pendings.len()));
        Some(self.world.evict(&key))
    }

    /// A standalone sweep journals under `KEY_MIN` and writes history under the names whose live card it takes.
    fn random_sweep(&mut self, rng: &mut Rng) -> Option<sim::SimTx> {
        // Prefer a live card, because that sweep is rarer. The retired cards of the spender ride along
        // and must write no history.
        let live = self.world.live_card_spenders();
        let spenders = if live.is_empty() { self.world.card_spenders() } else { live };
        if spenders.is_empty() {
            return None;
        }
        Some(self.world.sweep_cards(&spenders[rng.below(spenders.len())]))
    }
}

/// History compares by content, never by `id`, because a re-applied block writes the same events under higher ids.
async fn dump(pool: &PgPool) -> String {
    let mut conn = pool.acquire().await.unwrap();
    let deeds = dotk_indexer::db::all_deeds(&mut conn).await.unwrap();
    let gaps = dotk_indexer::db::all_gaps(&mut conn).await.unwrap();
    let cards = dotk_indexer::db::all_cards(&mut conn).await.unwrap();
    serde_json::to_string(&(
        deeds.iter().map(|(k, r)| (faster_hex::hex_string(k), r)).collect::<Vec<_>>(),
        gaps.iter().map(|(lo, g)| (faster_hex::hex_string(lo), g)).collect::<Vec<_>>(),
        cards
            .iter()
            .map(|c| (faster_hex::hex_string(&c.txid), c.idx, faster_hex::hex_string(&c.state.key), &c.blob, c.value, c.swept_at))
            .collect::<Vec<_>>(),
        history_dump(&mut conn).await,
    ))
    .unwrap()
}

async fn history_dump(conn: &mut sqlx::PgConnection) -> Vec<(String, i16, i16, i32, i64, String, String)> {
    let history = sqlx::query_as::<_, (Vec<u8>, i16, i16, i32, i64, Vec<u8>, Vec<u8>)>(
        "SELECT key, op, card, seq, blue_score, block_hash, txid FROM history \
         ORDER BY blue_score, seq, key, op",
    )
    .fetch_all(&mut *conn)
    .await
    .unwrap();
    history
        .into_iter()
        .map(|(k, op, card, seq, blue, hash, txid)| {
            (faster_hex::hex_string(&k), op, card, seq, blue, faster_hex::hex_string(&hash), faster_hex::hex_string(&txid))
        })
        .collect()
}

/// Numbers the blocks of the run, so every hash is distinct.
struct Blocks {
    blue: u64,
    count: u64,
}

impl Blocks {
    fn segment(&mut self, driver: &mut Driver, rng: &mut Rng) -> Vec<ChainBlock> {
        let mut blocks: Vec<ChainBlock> = Vec::new();
        for _ in 0..=rng.below(4) {
            let mut txs = Vec::new();
            for _ in 0..=rng.below(3) {
                let t = driver.random_op(rng);
                txs.push(t.accepted());
            }
            if rng.below(2) == 0
                && let Some(t) = driver.random_sweep(rng)
            {
                txs.push(t.accepted());
            }
            self.blue += 1;
            self.count += 1;
            let mut hash = [0x77u8; 32];
            hash[..8].copy_from_slice(&self.count.to_le_bytes());
            blocks.push(ChainBlock { hash, blue_score: self.blue, daa_score: self.blue, timestamp: self.blue * 1000, txs });
        }
        blocks
    }
}

async fn apply(pool: &PgPool, blocks: &[ChainBlock]) {
    let watch = genesis_file().watch_templates().unwrap();
    let params = test_params();
    for b in blocks {
        let mut tx = pool.begin().await.unwrap();
        dotk_indexer::chain::apply_block(&mut tx, &watch, &params, REGISTRY_COVENANT_ID, b).await.unwrap();
        tx.commit().await.unwrap();
    }
}

async fn undo(pool: &PgPool, blocks: &[ChainBlock]) {
    let mut tx = pool.begin().await.unwrap();
    for b in blocks.iter().rev() {
        dotk_indexer::chain::undo_block(&mut tx, &b.hash).await.unwrap();
    }
    tx.commit().await.unwrap();
}

#[tokio::test]
async fn undo_restores_the_exact_pre_state_and_reapply_reproduces_it() {
    let pool = fresh_pool("undo_prop").await;
    dotk_indexer::db::migrate(&pool).await.unwrap();
    let (mut driver, _genesis_tx) = Driver::new();
    let mut rng = Rng(0x00DE_C0DE);
    let mut chain = Blocks { blue: 100, count: 0 };

    // Each segment is kept after its undo and re-apply, so later segments run on deeper history.
    for segment in 0..24 {
        let before = dump(&pool).await;
        let blocks = chain.segment(&mut driver, &mut rng);
        apply(&pool, &blocks).await;
        let after_apply = dump(&pool).await;
        undo(&pool, &blocks).await;
        assert_eq!(dump(&pool).await, before, "segment {segment}: undo must restore the exact pre-state");
        apply(&pool, &blocks).await;
        assert_eq!(dump(&pool).await, after_apply, "segment {segment}: re-apply must reproduce the applied state");
    }

    let mut conn = pool.acquire().await.unwrap();
    assert_cached_gaps(&mut conn).await;
    assert_every_card_path(&mut conn).await;
}

/// Checks the cached gap rows, which handlers maintain incrementally. `partitions(derive_gaps(keys))`
/// holds for any input, so it checks nothing.
async fn assert_cached_gaps(conn: &mut sqlx::PgConnection) {
    let rows = dotk_indexer::db::all_deeds(conn).await.unwrap();
    let keys: Vec<[u8; 32]> = rows.iter().map(|(k, _)| *k).collect();
    let mut cached: Vec<dotk_core::state::GapState> =
        dotk_indexer::db::all_gaps(conn).await.unwrap().into_iter().map(|(lo, g)| g.state(lo)).collect();
    cached.sort_by_key(|g| g.lo);
    assert_eq!(cached, dotk_indexer::derive::derive_gaps(&rows), "the cached gap rows must be exactly the derived ones");
    assert!(dotk_core::registry::partitions(&cached, &keys), "gaps + deed keys must partition the keyspace");
}

/// The seed is fixed, so this makes sure that the run reaches every card path.
async fn assert_every_card_path(conn: &mut sqlx::PgConnection) {
    let cards = dotk_indexer::db::all_cards(conn).await.unwrap();
    assert!(!cards.is_empty(), "the sequence minted no card, so the cards half of the property proved nothing");
    assert!(cards.iter().any(|c| c.swept_at.is_some()), "no card was ever swept");
    let (changes, sweeps): (i64, i64) =
        sqlx::query_as("SELECT count(*) FILTER (WHERE card <> 0), count(*) FILTER (WHERE op = $1) FROM history")
            .bind(HistoryOp::Sweep as i16)
            .fetch_one(&mut *conn)
            .await
            .unwrap();
    assert!(changes > 0, "no history row recorded a record change");
    assert!(sweeps > 0, "no standalone sweep of a live card was ever reached");
}
