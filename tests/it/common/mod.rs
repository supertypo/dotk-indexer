pub(crate) mod sim;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use dotk_core::cards::{self, CARD_VALUE, CardInput, CardMint, Records};
use dotk_core::intents::{self, DeedUtxo, GapUtxo, Outpoint, TxIntent};
use dotk_core::registry::{KEY_MAX, KEY_MIN};
use dotk_core::state::{DeedState, GapState, OwnerType, Status};
use dotk_core::watch::GenesisFile;
use dotk_core::{Params, Templates};
use kaspa_addresses::Prefix;
use sqlx::PgPool;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::SyncRunner;
use testcontainers_modules::testcontainers::{Container, ImageExt};
use tower::util::ServiceExt;

use dotk_indexer::app::{App, Progress};
use dotk_indexer::chain::Kaspad;
use dotk_indexer::config::CliArgs;
use dotk_indexer::db;
use dotk_indexer::model::DeedRow;
use dotk_indexer::snapshot;

use sim::{SimKaspad, SimOut, SimTx};

/// Real x-only keys, because `transfer_intent` refuses an off-curve owner.
pub(crate) const OWNER_A: [u8; 32] = [0xA1; 32];
pub(crate) const OWNER_B: [u8; 32] = [0xB5; 32];

pub(crate) fn records_named(name: &str) -> Records {
    let mut r = Records::new();
    r.insert("url".into(), cards::RecordValue::Text(format!("https://{name}.example")));
    r
}

/// About half of all 32-byte values are off the curve.
pub(crate) fn curve_owner(seed: u8) -> [u8; 32] {
    let mut secret = [1u8; 32];
    secret[31] = seed;
    let compressed = dotk_core::sign::compressed_public_key(&secret).expect("a valid secret key");
    compressed[1..].try_into().expect("the compressed key's tail IS the x-only key")
}
pub(crate) const REGISTRY_COVENANT_ID: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

pub(crate) const PAYOUT_KEY: &str = "7777777777777777777777777777777777777777777777777777777777777777";

pub(crate) fn payout_address() -> kaspa_addresses::Address {
    let compressed = dotk_core::sign::compressed_public_key(&{
        let mut s = [0u8; 32];
        faster_hex::hex_decode(PAYOUT_KEY.as_bytes(), &mut s).unwrap();
        s
    })
    .unwrap();
    kaspa_addresses::Address::new(Prefix::Testnet, kaspa_addresses::Version::PubKey, &compressed[1..])
}

pub(crate) fn devfund_address() -> kaspa_addresses::Address {
    let spk = test_params().devfund_spk_bytes().unwrap();
    let spk = kaspa_consensus_core::tx::ScriptPublicKey::from_vec(0, spk);
    kaspa_txscript::extract_script_pub_key_address(&spk, Prefix::Testnet).unwrap()
}

pub(crate) const PAYOUT_SEED_VALUE: u64 = 1_000_000;

/// The evictor's funding input. It must be in the genesis block.
pub(crate) fn payout_seed_tx() -> SimTx {
    SimTx {
        txid: [0x77; 32],
        sig_scripts: vec![],
        inputs: vec![],
        outputs: vec![SimOut {
            value: PAYOUT_SEED_VALUE,
            spk: kaspa_txscript::pay_to_address_script(&payout_address()).script().to_vec(),
            covenant_id: None,
        }],
        payload: vec![],
    }
}

/// Its `t_evict` is short.
pub(crate) fn genesis_file() -> GenesisFile {
    serde_json::from_str(dotk_core::TEST_GENESIS).expect("the test deployment")
}

pub(crate) fn test_params() -> Params {
    genesis_file().params
}

pub(crate) fn templates() -> &'static Templates {
    static T: OnceLock<Templates> = OnceLock::new();
    T.get_or_init(|| Templates::from_manifest(&genesis_file()).expect("the test deployment's templates"))
}

/// Clone it to build conflicting spends of one outpoint.
#[derive(Clone)]
pub(crate) struct World {
    pub gaps: Vec<GapUtxo>,
    pub deeds: Vec<DeedUtxo>,
    pub names: Vec<([u8; 32], String)>,
    /// Unswept cards, retired ones included.
    pub cards: Vec<CardInput>,
    next_txid: u64,
}

impl World {
    pub(crate) fn new() -> (Self, SimTx) {
        let t = templates();
        let genesis = t.genesis_gap();
        let spk = t.gap.p2sh_spk(&genesis.encode()).unwrap();
        let mut w = Self { gaps: Vec::new(), deeds: Vec::new(), names: Vec::new(), cards: Vec::new(), next_txid: 0x1000 };
        let txid = w.gen_txid();
        w.gaps.push(GapUtxo {
            outpoint: Outpoint { transaction_id: faster_hex::hex_string(&txid), index: 0 },
            value: t.params.gap_value,
            state: genesis,
            covenant_id: REGISTRY_COVENANT_ID.into(),
        });
        let genesis_tx = SimTx {
            txid,
            sig_scripts: vec![],
            inputs: vec![],
            outputs: vec![SimOut { value: t.params.gap_value, spk, covenant_id: Some(REGISTRY_COVENANT_ID.into()) }],
            payload: vec![],
        };
        (w, genesis_tx)
    }

    fn gen_txid(&mut self) -> [u8; 32] {
        let mut h = [0xF0u8; 32];
        h[..8].copy_from_slice(&self.next_txid.to_le_bytes());
        self.next_txid += 1;
        h
    }

    fn unhex(s: &str) -> Vec<u8> {
        let mut v = vec![0u8; s.len() / 2];
        faster_hex::hex_decode(s.as_bytes(), &mut v).unwrap();
        v
    }

    fn unhex32(s: &str) -> [u8; 32] {
        <[u8; 32]>::try_from(Self::unhex(s).as_slice()).unwrap()
    }

    fn sim_tx(&mut self, intent: &TxIntent) -> SimTx {
        let txid = self.gen_txid();
        SimTx {
            txid,
            sig_scripts: intent.inputs.iter().map(|i| Self::unhex(&i.sig_script)).collect(),
            inputs: intent.inputs.iter().map(|i| (Self::unhex32(&i.outpoint.transaction_id), i.outpoint.index, i.sequence)).collect(),
            outputs: intent
                .outputs
                .iter()
                .map(|o| SimOut {
                    value: o.value,
                    spk: Self::unhex(&o.spk),
                    covenant_id: o.covenant.as_ref().map(|c| c.covenant_id.clone()),
                })
                .collect(),
            payload: vec![],
        }
    }

    pub(crate) fn deed(&self, key: &[u8; 32]) -> &DeedUtxo {
        self.deeds.iter().find(|d| &d.state.key == key).expect("tracked deed")
    }

    pub(crate) fn name_of(&self, key: &[u8; 32]) -> String {
        self.names.iter().find(|(k, _)| k == key).map(|(_, n)| n.clone()).expect("a name this world registered")
    }

    pub(crate) fn covering(&self, key: &[u8; 32]) -> GapUtxo {
        self.gaps.iter().find(|g| g.state.contains(key)).expect("a covering gap exists").clone()
    }

    /// Seats 0 and 2 of the exit merge.
    pub(crate) fn flanks(&self, key: &[u8; 32]) -> (GapUtxo, GapUtxo) {
        let pred = self.gaps.iter().find(|g| &g.state.hi == key).expect("predecessor gap").clone();
        let succ = self.gaps.iter().find(|g| &g.state.lo == key).expect("successor gap").clone();
        (pred, succ)
    }

    fn gap_out(&mut self, txid: &str, index: u32, state: GapState) {
        self.gaps.retain(|g| g.state.lo != state.lo);
        self.gaps.push(GapUtxo {
            outpoint: Outpoint { transaction_id: txid.into(), index },
            value: templates().params.gap_value,
            state,
            covenant_id: REGISTRY_COVENANT_ID.into(),
        });
    }

    fn deed_out(&mut self, txid: &str, index: u32, value: u64, state: DeedState) {
        self.deeds.retain(|d| d.state.key != state.key);
        self.deeds.push(DeedUtxo {
            outpoint: Outpoint { transaction_id: txid.into(), index },
            value,
            state,
            covenant_id: REGISTRY_COVENANT_ID.into(),
        });
    }

    pub(crate) fn split(&mut self, name: &str, owner: &[u8; 32]) -> SimTx {
        let t = templates();
        let key = dotk_core::key_of(name);
        let covering = self.covering(&key);
        let intent = intents::split_intent(t, &covering, name, OwnerType::Pubkey, owner).unwrap();
        let tx = self.sim_tx(&intent);
        let txid = faster_hex::hex_string(&tx.txid);
        self.gap_out(&txid, 0, GapState { lo: covering.state.lo, hi: key });
        self.gap_out(&txid, 1, GapState { lo: key, hi: covering.state.hi });
        self.deed_out(&txid, 2, t.params.bond + t.params.deposit, intent.pending_state.unwrap());
        self.names.push((key, name.into()));
        tx
    }

    pub(crate) fn activate(&mut self, name: &str, owner: &[u8; 32]) -> SimTx {
        let t = templates();
        let pending = self.deed(&dotk_core::key_of(name)).clone();
        let intent = intents::activate_intent(t, &pending, name, OwnerType::Pubkey, owner).unwrap();
        let tx = self.sim_tx(&intent);
        let txid = faster_hex::hex_string(&tx.txid);
        self.deed_out(
            &txid,
            0,
            t.params.bond,
            DeedState::active(pending.state.key, OwnerType::Pubkey, *owner, dotk_core::padded_name(name)),
        );
        tx
    }

    pub(crate) fn register(&mut self, name: &str, owner: &[u8; 32]) -> (SimTx, SimTx) {
        (self.split(name, owner), self.activate(name, owner))
    }

    pub(crate) fn transfer(&mut self, name: &str, new_owner: &[u8; 32]) -> SimTx {
        self.transfer_with_witness(name, new_owner, 0)
    }

    /// `witness` is dead for a curve-owned deed, so any script number is valid.
    pub(crate) fn transfer_with_witness(&mut self, name: &str, new_owner: &[u8; 32], witness: i64) -> SimTx {
        let t = templates();
        let deed = self.deed(&dotk_core::key_of(name)).clone();
        let intent = intents::transfer_intent(t, &deed, OwnerType::Pubkey, new_owner, witness).unwrap();
        let tx = self.sim_tx(&intent);
        let txid = faster_hex::hex_string(&tx.txid);
        self.deed_out(&txid, 0, deed.value, DeedState { owner: *new_owner, ..deed.state });
        tx
    }

    /// Also sweeps every card this world holds for `spender`.
    pub(crate) fn transfer_with_card(&mut self, name: &str, new_owner: &[u8; 32], records: &Records, spender: &[u8; 32]) -> SimTx {
        self.transfer_card(name, new_owner, records, spender, true)
    }

    pub(crate) fn transfer_minting_for(&mut self, name: &str, new_owner: &[u8; 32], records: &Records, spender: &[u8; 32]) -> SimTx {
        self.transfer_card(name, new_owner, records, spender, false)
    }

    fn transfer_card(&mut self, name: &str, new_owner: &[u8; 32], records: &Records, spender: &[u8; 32], sweep: bool) -> SimTx {
        let t = templates();
        let deed = self.deed(&dotk_core::key_of(name)).clone();
        let intent = intents::transfer_intent(t, &deed, OwnerType::Pubkey, new_owner, 0).unwrap();
        let next = DeedState { owner_type: OwnerType::Pubkey, owner: *new_owner, ..deed.state };
        let blob = cards::encode_records(records).unwrap();
        let mint = CardMint::for_deed(&next, blob, OwnerType::Pubkey, *spender).unwrap();
        let mut tx = self.sim_tx(&intent);
        let idx = u32::try_from(tx.outputs.len()).unwrap();
        assert_eq!(idx, 1, "a transfer pins one output, so the card is output 1");
        tx.outputs.push(SimOut { value: CARD_VALUE, spk: mint.state.spk(), covenant_id: None });
        tx.payload = cards::encode_payload(Some(&mint)).unwrap();
        if sweep {
            self.append_sweeps(&mut tx, spender);
        }
        let txid = faster_hex::hex_string(&tx.txid);
        self.deed_out(&txid, 0, deed.value, next);
        self.cards.push(CardInput { outpoint: Outpoint { transaction_id: txid, index: idx }, value: CARD_VALUE, state: mint.state });
        tx
    }

    pub(crate) fn sweep_cards(&mut self, spender: &[u8; 32]) -> SimTx {
        let txid = self.gen_txid();
        let mut tx = SimTx { txid, sig_scripts: vec![], inputs: vec![], outputs: vec![], payload: vec![] };
        self.append_sweeps(&mut tx, spender);
        assert!(!tx.inputs.is_empty(), "nothing to sweep");
        tx.outputs.push(SimOut {
            value: tx.inputs.len() as u64 * CARD_VALUE,
            spk: [&[0x20u8][..], spender, &[0xac]].concat(),
            covenant_id: None,
        });
        tx
    }

    pub(crate) fn card_spenders(&self) -> Vec<[u8; 32]> {
        let mut out: Vec<[u8; 32]> = Vec::new();
        for c in &self.cards {
            if !out.contains(&c.state.spender) {
                out.push(c.state.spender);
            }
        }
        out
    }

    /// Spenders whose card sits at output 1 of its name's current deed transaction.
    pub(crate) fn live_card_spenders(&self) -> Vec<[u8; 32]> {
        let mut out: Vec<[u8; 32]> = Vec::new();
        for c in &self.cards {
            let live = self.deeds.iter().any(|d| {
                d.state.key == c.state.key && d.outpoint.transaction_id == c.outpoint.transaction_id && c.outpoint.index == 1
            });
            if live && !out.contains(&c.state.spender) {
                out.push(c.state.spender);
            }
        }
        out
    }

    fn append_sweeps(&mut self, tx: &mut SimTx, spender: &[u8; 32]) {
        let (mine, keep): (Vec<CardInput>, Vec<CardInput>) = self.cards.drain(..).partition(|c| &c.state.spender == spender);
        self.cards = keep;
        for c in mine {
            tx.sig_scripts.push(c.state.sweep_sig_script(None));
            tx.inputs.push((Self::unhex32(&c.outpoint.transaction_id), c.outpoint.index, 0));
        }
    }

    pub(crate) fn transfer_to(&mut self, name: &str, owner_type: OwnerType, new_owner: &[u8; 32]) -> SimTx {
        let t = templates();
        let deed = self.deed(&dotk_core::key_of(name)).clone();
        let intent = intents::transfer_intent(t, &deed, owner_type, new_owner, 0).unwrap();
        let tx = self.sim_tx(&intent);
        let txid = faster_hex::hex_string(&tx.txid);
        let state = DeedState { owner_type, owner: *new_owner, ..deed.state };
        self.deed_out(&txid, 0, deed.value, state);
        tx
    }

    pub(crate) fn transfer_to_script(&mut self, name: &str, script_hash: &[u8; 32]) -> SimTx {
        self.transfer_to(name, OwnerType::ScriptHash, script_hash)
    }

    pub(crate) fn release(&mut self, name: &str) -> SimTx {
        self.release_with_witness(name, 0)
    }

    pub(crate) fn release_with_witness(&mut self, name: &str, witness: i64) -> SimTx {
        let t = templates();
        let key = dotk_core::key_of(name);
        let deed = self.deed(&key).clone();
        let (pred, succ) = self.flanks(&key);
        let intent = intents::release_intent(t, &pred, &deed, &succ, witness).unwrap();
        let tx = self.sim_tx(&intent);
        self.exit(&tx, &key, &pred, &succ);
        tx
    }

    pub(crate) fn evict(&mut self, key: &[u8; 32]) -> SimTx {
        let t = templates();
        let deed = self.deed(key).clone();
        let (pred, succ) = self.flanks(key);
        let intent = intents::evict_intent(t, &pred, &deed, &succ).unwrap();
        let tx = self.sim_tx(&intent);
        self.exit(&tx, key, &pred, &succ);
        tx
    }

    fn exit(&mut self, tx: &SimTx, key: &[u8; 32], pred: &GapUtxo, succ: &GapUtxo) {
        let txid = faster_hex::hex_string(&tx.txid);
        self.deeds.retain(|d| &d.state.key != key);
        self.gaps.retain(|g| g.state.lo != succ.state.lo);
        self.gap_out(&txid, 0, GapState { lo: pred.state.lo, hi: succ.state.hi });
    }
}

pub(crate) fn gap_script_hash(gap: &GapState) -> [u8; 32] {
    dotk_core::blake2b(&templates().gap.materialize(&gap.encode()).unwrap())
}

pub(crate) fn name_inside(lo: &[u8; 32], hi: &[u8; 32], salt: &str) -> String {
    (0..10_000)
        .map(|i| format!("{salt}-{i}"))
        .find(|n| {
            let k = dotk_core::key_of(n);
            *lo < k && k < *hi
        })
        .expect("no probe name found inside the interval")
}

pub(crate) fn name_above(low: &[u8; 32], salt: &str) -> String {
    name_inside(low, &KEY_MAX, salt)
}

static POSTGRES: Mutex<Option<Container<Postgres>>> = Mutex::new(None);

/// One container per run, and one database per test.
fn pg_base() -> &'static str {
    static BASE: OnceLock<String> = OnceLock::new();
    BASE.get_or_init(|| {
        // The blocking runner owns a runtime, and a runtime cannot start inside a test's.
        std::thread::spawn(|| {
            // Every test holds a few connections, and the whole suite runs at once. The command
            // replaces the module's own, which is what turns fsync off.
            let pg = Postgres::default()
                .with_tag("18-alpine")
                .with_cmd(["postgres", "-c", "fsync=off", "-c", "max_connections=500"])
                .start()
                .expect("starting postgres, which needs Docker");
            let base = format!("postgres://postgres:postgres@{}:{}", pg.get_host().unwrap(), pg.get_host_port_ipv4(5432).unwrap());
            *POSTGRES.lock().unwrap() = Some(pg);
            base
        })
        .join()
        .unwrap()
    })
}

/// Statics are never dropped, so the container goes at exit.
#[dtor::dtor(unsafe)]
fn stop_postgres() {
    drop(POSTGRES.lock().ok().and_then(|mut pg| pg.take()));
}

/// A database of its own, named after the test and numbered, so two tests never share one.
pub(crate) async fn fresh_pool(name: &str) -> PgPool {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let base = pg_base();
    let admin = db::connect(&format!("{base}/postgres")).await.unwrap();
    let dbname = format!("dotk_test_{}_{name}", NEXT.fetch_add(1, Ordering::Relaxed));
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE DATABASE {dbname}"))).execute(&admin).await.unwrap();
    db::connect(&format!("{base}/{dbname}")).await.unwrap()
}

pub(crate) struct Harness {
    pub app: Arc<App>,
    pub sim: Arc<SimKaspad>,
    tasks: Vec<tokio::task::JoinHandle<anyhow::Result<()>>>,
}

pub(crate) fn fast_args() -> CliArgs {
    let mut args = CliArgs::defaults();
    args.network = "testnet-10".into();
    args.vcp_interval = Duration::from_millis(20);
    args.vcp_tip_distance = 0;
    args.selftest_interval = Duration::from_secs(3600);
    args.selftest_confirm_delay = Duration::from_millis(30);
    args.evictor_tick = Duration::from_millis(50);
    args.evictor_key = None;
    args.evictor_address = None;
    args
}

pub(crate) fn test_app(pool: PgPool, sim: Arc<SimKaspad>, args: CliArgs, coverage_floor: u64) -> Arc<App> {
    Arc::new(App::new(args, pool, sim, genesis_file(), dotk_core::TEST_GENESIS.as_bytes(), coverage_floor).unwrap())
}

impl Harness {
    pub(crate) fn boot(pool: PgPool, sim: Arc<SimKaspad>, args: CliArgs, start: [u8; 32]) -> Self {
        Self::boot_with(pool, sim, args, start, 0, SelfTest::Driven)
    }

    pub(crate) fn boot_ambient(pool: PgPool, sim: Arc<SimKaspad>, args: CliArgs, start: [u8; 32]) -> Self {
        Self::boot_with(pool, sim, args, start, 0, SelfTest::Ambient)
    }

    pub(crate) fn boot_with(
        pool: PgPool,
        sim: Arc<SimKaspad>,
        args: CliArgs,
        start: [u8; 32],
        coverage_floor: u64,
        selftest: SelfTest,
    ) -> Self {
        let _ = env_logger::Builder::new().parse_filters("info").is_test(true).try_init();
        let evictor_on = args.evictor_on();
        let app = test_app(pool, sim.clone(), args, coverage_floor);
        let mut tasks = vec![tokio::spawn(dotk_indexer::chain::run(app.clone(), start))];
        // The evictor waits for the startup verdict.
        if matches!(selftest, SelfTest::Ambient) || evictor_on {
            let app = app.clone();
            tasks.push(tokio::spawn(async move {
                dotk_indexer::audit::run(app).await;
                Ok(())
            }));
        }
        if evictor_on {
            let app = app.clone();
            tasks.push(tokio::spawn(async move {
                dotk_indexer::evictor::run(app).await;
                Ok(())
            }));
        }
        Harness { app, sim, tasks }
    }

    /// Every task must end cleanly, so a panic or an error in one fails the test.
    pub(crate) async fn stop(self) {
        self.app.progress.request_shutdown();
        for t in self.tasks {
            t.await.expect("a task panicked").expect("a task exited with an error");
        }
    }

    pub(crate) async fn deed_row(&self, key: &[u8; 32]) -> Option<DeedRow> {
        let mut conn = self.app.backends.db.acquire().await.unwrap();
        db::get_deed(&mut conn, key).await.unwrap()
    }

    /// Oldest first, the reverse of the API order.
    pub(crate) async fn history(&self, key: &[u8; 32]) -> Vec<dotk_indexer::model::HistoryRow> {
        let mut conn = self.app.backends.db.acquire().await.unwrap();
        let mut rows = db::history_page(&mut conn, key, 1000, 0).await.unwrap();
        rows.reverse();
        rows
    }

    pub(crate) async fn all_history(&self) -> Vec<(Vec<u8>, i16, i64)> {
        let mut conn = self.app.backends.db.acquire().await.unwrap();
        sqlx::query_as::<_, (Vec<u8>, i16, i64)>("SELECT key, op, blue_score FROM history ORDER BY id")
            .fetch_all(&mut *conn)
            .await
            .unwrap()
    }

    pub(crate) async fn gap_rows(&self) -> Vec<GapState> {
        let mut conn = self.app.backends.db.acquire().await.unwrap();
        db::all_gaps(&mut conn).await.unwrap().into_iter().map(|(lo, g)| g.state(lo)).collect()
    }

    pub(crate) async fn wait_until<F, Fut>(&self, what: &str, cond: F)
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = bool>,
    {
        self.wait_until_for(what, Duration::from_secs(10), cond).await;
    }

    pub(crate) async fn wait_until_for<F, Fut>(&self, what: &str, timeout: Duration, mut cond: F)
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = bool>,
    {
        let deadline = std::time::Instant::now() + timeout;
        while !cond().await {
            assert!(std::time::Instant::now() < deadline, "timed out waiting for: {what}");
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    /// Production stamps caught-up from a committed block, and the sim mines nothing until asked.
    pub(crate) fn caught_up_without_a_block(&self) {
        self.app.progress.last_block_ms.store(App::now_ms(), Ordering::Relaxed);
    }

    pub(crate) async fn wait_selftest(&self) -> dotk_indexer::app::SelfTestReport {
        self.wait_until("a self-test verdict", || async { self.app.verdict.health.read().await.selftest.is_some() }).await;
        self.app.verdict.health.read().await.selftest.clone().unwrap()
    }

    /// Never read a pass's report from `health`, which holds whichever pass wrote last.
    pub(crate) async fn selftest_now(&self) -> dotk_indexer::app::SelfTestReport {
        dotk_indexer::audit::run_once(&self.app).await.expect("a fresh self-test verdict")
    }

    /// The verdict of a pass that the self-test task starts after this call, for a harness that
    /// runs the task.
    pub(crate) async fn next_verdict(&self) -> dotk_indexer::app::SelfTestReport {
        let asked_at = App::now_ms();
        // A pass qualifies only if it read its clock after `asked_at`, and the wait lets the
        // triggered pass do so.
        while App::now_ms() <= asked_at {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        self.app.verdict.trigger_selftest();
        self.wait_until("a self-test verdict from after the trigger", || async {
            self.app.verdict.health.read().await.selftest.as_ref().is_some_and(|t| t.started_ms > asked_at)
        })
        .await;
        self.app.verdict.health.read().await.selftest.clone().unwrap()
    }

    pub(crate) async fn checkpoint(&self) -> Option<String> {
        let mut conn = self.app.backends.db.acquire().await.unwrap();
        db::get_var(&mut conn, db::VAR_VCP_CHECKPOINT).await.unwrap()
    }
}

/// `audit::run_once` has no mutual exclusion, so `Driven` spawns no self-test task. A background
/// pass would apply the repair that a test's pass is about to make, and overwrite `/health`.
#[derive(Clone, Copy)]
pub(crate) enum SelfTest {
    Driven,
    Ambient,
}

pub(crate) async fn standard_boot(name: &str, args: CliArgs) -> (Harness, World) {
    boot_named(name, args, SelfTest::Driven).await
}

pub(crate) async fn standard_boot_ambient(name: &str, args: CliArgs) -> (Harness, World) {
    boot_named(name, args, SelfTest::Ambient).await
}

async fn boot_named(name: &str, args: CliArgs, selftest: SelfTest) -> (Harness, World) {
    let pool = fresh_pool(name).await;
    db::migrate(&pool).await.unwrap();
    let (world, genesis_tx) = World::new();
    let mut genesis_block = vec![genesis_tx];
    if args.evictor_key.is_some() {
        genesis_block.push(payout_seed_tx());
    }
    let sim = Arc::new(SimKaspad::new(Prefix::Testnet, genesis_block));
    let start = sim.dag_info().await.unwrap().virtual_parent;
    // The mandatory startup pass, which the audit task runs otherwise.
    let no_task = matches!(selftest, SelfTest::Driven) && !args.evictor_on();
    let h = Harness::boot_with(pool, sim, args, start, 0, selftest);
    if no_task {
        h.caught_up_without_a_block();
        assert!(h.selftest_now().await.proven, "a fresh registry proves at startup");
    }
    (h, world)
}

/// Holds until the next block commits.
pub(crate) fn fall_behind(progress: &Progress) {
    let stale = App::now_ms() - dotk_indexer::app::CAUGHT_UP_MAX_AGE_MS - 5_000;
    progress.last_block_ms.store(stale, Ordering::Relaxed);
    assert!(!progress.caught_up());
}

pub(crate) const WHOLE_KEYSPACE: GapState = GapState { lo: KEY_MIN, hi: KEY_MAX };

/// The fixtures hold exactly one PENDING deed.
pub(crate) fn pending_key(w: &World) -> [u8; 32] {
    w.deeds.iter().find(|d| d.state.status == Status::Pending).expect("a pending deed").state.key
}

pub(crate) fn event(block_hash: [u8; 32], seq: i32, blue_score: u64, key: [u8; 32], prev: Option<DeedRow>) -> db::EventRow {
    db::EventRow { block_hash, seq, blue_score, key, prev, prev_gaps: vec![], prev_cards: vec![] }
}

/// An unproven snapshot of this registry at `checkpoint`.
pub(crate) fn snapshot_at(
    checkpoint: String,
    deeds: Vec<snapshot::ExportDeed>,
    events: Vec<snapshot::ExportEvent>,
) -> snapshot::Snapshot {
    snapshot::Snapshot {
        proven: false,
        proven_at: None,
        registry_covenant_id: REGISTRY_COVENANT_ID.into(),
        vcp_checkpoint: checkpoint,
        history_seq: None,
        deeds,
        cards: vec![],
        events,
    }
}

/// Requests against one router, with the status, the `Cache-Control` value and the body.
pub(crate) struct Web {
    router: axum::Router,
}

impl Web {
    pub(crate) fn new(h: &Harness) -> Self {
        Self { router: dotk_indexer::web::router(h.app.clone()) }
    }

    pub(crate) async fn send(&self, req: Request<Body>) -> axum::response::Response {
        self.router.clone().oneshot(req).await.unwrap()
    }

    pub(crate) async fn get(&self, uri: &str) -> (StatusCode, String, String) {
        let res = self.send(Request::builder().uri(uri).body(Body::empty()).unwrap()).await;
        let status = res.status();
        let cache = res.headers().get("cache-control").map(|v| v.to_str().unwrap().to_string()).unwrap_or_default();
        let body = axum::body::to_bytes(res.into_body(), 1 << 22).await.unwrap();
        (status, cache, String::from_utf8_lossy(&body).to_string())
    }

    /// The status, `ETag`, `Cache-Control` and body length, sent with an `If-None-Match` when given.
    pub(crate) async fn get_tagged(
        &self,
        uri: &str,
        if_none_match: Option<String>,
    ) -> (StatusCode, Option<String>, Option<String>, usize) {
        let mut req = Request::builder().uri(uri);
        if let Some(tag) = if_none_match {
            req = req.header("if-none-match", tag);
        }
        let res = self.send(req.body(Body::empty()).unwrap()).await;
        let status = res.status();
        let headers = res.headers().clone();
        let body = axum::body::to_bytes(res.into_body(), 1 << 22).await.unwrap();
        let header = |n: &str| headers.get(n).map(|v| v.to_str().unwrap().to_string());
        (status, header("etag"), header("cache-control"), body.len())
    }

    pub(crate) async fn get_json(&self, uri: &str) -> (StatusCode, String, serde_json::Value) {
        let (status, cache, body) = self.get(uri).await;
        let v: serde_json::Value = serde_json::from_str(&body).unwrap_or_else(|e| panic!("{uri} body is not json ({e}): {body}"));
        (status, cache, v)
    }
}
