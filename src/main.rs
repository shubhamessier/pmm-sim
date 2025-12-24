#![doc = include_str!("../README.md")]
#![allow(clippy::type_complexity, clippy::result_large_err)]
#![deny(unused)]

//! Simulation environment for Solana Proprietary AMM swaps.
//!
//! Simulate Swaps across *any* of the major Solana Proprietary AMMs, locally, using LiteSVM.
use std::{
    collections::HashSet,
    fmt::{Debug, Display},
    fs::{self, File},
    io::Write,
    path::Path,
    str::FromStr,
    time::SystemTime,
};

use base64::{Engine, engine::general_purpose};
use clap::{Args, Parser, Subcommand};
use litesvm::LiteSVM;
use magnus_router_client::instructions::SwapBuilder;
use magnus_shared::{Dex, Route};
use pmm_sim::PMMCfg;
use secrecy::{ExposeSecret, SecretString};
use solana_client::rpc_client::RpcClient;
use solana_compute_budget::compute_budget::ComputeBudget;
use solana_sdk::{
    account::Account, message::AccountMeta, program_pack::Pack, pubkey::Pubkey, rent::Rent, signature::Keypair, signer::Signer, sysvar,
    transaction::Transaction,
};
use spl_associated_token_account::get_associated_token_address;
use tracing::{debug, info, warn};
use tracing_subscriber::{EnvFilter, fmt::time::UtcTime};

pub mod consts {
    use solana_sdk::{pubkey, pubkey::Pubkey};

    pub const ROUTER: &str = "magnus-router";
    pub const SETUP_PATH: &str = "./setup.toml";
    pub const PROGRAMS_PATH: &str = "./cfg/programs";
    pub const ACCOUNTS_PATH: &str = "./cfg/accounts";

    pub const WSOL: Pubkey = pubkey!("So11111111111111111111111111111111111111112");
    pub const WSOL_DECIMALS: u8 = 9;

    pub const USDC: Pubkey = pubkey!("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v");
    pub const USDC_DECIMALS: u8 = 6;

    pub const USDT: Pubkey = pubkey!("Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB");
    pub const USDT_DECIMALS: u8 = 6;
}

#[derive(Parser, Debug)]
#[command(version, about = "Simulation environment for Solana Proprietary AMM swaps.\nSimulate Swaps across *any* of the major Solana Prop AMMs.", long_about = None)]
pub struct CliArgs {
    #[command(subcommand)]
    pub command: Command,
}

impl CliArgs {
    fn parse_nested_pmms(s: &str) -> Result<Vec<Vec<Dex>>, String> {
        if let Ok(parsed) = serde_json::from_str::<Vec<Vec<String>>>(s) {
            return parsed.into_iter().map(|group| group.into_iter().map(|s| s.parse::<Dex>()).collect::<Result<Vec<Dex>, _>>()).collect();
        }

        let s = s.trim();
        if !s.starts_with("[[") || !s.ends_with("]]") {
            return Err("invalid format: expected [[dex1,dex2],[dex3]]".to_string());
        }

        let inner = &s[1..s.len() - 1];
        inner
            .split("],[")
            .map(|group| {
                let group = group.trim_matches('[').trim_matches(']');
                group.split(',').map(|dex| dex.trim().parse::<Dex>()).collect::<Result<Vec<Dex>, _>>()
            })
            .collect()
    }

    fn parse_nested_weights(s: &str) -> Result<Vec<Vec<u8>>, String> {
        serde_json::from_str(s).map_err(|e| format!("invalid format: {}", e))
    }

    fn default_pmm() -> Vec<Dex> {
        Dex::PMM.to_vec()
    }
}

#[derive(Args, Debug)]
pub struct CommonArgs {
    #[arg(long, env = "HTTP_URL", default_value = "https://api.mainnet.solana.com")]
    pub http_url: SecretString,

    #[arg(
        long,
        env = "JIT_ACCOUNTS",
        action = clap::ArgAction::Set,
        default_value_t = true,
        help = "Fetch accounts at runtime (use --jit-accounts=false for loading the local ones instead)"
    )]
    pub jit_accounts: bool,

    #[arg(long, env = "SRC_TOKEN", default_value = "wsol", help = "Source token: wsol, usdc, or usdt")]
    pub src_token: Token,

    #[arg(long, env = "DST_TOKEN", default_value = "usdc", help = "Destination token: wsol, usdc, or usdt")]
    pub dst_token: Token,

    #[arg(long, env = "SETUP_PATH", default_value = consts::SETUP_PATH, help = "Path to the setup configuration file")]
    pub setup_path: String,

    #[arg(long, env = "PROGRAMS_PATH", default_value = consts::PROGRAMS_PATH, help = "Directory to load programs from")]
    pub programs_path: String,

    #[arg(long, env = "ACCOUNTS_PATH", default_value = consts::ACCOUNTS_PATH, help = "Directory to load accounts from")]
    pub accounts_path: String,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    #[command(
        about = "Run a single swap instruction across one or more Prop AMMs with specified weights.",
        after_help = "Examples:
  pmm-sim single --pmms=humidifi --weights=100 --amount-in=100 --src-token=WSOL --dst-token=USDC
  pmm-sim single --pmms=humidifi,solfi-v2 --weights=50,50 --amount-in=150000 --src-token=USDC --dst-token=WSOL
  pmm-sim single --amount-in=10000 --pmms=solfi-v2 --weights=100
  pmm-sim single --amount-in=10000 --pmms=obric-v2 --weights=100 --src-token=USDC --dst-token=USDT"
    )]
    Single {
        #[command(flatten)]
        common: CommonArgs,

        #[arg(long, env = "AMOUNT_IN", default_value_t = 1, help = "The amount of tokens to trade")]
        amount_in: u64,

        #[arg(long, value_delimiter = ',', default_value = "humidifi,solfi-v2", help = "Comma-separated list of Prop AMMs")]
        pmms: Vec<Dex>,

        #[arg(long, value_delimiter = ',', default_value = "50,50", help = "Comma-separated weights")]
        weights: Vec<u8>,
    },

    #[command(
        about = "Execute multiple swap instructions across nested Prop AMM routes. Each inner list represents a single transaction step.",
        after_help = "Examples:
      # Single step with one DEX
      pmm-sim multi --pmms='[[humidifi]]' --weights='[[100]]'

      # Two sequential swaps: (Humidifi + Obric) followed by Zerofi
      pmm-sim multi --pmms='[[humidifi,zerofi],[solfi-v2]]' --weights='[[50,50],[100]]'

      # Complex three-step route
      pmm-sim multi --amount-in 10 --pmms='[[humidifi],[solfi-v2],[zerofi]]' --weights='[[100],[60,40],[100]]'"
    )]
    Multi {
        #[command(flatten)]
        common: CommonArgs,

        #[arg(
            long,
            env = "AMOUNT_IN",
            value_delimiter = ',',
            num_args = 1..,
            default_values_t = vec![1, 1],
            help = "Comma-separated amounts for each route, e.g. --amount-in=3,50"
        )]
        amount_in: Vec<u64>,

        #[arg(long, default_value = "[[humidifi,solfi-v2],[solfi-v2]]", help = "JSON nested routes, e.g. '[[dex1,dex2],[dex3]]'")]
        pmms: String,

        #[arg(
            long,
            default_value = "[[30,70],[100]]",
            help = "JSON nested weights matching the prop AMMs structure, e.g. '[[50,50],[100]]'"
        )]
        weights: String,
    },

    #[command(
        about = "Fetch accounts from the specified Pmms via RPC and save them locally (presumably for later usage).",
        after_help = "Examples:
  pmm-sim fetch-accounts --pmms=humidifi
  pmm-sim fetch-accounts --pmms=humidifi,obric-v2,zerofi,solfi-v2pmm-sim \
                      fetch-accounts --pmms=humidifi --http-url=https://my-rpc.com"
    )]
    FetchAccounts {
        #[arg(long, env = "HTTP_URL", default_value = "https://api.mainnet.solana.com")]
        http_url: SecretString,

        #[arg(long, env = "SETUP_PATH", default_value = consts::SETUP_PATH, help = "Path to the setup configuration file")]
        setup_path: String,

        #[arg(long, env = "ACCOUNTS_PATH", default_value = consts::ACCOUNTS_PATH, help = "Directory to save fetched accounts")]
        accounts_path: String,

        #[arg(
            long,
            value_delimiter = ',',
            default_values_t = CliArgs::default_pmm(),
            help = "Comma-separated list of Prop AMMs to fetch accounts for"
        )]
        pmms: Vec<Dex>,
    },
}

impl Command {
    fn setup_path(&self) -> &str {
        match self {
            Command::FetchAccounts { setup_path, .. } => setup_path,
            Command::Single { common, .. } => &common.setup_path,
            Command::Multi { common, .. } => &common.setup_path,
        }
    }

    fn name(&self) -> &'static str {
        match self {
            Command::FetchAccounts { .. } => "FetchAccounts",
            Command::Single { .. } => "SingleSwap",
            Command::Multi { .. } => "MultiSwap",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Token {
    WSOL,
    USDC,
    USDT,
}

impl FromStr for Token {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "wsol" | "sol" => Ok(Token::WSOL),
            "usdc" => Ok(Token::USDC),
            "usdt" => Ok(Token::USDT),
            _ => Err(format!("invalid token '{}'. valid options: wsol, usdc, usdt", s)),
        }
    }
}

impl Display for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Token::WSOL => f.write_str("WSOL"),
            Token::USDT => f.write_str("USDT"),
            Token::USDC => f.write_str("USDC"),
        }
    }
}

impl Token {
    fn get_addr(&self) -> Pubkey {
        match *self {
            Token::WSOL => consts::WSOL,
            Token::USDC => consts::USDC,
            Token::USDT => consts::USDT,
        }
    }

    fn get_decimals(&self) -> u8 {
        match *self {
            Token::WSOL => consts::WSOL_DECIMALS,
            Token::USDC => consts::USDC_DECIMALS,
            Token::USDT => consts::USDT_DECIMALS,
        }
    }
}

/// The Simulation Environment;
/// Ensures proper setup of the LiteSVM, wallet, programs, and accounts.
/// Also provides utility functions for common operations, like
/// loading programs/accounts, setting up the wallet, sending transactions, etc.
struct Environment<'a, P: Into<String> + Display + Clone + Debug> {
    svm: LiteSVM,
    wallet: Keypair,
    mints: Option<&'a [(Pubkey, u8)]>,
    cfg: PMMCfg,

    programs_path: P,
    accounts_path: P,
}

impl<'a, P: Into<String> + Display + Clone + std::fmt::Debug> Debug for Environment<'a, P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Environment")
            .field("wallet_pubkey", &self.wallet.pubkey())
            .field("programs_path", &self.programs_path)
            .field("accounts_path", &self.accounts_path)
            .field("mints", &self.mints)
            .finish()
    }
}

impl<'a, P: Into<String> + Display + Clone + Debug> Environment<'a, P> {
    fn new(programs_path: P, accounts_path: P, mints: Option<&[(Pubkey, u8)]>, cfg: PMMCfg) -> eyre::Result<Environment<'_, P>> {
        let mut budget = ComputeBudget::new_with_defaults(false);
        budget.compute_unit_limit = 20_000_000;

        let wallet = Keypair::new();
        let mut svm = LiteSVM::new().with_default_programs().with_sysvars().with_sigverify(true).with_compute_budget(budget);

        if let Some(mints) = mints {
            for (mint, mint_decimals) in mints {
                svm.set_account(*mint, Misc::mk_mint_acc(*mint_decimals))?;
            }
        }

        Ok(Environment { svm, wallet, programs_path, accounts_path, mints, cfg })
    }

    fn setup_wallet(&mut self, mint: &Pubkey, mint_amount: u64, airdrop_amount: u64) -> eyre::Result<()> {
        // create the ATAs for all initialised mints
        if let Some(mints) = self.mints {
            for (mint, _) in mints {
                let ata = get_associated_token_address(&self.wallet.pubkey(), mint);
                self.svm.set_account(ata, Misc::mk_ata(mint, &self.wallet.pubkey(), 0))?;
            }
        }

        let ata = get_associated_token_address(&self.wallet.pubkey(), mint);
        self.svm.set_account(ata, Misc::mk_ata(mint, &self.wallet.pubkey(), mint_amount))?;

        self.svm.airdrop(&self.wallet.pubkey(), airdrop_amount).expect("airdrop failed");

        Ok(())
    }

    fn wallet_pubkey(&self) -> Pubkey {
        self.wallet.pubkey()
    }

    fn wallet_ata(&self, mint: &Pubkey) -> Pubkey {
        get_associated_token_address(&self.wallet.pubkey(), mint)
    }

    fn load_programs(&mut self, pmms: &[Dex]) -> eyre::Result<()> {
        // mandatory load
        self.svm
            .add_program_from_file(magnus_router_client::programs::ROUTER_ID, format!("{}/{}.so", self.programs_path, consts::ROUTER))?;

        let unique_pmms: HashSet<_> = pmms.iter().collect();
        for dex in unique_pmms {
            let program_id = dex.program_id();

            self.svm.add_program_from_file(Pubkey::new_from_array(program_id.to_bytes()), format!("{}/{}.so", self.programs_path, dex))?;

            info!("loaded program for {dex}");
        }

        Ok(())
    }

    fn load_accounts(&mut self, pmms: &[Dex], jit: bool, client: Option<&RpcClient>) -> eyre::Result<()> {
        match jit {
            true => {
                let rpc_client = client.expect("RPC client is required for JIT account loading");
                self.load_jit_accounts(pmms, rpc_client)?;
            }
            false => {
                self.load_static_accounts(pmms)?;
            }
        }

        Ok(())
    }

    fn load_static_accounts(&mut self, pmms: &[Dex]) -> eyre::Result<()> {
        let unique_pmms: HashSet<_> = pmms.iter().collect();
        let mut all_slots: Vec<u64> = vec![];

        for dex in unique_pmms {
            let (accounts, slot) = self.read_accounts_from_disk(dex)?;

            for (pubkey, account) in accounts {
                self.svm.set_account(pubkey, account)?;
                debug!("loaded account {pubkey} for {dex}");
            }

            if let Some(s) = slot {
                all_slots.push(s);
            }

            info!("loaded accounts for {dex}");
        }

        // check if for some reason the slots logged in the accs config files differ
        // if they do, warp up to the first slot
        if !all_slots.is_empty() {
            let first_slot = all_slots[0];
            if all_slots.iter().any(|&s| s != first_slot) {
                let min_slot = all_slots.iter().min().copied().unwrap();
                let max_slot = all_slots.iter().max().copied().unwrap();
                warn!("slot mismatch across Prop AMMs: accounts fetched at different slots ({min_slot} - {max_slot}), using {first_slot}");
            }
            self.svm.warp_to_slot(first_slot);
            info!("warped to slot {first_slot}");
        }

        Ok(())
    }

    fn load_jit_accounts(&mut self, pmms: &[Dex], client: &RpcClient) -> eyre::Result<()> {
        let (slot, fetched) = Misc::acquire_pmm_accounts(pmms, client, &self.cfg)?;

        for (dex, accounts) in fetched {
            for (pubkey, account) in accounts {
                self.svm.set_account(pubkey, account)?;
                debug!("loaded account {pubkey} for {dex}");
            }
            info!("loaded accounts for {dex}");
        }

        self.svm.warp_to_slot(slot);
        info!("warped to slot {slot}");

        Ok(())
    }

    fn read_accounts_from_disk(&self, pmm: &Dex) -> eyre::Result<(Vec<(Pubkey, Account)>, Option<u64>)> {
        let prefix = pmm.to_string();
        let accounts_path = format!("{}", self.accounts_path);
        let data_dir = Path::new(&accounts_path);

        if !data_dir.exists() {
            return Ok((vec![], None));
        }

        let (mut accounts, mut slots) = (vec![], vec![]);
        for entry in fs::read_dir(data_dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_file()
                && path.file_name().and_then(|n| n.to_str()).is_some_and(|name| name.starts_with(&prefix) && name.ends_with(".json"))
            {
                let (pubkey, account, slot) = Self::read_account_from_file(&path)?;
                accounts.push((pubkey, account));
                if let Some(s) = slot {
                    slots.push(s);
                }
            }
        }

        let slot = if slots.is_empty() {
            None
        } else {
            let first_slot = slots[0];
            if slots.iter().any(|&s| s != first_slot) {
                let min_slot = slots.iter().min().copied().unwrap();
                let max_slot = slots.iter().max().copied().unwrap();
                warn!("slot mismatch for {pmm}: accounts fetched at different slots ({min_slot} - {max_slot}), using {first_slot}");
            }
            Some(first_slot)
        };

        debug!(?accounts, ?slot);
        Ok((accounts, slot))
    }

    fn read_account_from_file(path: &Path) -> eyre::Result<(Pubkey, Account, Option<u64>)> {
        let contents = fs::read_to_string(path)?;
        let value: serde_json::Value = serde_json::from_str(&contents)?;

        let pubkey = Pubkey::from_str(value["pubkey"].as_str().ok_or_else(|| eyre::eyre!("missing pubkey"))?)?;
        let lamports = value["account"]["lamports"].as_u64().ok_or_else(|| eyre::eyre!("missing lamports"))?;
        let data_base64 = value["account"]["data"][0].as_str().ok_or_else(|| eyre::eyre!("missing data"))?;
        let data = general_purpose::STANDARD.decode(data_base64)?;
        let owner = Pubkey::from_str(value["account"]["owner"].as_str().ok_or_else(|| eyre::eyre!("missing owner"))?)?;
        let executable = value["account"]["executable"].as_bool().ok_or_else(|| eyre::eyre!("missing executable"))?;
        let rent_epoch = value["account"]["rentEpoch"].as_u64().ok_or_else(|| eyre::eyre!("missing rentEpoch"))?;
        let slot = value["slot"].as_u64();

        Ok((pubkey, Account { lamports, data, owner, executable, rent_epoch }, slot))
    }

    fn save_account_to_disk(&self, dex: &Dex, pubkey: &Pubkey, account: &Account, slot: u64) -> eyre::Result<()> {
        let filename = format!("{}_{}.json", dex, pubkey);
        let accounts_path = format!("{}", self.accounts_path);
        let data_dir = Path::new(&accounts_path);

        if !data_dir.exists() {
            fs::create_dir_all(data_dir)?;
        }

        let file_path = data_dir.join(filename);

        let value = serde_json::json!({
            "pubkey": pubkey.to_string(),
            "slot": slot,
            "account": {
                "lamports": account.lamports,
                "data": [general_purpose::STANDARD.encode(&account.data), "base64"],
                "owner": account.owner.to_string(),
                "executable": account.executable,
                "rentEpoch": account.rent_epoch,
            }
        });

        let mut file = File::create(file_path)?;
        file.write_all(serde_json::to_string_pretty(&value)?.as_bytes())?;

        Ok(())
    }

    fn token_balance(&self, mint: &Pubkey) -> u64 {
        let ata = self.wallet_ata(mint);
        let account = self.svm.get_account(&ata).unwrap_or_default();
        spl_token::state::Account::unpack(&account.data).map(|a| a.amount).unwrap_or(0)
    }

    fn latest_blockhash(&self) -> solana_sdk::hash::Hash {
        self.svm.latest_blockhash()
    }

    fn send_transaction(&mut self, tx: Transaction) -> litesvm::types::TransactionResult {
        self.svm.send_transaction(tx)
    }
}

/// A helper struct to construct swap instructions with the required accounts
/// for different Prop AMMs.
///
/// As is currently the case, all swaps pass through the Magnus Router program,
/// which in turn calls the respective Prop AMM program. Therefore, the swap
/// instruction is built using the `SwapBuilder` from the `magnus-router-client`
/// crate, and then the required accounts for the specific Prop AMM are attached.
pub struct ConstructSwap<'a> {
    cfg: PMMCfg,
    builder: &'a mut SwapBuilder,
    payer: Pubkey,
    sta: Pubkey,
    dta: Pubkey,
}

impl<'a> ConstructSwap<'a> {
    fn instruction(&self) -> solana_sdk::instruction::Instruction {
        self.builder.instruction()
    }

    pub fn attach_solfiv2_accs(&mut self) {
        if let Some(cfg) = &self.cfg.solfi_v2 {
            self.builder
                .add_remaining_account(AccountMeta::new_readonly(
                    Pubkey::new_from_array(magnus_shared::pmm_solfi_v2::id().to_bytes()),
                    false,
                ))
                .add_remaining_account(AccountMeta::new(self.payer, true))
                .add_remaining_account(AccountMeta::new(self.sta, false))
                .add_remaining_account(AccountMeta::new(self.dta, false))
                .add_remaining_account(AccountMeta::new(cfg.market, false))
                .add_remaining_account(AccountMeta::new_readonly(cfg.oracle, false))
                .add_remaining_account(AccountMeta::new_readonly(cfg.cfg, false))
                .add_remaining_account(AccountMeta::new(cfg.pool_base_vault, false))
                .add_remaining_account(AccountMeta::new(cfg.pool_quote_vault, false))
                .add_remaining_account(AccountMeta::new_readonly(consts::WSOL, false))
                .add_remaining_account(AccountMeta::new_readonly(consts::USDC, false))
                .add_remaining_account(AccountMeta::new_readonly(spl_token::id(), false))
                .add_remaining_account(AccountMeta::new_readonly(spl_token::id(), false))
                .add_remaining_account(AccountMeta::new_readonly(sysvar::instructions::id(), false));
        } else {
            panic!("SolfiV2 config is missing, cannot attach accounts.");
        }
    }

    pub fn attach_humidifi_accs(&mut self) {
        if let Some(cfg) = &self.cfg.humidifi {
            self.builder
                .add_remaining_account(AccountMeta::new_readonly(
                    Pubkey::new_from_array(magnus_shared::pmm_humidifi::id().to_bytes()),
                    false,
                ))
                .add_remaining_account(AccountMeta::new(self.payer, true))
                .add_remaining_account(AccountMeta::new(self.sta, false))
                .add_remaining_account(AccountMeta::new(self.dta, false))
                .add_remaining_account(AccountMeta::new_readonly(Misc::create_humidifi_param(1500), false))
                .add_remaining_account(AccountMeta::new(cfg.market, false))
                .add_remaining_account(AccountMeta::new(cfg.base_token_account, false))
                .add_remaining_account(AccountMeta::new(cfg.quote_token_account, false))
                .add_remaining_account(AccountMeta::new_readonly(sysvar::clock::id(), false))
                .add_remaining_account(AccountMeta::new_readonly(spl_token::id(), false))
                .add_remaining_account(AccountMeta::new_readonly(sysvar::instructions::id(), false));
        } else {
            panic!("Humidifi config is missing, cannot attach accounts.");
        }
    }

    pub fn attach_zerofi_accs(&mut self) {
        if let Some(cfg) = &self.cfg.zerofi {
            self.builder
                .add_remaining_account(AccountMeta::new_readonly(Pubkey::new_from_array(magnus_shared::pmm_zerofi::id().to_bytes()), false))
                .add_remaining_account(AccountMeta::new(self.payer, true))
                .add_remaining_account(AccountMeta::new(self.sta, false))
                .add_remaining_account(AccountMeta::new(self.dta, false))
                .add_remaining_account(AccountMeta::new(cfg.pair, false))
                .add_remaining_account(AccountMeta::new(cfg.vault_info_base, false))
                .add_remaining_account(AccountMeta::new(cfg.vault_base, false))
                .add_remaining_account(AccountMeta::new(cfg.vault_info_quote, false))
                .add_remaining_account(AccountMeta::new(cfg.vault_quote, false))
                .add_remaining_account(AccountMeta::new_readonly(spl_token::id(), false))
                .add_remaining_account(AccountMeta::new_readonly(sysvar::instructions::id(), false));
        } else {
            panic!("Zerofi config is missing, cannot attach accounts.");
        }
    }

    pub fn attach_obric_v2_accs(&mut self) {
        if let Some(cfg) = &self.cfg.obric_v2 {
            self.builder
                .add_remaining_account(AccountMeta::new_readonly(
                    Pubkey::new_from_array(magnus_shared::pmm_obric_v2::id().to_bytes()),
                    false,
                ))
                .add_remaining_account(AccountMeta::new(self.payer, true))
                .add_remaining_account(AccountMeta::new(self.sta, false))
                .add_remaining_account(AccountMeta::new(self.dta, false))
                .add_remaining_account(AccountMeta::new(cfg.trading_pair, false))
                .add_remaining_account(AccountMeta::new_readonly(cfg.second_ref_oracle, false))
                .add_remaining_account(AccountMeta::new_readonly(cfg.third_ref_oracle, false))
                .add_remaining_account(AccountMeta::new(cfg.reserve_x, false))
                .add_remaining_account(AccountMeta::new(cfg.reserve_y, false))
                .add_remaining_account(AccountMeta::new(cfg.ref_oracle, false))
                .add_remaining_account(AccountMeta::new_readonly(cfg.x_price_feed, false))
                .add_remaining_account(AccountMeta::new_readonly(cfg.y_price_feed, false))
                .add_remaining_account(AccountMeta::new_readonly(spl_token::id(), false));
        } else {
            panic!("ObricV2 config is missing, cannot attach accounts.");
        }
    }

    pub fn attach_tessera_accs(&mut self, src_mint: &Pubkey, dst_mint: &Pubkey) {
        if let Some(cfg) = &self.cfg.tessera {
            self.builder
                .add_remaining_account(AccountMeta::new_readonly(
                    Pubkey::new_from_array(magnus_shared::pmm_tessera::id().to_bytes()),
                    false,
                ))
                .add_remaining_account(AccountMeta::new(self.payer, true))
                .add_remaining_account(AccountMeta::new(self.sta, false))
                .add_remaining_account(AccountMeta::new(self.dta, false))
                .add_remaining_account(AccountMeta::new_readonly(cfg.global_state, false))
                .add_remaining_account(AccountMeta::new(cfg.market, false))
                .add_remaining_account(AccountMeta::new(cfg.base_token_account, false))
                .add_remaining_account(AccountMeta::new(cfg.quote_token_account, false))
                .add_remaining_account(AccountMeta::new_readonly(*src_mint, false))
                .add_remaining_account(AccountMeta::new_readonly(*dst_mint, false))
                .add_remaining_account(AccountMeta::new_readonly(spl_token::id(), false))
                .add_remaining_account(AccountMeta::new_readonly(spl_token::id(), false))
                .add_remaining_account(AccountMeta::new_readonly(sysvar::instructions::id(), false));
        } else {
            panic!("Tessera config is missing, cannot attach accounts.");
        }
    }
}

struct Misc;
impl Misc {
    fn mk_ata(mint: &Pubkey, user: &Pubkey, amount: u64) -> Account {
        let ata = spl_token::state::Account {
            mint: *mint,
            owner: *user,
            amount,
            state: spl_token::state::AccountState::Initialized,
            ..Default::default()
        };

        let mut data = vec![0u8; spl_token::state::Account::LEN];
        ata.pack_into_slice(&mut data);

        Account {
            lamports: Rent::default().minimum_balance(data.len()),
            data,
            owner: spl_token::id(),
            executable: false,
            rent_epoch: u64::MAX,
        }
    }

    fn mk_mint_acc(decimals: u8) -> Account {
        let mint = spl_token::state::Mint {
            mint_authority: solana_sdk::program_option::COption::None,
            supply: u64::MAX,
            decimals,
            is_initialized: true,
            freeze_authority: Default::default(),
        };

        let mut data = vec![0u8; spl_token::state::Mint::LEN];
        spl_token::state::Mint::pack(mint, &mut data).unwrap();

        Account {
            lamports: Rent::default().minimum_balance(data.len()),
            data,
            owner: spl_token::id(),
            executable: false,
            rent_epoch: u64::MAX,
        }
    }

    fn create_humidifi_param(swap_id: u64) -> Pubkey {
        let mut bytes = [0u8; 32];
        bytes[0..8].copy_from_slice(&swap_id.to_le_bytes());
        Pubkey::new_from_array(bytes)
    }

    fn acquire_pmm_accounts(pmms: &[Dex], client: &RpcClient, cfg: &PMMCfg) -> eyre::Result<(u64, Vec<(Dex, Vec<(Pubkey, Account)>)>)> {
        let slot = client.get_slot()?;
        let unique_pmms: HashSet<_> = pmms.iter().collect();
        let mut results = vec![];

        for dex in unique_pmms {
            let Some(accounts) = cfg.get_accounts(dex) else {
                warn!("skipping unsupported prop amms: {dex}");
                continue;
            };

            info!("fetching {} accounts for {dex} at slot {slot}", accounts.len());

            let fetched = client.get_multiple_accounts(&accounts)?;
            let mut dex_accounts = vec![];
            for (pubkey, account) in accounts.iter().zip(fetched.into_iter()) {
                if let Some(acc) = account {
                    dex_accounts.push((*pubkey, acc));
                } else {
                    warn!("account {pubkey} not found for {dex}");
                }
            }

            results.push((*dex, dex_accounts));
        }

        Ok((slot, results))
    }
}

pub struct Run {
    args: CliArgs,
    cfg: PMMCfg,
}

impl Run {
    fn new(args: CliArgs, cfg: PMMCfg) -> Self {
        Self { args, cfg }
    }

    fn run(&self) -> eyre::Result<()> {
        match &self.args.command {
            Command::FetchAccounts { .. } => self.fetch_accounts(),
            Command::Single { .. } | Command::Multi { .. } => self.simulate(),
        }
    }

    fn fetch_accounts(&self) -> eyre::Result<()> {
        let Command::FetchAccounts { http_url, accounts_path, pmms, .. } = &self.args.command else { unreachable!() };

        let rpc_client = RpcClient::new(http_url.expose_secret().to_string());
        let env = Environment::new("", accounts_path, None, self.cfg.clone())?;

        let (slot, fetched) = Misc::acquire_pmm_accounts(pmms, &rpc_client, &self.cfg)?;
        for (dex, accounts) in fetched {
            for (pubkey, account) in accounts {
                env.save_account_to_disk(&dex, &pubkey, &account, slot)?;
                info!("saved account {pubkey} for {dex}");
            }
        }

        info!("done fetching accounts at slot {slot}");
        Ok(())
    }

    fn simulate(&self) -> eyre::Result<()> {
        let (common, amount_in, pmms, weights) = match &self.args.command {
            Command::Single { common, amount_in, pmms, weights } => (common, vec![*amount_in], vec![pmms.clone()], vec![weights.clone()]),
            Command::Multi { common, amount_in, pmms, weights } => {
                let pmms = CliArgs::parse_nested_pmms(pmms).expect("invalid format for nested dexes");
                let weights = CliArgs::parse_nested_weights(weights).expect("invalid format for nested weights");

                (common, amount_in.clone(), pmms, weights)
            }
            _ => unreachable!(),
        };

        // ensure that each dex has a corresponding weight
        assert_eq!(pmms.len(), weights.len(), "dexes and weights outer length mismatch");
        for (d, w) in pmms.iter().zip(weights.iter()) {
            assert_eq!(d.len(), w.len(), "dexes and weights length mismatch");
        }

        let rpc_client = RpcClient::new(common.http_url.expose_secret().to_string());
        let flat_pmms: Vec<Dex> = pmms.iter().flatten().copied().collect();

        let (src_mint, src_dec, src_name) = (common.src_token.get_addr(), common.src_token.get_decimals(), common.src_token.to_string());
        let (dst_mint, dst_dec, dst_name) = (common.dst_token.get_addr(), common.dst_token.get_decimals(), common.dst_token.to_string());
        let mints = vec![(src_mint, src_dec), (dst_mint, dst_dec)];

        let mut env = Environment::new(consts::PROGRAMS_PATH, consts::ACCOUNTS_PATH, Some(&mints), self.cfg.clone())?;
        env.load_programs(&flat_pmms)?;
        env.load_accounts(&flat_pmms, common.jit_accounts, Some(&rpc_client))?;

        // - mint only the source token's desired amount (i.e the amount we're going to swap)
        // - airdrop some SOL to cover fees
        env.setup_wallet(&src_mint, amount_in.iter().sum::<u64>() * 10u64.pow(src_dec as u32), 10_000_000_000)?;
        info!(?env);

        let (src_ata, dst_ata) = (env.wallet_ata(&src_mint), env.wallet_ata(&dst_mint));
        let (src_before, dst_before) = (
            env.token_balance(&src_mint) as f64 / 10_f64.powi(src_dec as i32),
            env.token_balance(&dst_mint) as f64 / 10_f64.powi(dst_dec as i32),
        );
        info!("before: {} = {} | {} = {}", src_name, src_before, dst_name, dst_before);

        let routes: Vec<Vec<magnus_router_client::types::Route>> = pmms
            .iter()
            .zip(weights.iter())
            .map(|(dex_group, weight_group)| vec![Route { dexes: dex_group.clone(), weights: weight_group.clone() }.into()])
            .collect();

        info!("ROUTES LEN: {}", routes.len());

        let norm_amount_in: Vec<u64> = amount_in.iter().map(|amount| amount * 10u64.pow(src_dec as u32)).collect();
        info!("swapping {:?} {} via routes: {:?}", norm_amount_in, src_name, routes);

        let mut swap_builder = SwapBuilder::new();
        let swap = swap_builder
            .payer(env.wallet_pubkey())
            .source_token_account(src_ata)
            .destination_token_account(dst_ata)
            .source_mint(src_mint)
            .destination_mint(dst_mint)
            .amount_in(norm_amount_in.iter().sum())
            .expect_amount_out(1)
            .min_return(1)
            .amounts(norm_amount_in)
            .routes(routes)
            .order_id(SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs());

        let mut construct = ConstructSwap { cfg: self.cfg.clone(), builder: swap, payer: env.wallet_pubkey(), sta: src_ata, dta: dst_ata };

        // attach the necessary accounts for each of the implemented Prop AMMs
        for dex in flat_pmms.iter() {
            match dex {
                Dex::Humidifi => construct.attach_humidifi_accs(),
                Dex::SolfiV2 => construct.attach_solfiv2_accs(),
                Dex::Zerofi => construct.attach_zerofi_accs(),
                Dex::ObricV2 => construct.attach_obric_v2_accs(),
                Dex::Tessera => construct.attach_tessera_accs(&src_mint, &dst_mint),
                _ => {
                    unimplemented!()
                }
            };
        }

        let swap_ix = construct.instruction();
        debug!("router program id: {}", swap_ix.program_id);

        let tx = Transaction::new_signed_with_payer(&[swap_ix], Some(&env.wallet_pubkey()), &[&env.wallet], env.latest_blockhash());
        let res = env.send_transaction(tx).expect("failed to exec tx");

        let (src_after, dst_after) = (
            env.token_balance(&src_mint) as f64 / 10_f64.powi(src_dec as i32),
            env.token_balance(&dst_mint) as f64 / 10_f64.powi(dst_dec as i32),
        );
        info!("|SWAP EXECUTED| compute units consumed: {}", res.compute_units_consumed);
        info!("after: {} = {:.6} | {} = {:.6} | ", src_name, src_after, dst_name, dst_after);
        info!("diff: {} spent = {:.6} | {} received = {:.6}", src_name, src_before - src_after, dst_name, dst_after - dst_before);

        Ok(())
    }
}

fn main() -> eyre::Result<()> {
    tracing_subscriber::fmt()
        .with_thread_ids(true)
        .with_line_number(true)
        .with_target(true)
        .with_timer(UtcTime::rfc_3339())
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::default().add_directive(tracing::Level::INFO.into())),
        )
        .init();

    let args = CliArgs::parse();
    info!(?args, command = args.command.name());

    let cfg = PMMCfg::load(args.command.setup_path())?;

    Run::new(args, cfg).run()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_nested_pmms_json_single() {
        let input = r#"[["humidifi"]]"#;
        let result = CliArgs::parse_nested_pmms(input).unwrap();
        assert_eq!(result, vec![vec![Dex::Humidifi]]);
    }

    #[test]
    fn test_parse_nested_pmms_json_multiple() {
        let input = r#"[["humidifi","obric-v2"],["zerofi"]]"#;
        let result = CliArgs::parse_nested_pmms(input).unwrap();
        assert_eq!(result, vec![vec![Dex::Humidifi, Dex::ObricV2], vec![Dex::Zerofi]]);
    }

    #[test]
    fn test_parse_nested_pmms_no_quotes_single() {
        let input = "[[humidifi]]";
        let result = CliArgs::parse_nested_pmms(input).unwrap();
        assert_eq!(result, vec![vec![Dex::Humidifi]]);
    }

    #[test]
    fn test_parse_nested_pmms_no_quotes_single_route_multiple_pmms() {
        let input = "[[humidifi,obric-v2]]";
        let result = CliArgs::parse_nested_pmms(input).unwrap();
        assert_eq!(result, vec![vec![Dex::Humidifi, Dex::ObricV2]]);
    }

    #[test]
    fn test_parse_nested_pmms_no_quotes_multiple_routes() {
        let input = "[[humidifi,obric-v2],[zerofi]]";
        let result = CliArgs::parse_nested_pmms(input).unwrap();
        assert_eq!(result, vec![vec![Dex::Humidifi, Dex::ObricV2], vec![Dex::Zerofi]]);
    }

    #[test]
    fn test_parse_nested_pmms_no_quotes_three_routes() {
        let input = "[[humidifi],[obric-v2,solfi-v2],[zerofi]]";
        let result = CliArgs::parse_nested_pmms(input).unwrap();
        assert_eq!(result, vec![vec![Dex::Humidifi], vec![Dex::ObricV2, Dex::SolfiV2], vec![Dex::Zerofi],]);
    }

    #[test]
    fn test_parse_nested_pmms_no_quotes_all_pmms() {
        let input = "[[raydium-cl-v2,raydium-cp],[obric-v2,solfi-v2,zerofi,humidifi]]";
        let result = CliArgs::parse_nested_pmms(input).unwrap();
        assert_eq!(result, vec![vec![Dex::RaydiumClV2, Dex::RaydiumCp], vec![Dex::ObricV2, Dex::SolfiV2, Dex::Zerofi, Dex::Humidifi],]);
    }

    #[test]
    fn test_parse_nested_pmms_no_quotes_with_spaces() {
        let input = "[[ humidifi , obric-v2 ],[ zerofi ]]";
        let result = CliArgs::parse_nested_pmms(input).unwrap();
        assert_eq!(result, vec![vec![Dex::Humidifi, Dex::ObricV2], vec![Dex::Zerofi]]);
    }

    #[test]
    fn test_parse_nested_pmms_invalid_pmm() {
        let input = "[[humidifi,invalid-dex]]";
        let result = CliArgs::parse_nested_pmms(input);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_nested_pmms_invalid_format() {
        let input = "[humidifi]"; // not nested
        let result = CliArgs::parse_nested_pmms(input);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_nested_weights_single() {
        let input = "[[100]]";
        let result = CliArgs::parse_nested_weights(input).unwrap();
        assert_eq!(result, vec![vec![100u8]]);
    }

    #[test]
    fn test_parse_nested_weights_multiple() {
        let input = "[[50,50],[100]]";
        let result = CliArgs::parse_nested_weights(input).unwrap();
        assert_eq!(result, vec![vec![50u8, 50u8], vec![100u8]]);
    }

    #[test]
    fn test_parse_nested_weights_complex() {
        let input = "[[30,30,40],[60,40],[100]]";
        let result = CliArgs::parse_nested_weights(input).unwrap();
        assert_eq!(result, vec![vec![30u8, 30u8, 40u8], vec![60u8, 40u8], vec![100u8]]);
    }

    #[test]
    fn test_pmms_and_weights_match() {
        let pmms_input = "[[humidifi,obric-v2],[zerofi]]";
        let weights_input = "[[50,50],[100]]";

        let pmms = CliArgs::parse_nested_pmms(pmms_input).unwrap();
        let weights = CliArgs::parse_nested_weights(weights_input).unwrap();

        assert_eq!(pmms.len(), weights.len());
        for (d, w) in pmms.iter().zip(weights.iter()) {
            assert_eq!(d.len(), w.len());
        }
    }
}
