use std::{fs, str::FromStr};

use magnus_shared::Dex;
use serde::Deserialize;
use solana_sdk::pubkey::Pubkey;

fn deserialize_pubkey<'de, D>(deserializer: D) -> Result<Pubkey, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    Pubkey::from_str(&s).map_err(serde::de::Error::custom)
}

macro_rules! define_dex_configs {
    (
        $(
            $dex_variant:ident => $struct_name:ident : $cfg_field:ident ($toml_key:literal) {
                $( $field:ident ),* $(,)?
            }
        ),* $(,)?
    ) => {
        $(
            #[derive(Debug, Deserialize, Clone)]
            pub struct $struct_name {
                $(
                    #[serde(deserialize_with = "deserialize_pubkey")]
                    pub $field: Pubkey,
                )*
            }

            impl $struct_name {
                pub fn accounts(&self) -> Vec<Pubkey> {
                    vec![ $( self.$field ),* ]
                }
            }
        )*

        #[derive(Clone, Debug, Deserialize, Default)]
        pub struct PMMCfg {
            $(
                #[serde(rename = $toml_key, default)]
                pub $cfg_field: Option<$struct_name>,
            )*
        }

        impl PMMCfg {
            pub fn load(path: &str) -> eyre::Result<Self> {
                let contents = fs::read_to_string(path)?;
                let cfg: PMMCfg = toml::from_str(&contents)?;
                Ok(cfg)
            }

            pub fn get_accounts(&self, dex: &Dex) -> Option<Vec<Pubkey>> {
                match dex {
                    $(
                        Dex::$dex_variant => self.$cfg_field.as_ref().map(|c| c.accounts()),
                    )*
                    _ => None,
                }
            }

            pub fn get_market(&self, dex: &Dex) -> Option<Pubkey> {
                match dex {
                    $(
                        Dex::$dex_variant => self.$cfg_field.as_ref().map(|c| c.market),
                    )*
                    _ => None,
                }
            }

            pub fn get_config<T: DexCfg>(&self) -> Option<&T> {
                T::from_cfg(self)
            }
        }

        pub trait DexCfg: Sized {
            fn from_cfg(cfg: &PMMCfg) -> Option<&Self>;
        }

        $(
            impl DexCfg for $struct_name {
                fn from_cfg(cfg: &PMMCfg) -> Option<&Self> {
                    cfg.$cfg_field.as_ref()
                }
            }
        )*
    };
}

// Define all DEX configurations
define_dex_configs! {
    Humidifi => HumidifiCfg : humidifi ("humidifi") {
        market,
        base_token_acc,
        quote_token_acc,
    },
    Tessera => TesseraCfg : tessera ("tessera") {
        market,
        base_token_acc,
        quote_token_acc,
        global_state,
    },
    Goonfi => GoonfiCfg : goonfi ("goonfi") {
        market,
        base_token_acc,
        quote_token_acc,
        blacklist,
    },
    SolfiV2 => SolfiV2Cfg : solfi_v2 ("solfi-v2") {
        market,
        base_token_acc,
        quote_token_acc,
        cfg,
        oracle,
    },
    Zerofi => ZerofiCfg : zerofi ("zerofi") {
        market,
        vault_info_base,
        vault_base,
        vault_info_quote,
        vault_quote,
    },
    ObricV2 => ObricV2Cfg : obric_v2 ("obric-v2") {
        market,
        second_ref_oracle,
        third_ref_oracle,
        reserve_x,
        reserve_y,
        ref_oracle,
        x_price_feed,
        y_price_feed,
    },
}
