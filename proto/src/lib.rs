use substreams_solana::pb::sf::solana::type::v1::Block;
use bs58;

mod pb {
    pub mod solana {
        pub mod transfers {
            pub mod v1 {
                include!(concat!(env!("OUT_DIR"), "/solana.transfers.v1.rs"));
            }
        }
    }
}

use pb::solana::transfers::v1::{Transfer, Transfers};

// System Program ID: 32 bytes of zeros
const SYSTEM_PROGRAM_ID: [u8; 32] = [0u8; 32];

#[substreams::handlers::map]
pub fn map_transfers(block: Block) -> Result<Transfers, substreams::errors::Error> {
    let mut transfers = Vec::new();

    for tx in &block.transactions {
        // Skip failed transactions safely
        if let Some(meta) = &tx.meta {
            if meta.err.is_some() {
                continue;
            }
        }

        let transaction = match &tx.transaction {
            Some(t) => t,
            None => continue,
        };

        let message = match &transaction.message {
            Some(m) => m,
            None => continue,
        };

        // Access raw account keys slice directly (avoids internal crate helper panic)
        let account_keys = &message.account_keys;
        if account_keys.is_empty() {
            continue;
        }

        for inst in &message.instructions {
            let program_id_idx = inst.program_id_index as usize;
            
            let program_id = match account_keys.get(program_id_idx) {
                Some(id) => id,
                None => continue,
            };

            if program_id.as_slice() != SYSTEM_PROGRAM_ID {
                continue;
            }

            // Ensure data length is at least 12 bytes
            if inst.data.len() < 12 {
                continue;
            }

            // Safely parse instruction type (u32 LE)
            let type_bytes = match inst.data.get(0..4) {
                Some(slice) => match slice.try_into() {
                    Ok(arr) => arr,
                    Err(_) => continue,
                },
                None => continue,
            };

            let instruction_type = u32::from_le_bytes(type_bytes);
            if instruction_type != 2 {
                continue;
            }

            // Safely parse transfer amount (u64 LE)
            let amount_bytes = match inst.data.get(4..12) {
                Some(slice) => match slice.try_into() {
                    Ok(arr) => arr,
                    Err(_) => continue,
                },
                None => continue,
            };

            let amount = u64::from_le_bytes(amount_bytes);

            // Extract from and to account indices
            let from_idx = match inst.accounts.get(0) {
                Some(&idx) => idx as usize,
                None => continue,
            };

            let to_idx = match inst.accounts.get(1) {
                Some(&idx) => idx as usize,
                None => continue,
            };

            let from_account = match account_keys.get(from_idx) {
                Some(acc) => acc,
                None => continue,
            };

            let to_account = match account_keys.get(to_idx) {
                Some(acc) => acc,
                None => continue,
            };

            transfers.push(Transfer {
                from: bs58::encode(from_account).into_string(),
                to: bs58::encode(to_account).into_string(),
                amount,
            });
        }
    }

    Ok(Transfers { transfers })
}