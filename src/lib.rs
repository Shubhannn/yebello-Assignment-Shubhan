use bs58;
use substreams::errors::Error;
use substreams_solana::pb::sf::solana::r#type::v1::Block;

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

const SYSTEM_PROGRAM_ID: [u8; 32] = [0; 32];

#[substreams::handlers::map]
pub fn map_transfers(block: Block) -> Result<Transfers, Error> {
	let mut transfers = Vec::new();

	for tx in &block.transactions {
		if tx.meta.as_ref().is_some_and(|meta| meta.err.is_some()) {
			continue;
		}

		let Some(transaction) = tx.transaction.as_ref() else {
			continue;
		};
		let Some(message) = transaction.message.as_ref() else {
			continue;
		};
		let account_keys = &message.account_keys;

		for instruction in &message.instructions {
			let Some(program_id) = account_keys.get(instruction.program_id_index as usize) else {
				continue;
			};
			if program_id.as_slice() != SYSTEM_PROGRAM_ID {
				continue;
			}

			let Some(data) = instruction.data.get(..12) else {
				continue;
			};
			let Ok(instruction_type) = <[u8; 4]>::try_from(&data[..4]) else {
				continue;
			};
			if u32::from_le_bytes(instruction_type) != 2 {
				continue;
			}
			let Ok(amount_bytes) = <[u8; 8]>::try_from(&data[4..12]) else {
				continue;
			};

			let Some(&from_index) = instruction.accounts.first() else {
				continue;
			};
			let Some(&to_index) = instruction.accounts.get(1) else {
				continue;
			};
			let (Some(from), Some(to)) = (
				account_keys.get(from_index as usize),
				account_keys.get(to_index as usize),
			) else {
				continue;
			};

			transfers.push(Transfer {
				from: bs58::encode(from).into_string(),
				to: bs58::encode(to).into_string(),
				amount: u64::from_le_bytes(amount_bytes),
			});
		}
	}

	Ok(Transfers { transfers })
}
