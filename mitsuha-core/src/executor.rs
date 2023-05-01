use std::collections::HashMap;

use crate::{
    errors::Error,
    symbol::{Symbol, SymbolFunc},
    types,
};

pub struct ExecutorContext {
    symbol_table: HashMap<Symbol, SymbolFunc>,
}

impl ExecutorContext {
    pub fn new() -> Self {
        Self {
            symbol_table: HashMap::new(),
        }
    }

    pub fn add_symbol(&mut self, symbol: Symbol, func: SymbolFunc) -> types::Result<()> {
        if self.symbol_table.contains_key(&symbol) {
            return Err(Error::AmbiguousSymbolError {
                symbol: symbol.clone(),
            });
        }

        self.symbol_table.insert(symbol, func);

        Ok(())
    }

    pub async fn call(&self, symbol: &Symbol, input: Vec<u8>) -> types::Result<Vec<u8>> {
        let output = self
            .symbol_table
            .get(symbol)
            .ok_or(Error::NotFoundSymbolError {
                symbol: symbol.clone(),
            })?
            .as_ref()
            .read()
            .await
            (input)
        .await;

        Ok(output)
    }
}
