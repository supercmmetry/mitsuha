use std::collections::HashMap;

use crate::{symbol::{SymbolFunc, Symbol}, types, errors::Error, kernel::Kernel};

pub struct ExecutorContext {
    symbol_table: HashMap<Symbol, SymbolFunc>,
    kernel: Box<dyn Kernel>,
}

impl ExecutorContext {
    pub fn new(kernel: Box<dyn Kernel>) -> Self {
        Self {
            symbol_table: HashMap::new(),
            kernel,
        }
    }

    pub fn add_symbol(&mut self, symbol: Symbol, func: SymbolFunc) -> types::Result<()> {
        if self.symbol_table.contains_key(&symbol) {
            return Err(Error::AmbiguousSymbolError { symbol: symbol.clone() });
        }

        self.symbol_table.insert(symbol, func);

        Ok(())
    }

    pub async fn call(&self, symbol: &Symbol, input: Vec<u8>) -> types::Result<Vec<u8>> {
        let output = self
            .symbol_table
            .get(symbol)
            .ok_or(Error::NotFoundSymbolError { symbol: symbol.clone() })?
            .as_ref()
            .read()
            .unwrap()(input)
            .await;

        Ok(output)
    }

    pub fn get_kernel(&self) -> &dyn Kernel {
        self.kernel.as_ref()
    }
    
}