pick_symbol <- function(db.con, strategy.list) {
  
  # Get list of all allowed symbols
  symbols_df <- db.con %>% 
    tbl('symbols') %>% 
    inner_join(
      db.con %>% 
        tbl('symbols') %>% 
        group_by(fetcher_name) %>% 
        summarise(update_timestamp = max(update_timestamp, na.rm = TRUE)),
      by = c('fetcher_name', 'update_timestamp')
    ) %>% 
    inner_join(
      db.con %>% 
        tbl('daily_ohlc') %>% 
        group_by(symbol) %>% 
        summarise(count = n()) %>% 
        filter(count >= 150) %>%
        select(symbol) %>% 
        distinct(),
      by = 'symbol'
    ) %>% 
    select(symbol) %>% 
    distinct() %>%
    collect()
  
  # Check for existing backtests for valid symbols
  if(DBI::dbExistsTable(db.con, 'backtests')) {
    
    previous_backtests_df <- db.con %>% 
      tbl('backtests') %>% 
      filter(
        strategy %in% !!strategy.list,
        symbol %in% !!symbols_df$symbol
      ) %>% 
      collect()
    
    symbol_count_df <- symbols_df %>%
      left_join(
        previous_backtests_df, 
        by = 'symbol',
        relationship = 'many-to-many'
      ) %>% 
      group_by(symbol) %>% 
      summarise(count = n())
    
    picked_symbol <- sample(
      x = symbol_count_df$symbol, 
      size = 1, 
      prob = 1/symbol_count_df$count
    )
    
  } else {
    
    picked_symbol <- sample(
      x = symbols_df$symbol,
      size = 1
    )
    
  }
  
  return(picked_symbol)
  
}
