pick_strategy <- function(db.con, strategy.list, symbol) {
  
  if(DBI::dbExistsTable(db.con, 'backtests')) {
    
    previous_backtests_df <- db_con %>% 
      tbl('backtests') %>% 
      filter(
        strategy %in% !!strategy.list,
        symbol %in% !!symbol
      ) %>% 
      collect()
    
    strategy_count_df <- data.frame(strategy = strategy.list) %>%
      left_join(
        previous_backtests_df, 
        by = 'strategy',
        relationship = 'many-to-many'
      ) %>% 
      group_by(strategy) %>% 
      summarise(count = n())
    
    picked_strategy <- sample(
      x = strategy_count_df$strategy, 
      size = 1, 
      prob = 1/strategy_count_df$count
    )
    
  } else {
    
    picked_strategy <- sample(
      x = strategy.list,
      size = 1
    )
    
  }
  
  return(picked_strategy)
  
}
