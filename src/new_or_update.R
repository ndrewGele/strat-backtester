new_or_update <- function(db.con, strategy.list) {
  
  mode <- 'new'
  
  if(DBI::dbExistsTable(db.con, 'backtests')) {
    
    # Get data for previous backtests, if available
    previous_backtests_df <- db.con %>% 
      tbl('backtests') %>% 
      filter(strategy %in% !!strategy.list) %>% 
      collect()
    print(previous_backtests_df)
    
    # Just make a new backtest if existing data is all updated
    if(min(previous_backtests_df$end_date) == Sys.Date()) {
      message('All appear to be up to date. Trying new backtest')
      mode <- 'new'
    } else {
      
      # Find all backtests that could potentially get updated
      updated_backtests_df <- previous_backtests_df %>% 
        slice_max(order_by = end_date, n = 1)
      print(updated_backtests_df)
      
      print(names(previous_backtests_df))
      print(names(updated_backtests_df))
      
      old_backtests_df <- anti_join(
        previous_backtests_df,
        updated_backtests_df,
        by = c('strategy', 'symbol', 'model_set_hash', 'parameter_set_hash')
      )
      message(glue::glue(
        '{nrow(old_backtests_df)} backtests are not updated. Rolling dice.'
      ))
      
      # Chance to perform a new backtest is based on how many olds exist,  
      # but the chance is never less than 1%.
      # We add an arbitrary value to the number of old backtests because 
      # if there was a single outdated backtest, it would never get updated.
      if(runif(1) < max(.01, (1/(nrow(old_backtests_df) + 19)))) {
        message('Lucky! Trying new backtest.')
        mode <- 'new'
      } else {
        message('Will try to update existing backtest.')
        mode <- 'update'
      }
      
    }
    
  }
  
  return(mode)
  
}