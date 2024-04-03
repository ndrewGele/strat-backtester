pick_params <- function(
  db.con,
  backtest.mode = c('new', 'update'),
  symbol,
  strategy,
  param.set.fun
) {
  
  if(!DBI::dbExistsTable(db.con, 'backtests')) {
    # Just generate fresh params if no backtests have ever been done
    picked_params <- param.set.fun()
  } else {
    
    # If table does exist, decision will be based on what's in it
    previous_backtests_df <- db.con %>% 
      tbl('backtests') %>% 
      filter(
        symbol == !!symbol,
        strategy == !!strategy
      ) %>% 
      collect()
    
    if(nrow(previous_backtests_df) == 0) {
      
      # If there were no backtests for the selected strategy, go fresh
      picked_params <- param.set.fun()
    
    } else if(backtest.mode == 'update') {
      
      # If update, just find an un-updated param set
      picked_params <- previous_backtests_df %>% 
        slice_min(order_by = end_date, n = 1) %>% 
        pluck(parameter_set) %>% 
        sample(size = 1)
      
    } else if(backtest.mode == 'new') {
      
      # If new, let's consider controlling an existing parameter
      tried_params <- previous_backtests_df %>% 
        pluck(parameter_set)
      
      # RNG time
      if(runif(1) < max(.01, (1/(length(tried_params) + 4)))) {
        
        # If lucky, generate fresh params
        picked_params <- param.set.fun()
        
      } else {
        
        # If not lucky, modify existing param set
        fresh_params <- param.set.fun()
        old_params <- sample(x = tried_params, size = 1)
        param_to_sub <- sample(x = 1:length(old_params), size = 1)
        
        old_params[param_to_sub] <- fresh_params[param_to_sub]
        
        picked_params <- old_params
        
      }
        
    } else {
      stop('pick_params.R: how did i get here')
    }
  
  }
  
  return(picked_params)
  
}