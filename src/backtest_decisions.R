backtest_decisions <- function(decisions.df, ohlc.df, starting.cash) {
  
  require(dplyr)
  
  # First make sure that the inputs are valid
  expected_names <- c(
    'strategy', 'date', 'symbol', 'decision', 'order_type', 'limit_price', 
    'stop_price', 'trail_price', 'trail_percent', 'order_class', 
    'take_profit', 'stop_loss', 'stop_limit'
  )
  
  purrr::walk(
    .x = expected_names,
    .f = \(x) {
      if(!x %in% names(decisions.df)) {
        stop(glue::glue('decisions.df is missing needed variable: {x}'))
      }
    }
  ) 
  
  purrr::walk(
    .x = c('symbol', 'date', 'open', 'high', 'low', 'close'),
    .f = \(x) {
      if(!x %in% names(ohlc.df)) {
        stop(glue::glue('ohlc.df is missing needed variable: {x}'))
      }
    }
  )
  
  
  # Create unfilled data frame of results
  backtest_df <- bind_rows(
    ohlc.df %>% 
      mutate(time = 'market hours'),
    decisions.df %>% 
      mutate(time = 'after hours') %>% 
      select(-strategy)
  ) %>% 
    arrange(date, desc(time)) %>% 
    relocate(date, time)
  
  # Initialize some variables for our loop process
  current_cash <- starting.cash
  current_qty <- 0L
  open_order <- tibble(decision = NA)
  results_df <- tibble()

  for(x in 1:nrow(backtest_df)) {
    
    current_row <- backtest_df[x,]
    
    # After hours, turn new decisions into orders
    if(current_row$time == 'after hours' & !is.na(current_row$decision)) {
      
      if(!is.na(open_order$decision)) {
        if(open_order$decision == current_row$decision) {
          current_row$event <- 'order replaced'
          open_order <- current_row
        }
      } else {
        current_row$event <- 'new order opened'
        open_order <- current_row
      }
      
      
    } # end of after hours
    
    # Market hours, execute open orders is prices are right
    if(current_row$time == 'market hours' & !is.na(open_order$decision)) {
      
      current_row$old_qty <- as.integer(current_qty)
      current_row$old_cash <- current_cash
      
      # In order to start simple and add complexity later,
      # logic will be based on order type
      if(open_order$order_type == 'market') {
        
        if(
          open_order$decision == 'buy' & current_cash & 
          current_cash > current_row$open
        ) {
          current_row$event <- 'stock bought'
          current_row$new_qty <- current_row$old_qty + as.integer(
            floor(current_cash / current_row$open)
          )
          current_row$new_cash <- current_cash - 
            (current_row$open * (current_row$new_qty - current_row$old_qty))
        } else if(open_order$decision == 'sell' & current_qty > 0) {
          current_row$event <- 'stock sold'
          current_row$new_qty <- 0L
          current_row$new_cash <- current_cash + 
            (current_row$open * current_row$old_qty)
        }
        
      } # end of order type: market
      else if(open_order$order_type == 'limit') {
        
        warn('limit orders arent ready yet')
        
      } # end of order type: limit
      
      if(hasName(current_row, 'event')) {
        current_cash <- current_row$new_cash
        current_qty <- current_row$new_qty
        open_order <- tibble(decision = NA)
      }
      
    } # end of market hours
    
    # only retain rows with events to keep output simple
    if(hasName(current_row, 'event')) {
      results_df <- bind_rows(results_df, current_row)
    }

  }
  
  results <- list(
    df = results_df,
    cash = current_cash,
    qty = current_qty,
    last_close = tail(x = ohlc.df$close, n = 1)
  )
  results$market_value <- results$qty * results$last_close
  results$total_assets <- results$cash + results$market_value
  
  return(results)
  
}

# # Test case
# backtest_test <- backtest_decisions(
#   decisions.df = strategy_decisions_df,
#   ohlc.df = picked_symbol_df %>% select(date, symbol, open, high, low, close),
#   starting.cash = 10000.00
# )
# print(backtest_test$total_assets)
