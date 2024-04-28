# This script will run a backtest for a single strategy/symbol and record the 
# results

library(dplyr)
library(dbplyr)
library(crayon) # rlang::last_trace keeps demanding crayon be installed

source(glue::glue('{Sys.getenv("COMMON_CODE_CONTAINER")}/src/get_all_data.R'))


# Init Connections to run Pre-req Checks ----------------------------------

# Init Strategies List and Run Check
strat_list <- list.files(
  glue::glue('{Sys.getenv("STRATEGIES_CONTAINER")}/strategies')
)
if(length(strat_list) < 1) {
  message(
    'Strategies directory is empty. ',
    'Ensure environment var points to the correct path.'
  )
  Sys.sleep(60)
  stop('Stopping process.')
}


# Initialize S3 Connection and Run Check
s3 <- paws.storage::s3(
  config = list(
    credentials = list(anonymous = TRUE),
    endpoint = glue::glue(
      'http://', 
      Sys.getenv('SEAWEED_HOST'), 
      ':', 
      Sys.getenv('SEAWEED_S3_PORT')
    ),
    region = 'xx' # can't be blank
  )
)

bucket_names <- s3$list_buckets() %>% 
  purrr::pluck('Buckets') %>% 
  purrr::map_chr(purrr::pluck, 'Name')

if(!Sys.getenv('SEAWEED_MODEL_BUCKET') %in% bucket_names) {
  warning('Model bucket not found, certain strategies may fail.')
}

# Connect to Database and Run Checks
db_con <- DBI::dbConnect(
  drv = RPostgres::Postgres(),
  dbname = Sys.getenv('POSTGRES_DB'),
  host = Sys.getenv('POSTGRES_HOST'),
  port = Sys.getenv('POSTGRES_PORT'),
  user = Sys.getenv('POSTGRES_USER'),
  password = Sys.getenv('POSTGRES_PASSWORD')
)

if(
  !DBI::dbExistsTable(db_con, 'symbols') |
  !DBI::dbExistsTable(db_con, 'daily_ohlc') |
  !DBI::dbExistsTable(db_con, 'indicators') |
  !DBI::dbExistsTable(db_con, 'macro_indicators') |
  !DBI::dbExistsTable(db_con, 'models')
) {
  DBI::dbDisconnect(db_con)
  message('Missing one or more required tables.')
  Sys.sleep(60)
  stop('Stopping process.')
}


# New Backtest or Update Existing -----------------------------------------

source('./src/new_or_update.R')
backtest_mode <- new_or_update(
  db.con = db_con,
  strategy.list = strat_list
)


# Select a Strategy and Symbol --------------------------------------------

# Didn't want to clutter main script with picking logic
# Goal is to randomly pick, but with a preference for less tested combos
source('./src/pick_symbol.R')
picked_symbol <- pick_symbol(
  db.con = db_con,
  strategy.list = strat_list
)
source('./src/pick_strategy.R')
picked_strategy <- pick_strategy(
  db.con = db_con,
  strategy.list = strat_list,
  symbol = picked_symbol
)


# Initialize Backtest Parameters ------------------------------------------

source('./src/pick_params.R')
source(
  paste0(
    glue::glue('{Sys.getenv("STRATEGIES_CONTAINER")}/strategies/'),
    glue::glue('{picked_strategy}/params.R')
  )
)

picked_params <- pick_params(
  db.con = db_con,
  backtest.mode = backtest_mode,
  symbol = picked_symbol,
  strategy = picked_strategy,
  param.set.fun = param_set_generator
)


# Pull Data and Models ----------------------------------------------------

picked_symbol_df <- get_all_data(
  db.con = db_con,
  symbols = picked_symbol
)

# SPY/VOO is our benchmark
voo_df <- get_all_data(
  db.con = db_con,
  symbols = 'VOO'
)

source(
  paste0(
    glue::glue('{Sys.getenv("STRATEGIES_CONTAINER")}/strategies/'),
    glue::glue('{picked_strategy}/models.R')
  )
)
model_reqs <- model_reqs_generator()

if(length(model_reqs) > 0) {
  
  library(tidymodels)
  
  model_options <- db_con %>%
    tbl('models') %>%
    inner_join(
      db_con %>%
        tbl('models') %>%
        filter(name %in% !!model_reqs) %>%
        group_by(name, hash) %>%
        summarise(update_timestamp = max(update_timestamp, na.rm = TRUE)),
      by = c('name', 'hash', 'update_timestamp')
    ) %>%
    filter(status == 'champion') %>%
    collect()
  
  model_set <- purrr::map(
    .x = model_reqs,
    .f = \(x) {
      model <- model_options %>%
        filter(name == x) %>%
        sample_n(1)
      
      tmp <- tempfile(pattern = model$hash)
      
      s3$download_file(
        Bucket = Sys.getenv('SEAWEED_MODEL_BUCKET'),
        Key = model$file_name,
        Filename = tmp
      )
      
      list(
        hash = model$hash,
        model = readRDS(tmp)
      )
    }
  )
  names(model_set) <- model_reqs
  
  predictions <- purrr::map_dfc(
    .x = model_set,
    .f = function(x) {
      predictions <- predict(
        object = x$model,
        new_data = picked_symbol_df
      )
    }
  )
  
  names(predictions) <- model_reqs
  
  picked_symbol_df <- bind_cols(
    picked_symbol_df,
    predictions
  )
  
}


# Perform Backtest --------------------------------------------------------

strategy_fn <- source(
  paste0(
    glue::glue('{Sys.getenv("STRATEGIES_CONTAINER")}/strategies/'),
    glue::glue('{picked_strategy}/{picked_strategy}.R')
  )
)$value

message('Running strategy funciton with the following args:')
message(glue::glue('Symbol: {picked_symbol}'))
message(glue::glue('Start Date: {min(picked_symbol_df$date)}'))
message(glue::glue('End Date: {max(picked_symbol_df$date)}'))
if(exists('model_set')) {
  message(glue::glue('Model Set (Names): {names(model_set)}'))
}
message('Param Set:')
for(i in 1:length(picked_params)) {
  message(paste0(names(picked_params[i]), ': ', picked_params[[i]]))
}

strategy_decisions_df <- strategy_fn(
  all.data = picked_symbol_df,
  model.set = model_set,
  param.set = picked_params
)

message('Running Backtest')

source('./src/backtest_decisions.R')

starting_cash <- 100000.00
backtest_results_list <- backtest_decisions(
  decisions.df = strategy_decisions_df,
  ohlc.df = picked_symbol_df %>% select(date, symbol, open, high, low, close),
  starting.cash = starting_cash
)

strat_roi <- round(
  x = backtest_results_list$total_assets / starting_cash, 
  digits = 6
)
voo_roi <- round(
  x = voo_df$close[nrow(voo_df)] / voo_df$open[1], 
  digits = 6
)
stock_roi <- round(
  x = picked_symbol_df$close[nrow(picked_symbol_df)] / picked_symbol_df$open[1],
  digits = 6
)


message(glue::glue('strat_roi: {strat_roi}'))
message(glue::glue('voo_roi: {voo_roi}'))
strat_vs_voo <- round(strat_roi / voo_roi, 6)
strat_vs_stock <- round(strat_roi / stock_roi, 6)


# Write Results to DB -----------------------------------------------------

if(exists('model_set')) {
  model_hashes <- jsonlite::toJSON(
    purrr::map(
      .x = model_set, 
      .f = purrr::pluck, 
      'hash'
    )
  )
} else {
  # Empty list to JSON doesn't create a valid format for Postgres
  model_hashes <- jsonlite::toJSON(list(no_models = 'none'))
}

row_to_write <- data.frame(
  strategy = picked_strategy,
  symbol = picked_symbol,
  start_date = min(picked_symbol_df$date),
  end_date = max(picked_symbol_df$date)
) %>%
  mutate( # weird issues with JSON when using data.frame()
    model_set = model_hashes,
    model_set_hash = substr(rlang::hash(model_hashes), 1, 8),
    parameter_set = jsonlite::toJSON(picked_params),
    parameter_set_hash = substr(rlang::hash(picked_params), 1, 8),
    performance_vs_voo = strat_vs_voo,
    performace_vs_self = strat_vs_stock,
    backtest_timestamp = Sys.time()
  )

if(!DBI::dbExistsTable(db_con, 'backtests')) {
  row_to_write %>%
    DBI::dbCreateTable(
      conn = db_con,
      name = 'backtests',
      fields = .
    )
  message('Backtests table created.')
}

existing_backtests_df <- db_con %>%
  tbl('backtests') %>%
  select(
    strategy, symbol,
    start_date, end_date,
    model_set_hash, parameter_set_hash
  ) %>%
  distinct() %>%
  collect()

row_to_write <- anti_join(
  row_to_write,
  existing_backtests_df,
  by = c(
    'strategy', 'symbol',
    'start_date', 'end_date',
    'model_set_hash', 'parameter_set_hash'
  )
)

if(nrow(row_to_write) >= 1) {
  row_to_write %>%
    DBI::dbAppendTable(
      conn = db_con,
      name = 'backtests',
      value = .
    )
  message('Results written to backtests table.')
} else {
  message('Backtest is duplicate despite. Not writing to table.')
}

DBI::dbDisconnect(db_con)

message('Process finished succesfully.')
Sys.sleep(300)
stop('Stopping process.')
