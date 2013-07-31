# Impaler

Impaler combines the best of Impala and Hive.  Queries are run on Impala and if it fails there it will fallback to running the query in Hive.  

## Installation

Add this line to your application's Gemfile:

    gem 'impaler'

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install impaler

## Usage

Basic Usage

    require 'impaler'
    c = Impaler.connect(['hivethrift_server:10000'], ['impala_server:21000'])
    c.query("select count(*) from my_table") # This will run in Impala
    c.query("select name, collect_set(foo) from my_table") # This will run in Hive (after a quick error on Impala)
    c.query("select count(*) from my_table", Impaler::HIVE_ONLY) # This is forced to run on Hive

## Contributing

1. Fork it
1. Create your feature branch (`git checkout -b my-new-feature`)
1. Test your changes in both connected and unconnected modes (`rspec` and `IMPALA_SERVER=server:21000 HIVETHRIFT_SERVER=server:10000 TEST_TABLE=my_test_table TEST_TABLE_COLUMN=some_test_column rspec`
1. Commit your changes (`git commit -am 'Add some feature'`)
1. Push to the branch (`git push origin my-new-feature`)
1. Create new Pull Request
