require 'impaler'

# These tests cover the Impaler interaction with a real impala and hive server.
# To run these define environment variables for IMPALA_SERVER and HIVETHRIFT_SERVER
# in the format "server:port".

IMPALA_SERVER=ENV['IMPALA_SERVER']
HIVETHRIFT_SERVER=ENV['HIVETHRIFT_SERVER']
has_servers=!IMPALA_SERVER.nil? && !HIVETHRIFT_SERVER.nil?
IMPALA_SERVERS=[IMPALA_SERVER]
HIVETHRIFT_SERVERS=[HIVETHRIFT_SERVER]

TEST_TABLE=ENV['TEST_TABLE']
TEST_TABLE_COLUMN=ENV['TEST_TABLE_COLUMN']
has_tables=!TEST_TABLE.nil? && !TEST_TABLE_COLUMN.nil?

run_tests=has_tables && has_servers

def connect
  Impaler.connect(IMPALA_SERVERS, HIVETHRIFT_SERVERS)
end

describe Impaler, :if => run_tests do 
  it("Connected skip", :if=>!run_tests) {
    puts "Skipping connected tests for Impaler, set the environment variables IMPALA_SERVER, HIVETHRIFT_SERVER, TEST_TABLE, and TEST_TABLE_COLUMN to enable these"
    puts "IMPALA_SERVER and HIVETHRIFT_SERVER are in the format 'server:port'"
    puts "TEST_TABLE should be a fairly small table for quick tests and TEST_TABLE_COLUMN will be used for some simple test queries where Impala is known to not support the query"
  }


  describe "connect" do
    it "connects without error" do
      expect { Impaler::connect(IMPALA_SERVERS, HIVETHRIFT_SERVERS) }.not_to raise_error
      expect { Impaler::connect(impala_servers=IMPALA_SERVERS, hivethrift_servers=HIVETHRIFT_SERVERS) }.not_to raise_error
      # These aren't supported yet
      #expect { Impaler::connect(impala_servers=IMPALA_SERVERS) }.not_to raise_error
      #expect { Impaler::connect(hivethrift_servers=HIVETHRIFT_SERVERS) }.not_to raise_error
    end
  end

  describe "simple query" do
    it "supports a count(*) query" do
      c = connect
      count = (c.query "select count(*) c from #{TEST_TABLE}").first[:c]
      (c.query "select count(*) c from #{TEST_TABLE}").first[:c].should eq(count)
      (c.query "select count(*) c from #{TEST_TABLE}", Impaler::HIVE_ONLY).first[:c].should eq(count)
      (c.query "select count(*) c from #{TEST_TABLE}", Impaler::IMPALA_ONLY).first[:c].should eq(count)
      (c.query "select count(*) c from #{TEST_TABLE}", Impaler::IMPALA_THEN_HIVE).first[:c].should eq(count)
    end
  end
end
